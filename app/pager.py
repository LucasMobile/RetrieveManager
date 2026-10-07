from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlencode

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session
from starlette.exceptions import HTTPException as StarletteHTTPException


def _page_items(page: int, pages: int) -> list[int | None]:
    """Return the compact page-number sequence shown by the shared pager."""
    if pages <= 7:
        return list(range(1, pages + 1))
    if page <= 3:
        return [1, 2, 3, None, pages]
    if page >= pages - 2:
        return [1, None, pages - 2, pages - 1, pages]
    return [1, None, page - 1, page, page + 1, None, pages]


def paginate(total: int, page: int, size: int) -> dict:
    size = max(1, size)
    pages = max(1, (total + size - 1) // size) if total else 1
    page = max(1, min(int(page or 1), pages))
    offset = (page - 1) * size
    return {
        "page": page,
        "pages": pages,
        "page_items": _page_items(page, pages),
        "size": size,
        "total": total,
        "offset": offset,
        "has_prev": page > 1,
        "has_next": page < pages,
        "prev": page - 1,
        "next": page + 1,
        "from": (offset + 1) if total else 0,
        "to": min(offset + size, total),
    }


def cursor_page_links(
    pager: dict,
    *,
    page_cursors: Mapping[int, int | None],
) -> list[dict | None]:
    """Build compact numbered links backed by keyset cursors."""
    links: list[dict | None] = []
    for item in pager["page_items"]:
        if item is None:
            links.append(None)
            continue
        link = {"page": item, "current": item == pager["page"]}
        if item == 1:
            link["query"] = query_keep(page=1)
        elif item == pager["pages"]:
            link["query"] = query_keep(last=1, page=pager["pages"])
        elif item == pager["page"]:
            link["query"] = ""
        elif page_cursors.get(item) is not None:
            link["query"] = query_keep(before=page_cursors[item], page=item)
        else:
            link["disabled"] = True
        links.append(link)
    return links


def cursor_page_cursors(
    db: Session,
    filtered: Select,
    id_column,
    pager: dict,
) -> dict[int, int | None]:
    """Return the keyset boundary required to open every visible page."""
    cursors: dict[int, int | None] = {}
    for target_page in pager["page_items"]:
        if target_page in (None, 1, pager["page"], pager["pages"]):
            continue
        boundary_offset = ((target_page - 1) * pager["size"]) - 1
        cursors[target_page] = db.scalar(
            filtered.with_only_columns(id_column)
            .order_by(id_column.desc())
            .offset(boundary_offset)
            .limit(1)
        )
    return cursors


def keyset_page(
    db: Session,
    filtered: Select,
    id_column,
    *,
    page: int,
    page_size: int,
    size_options: Sequence[int],
    before: int | None,
    after: int | None,
    last: bool,
    keep: Mapping[str, Any],
    options: Sequence[Any] = (),
) -> tuple[list[Any], dict]:
    """Return one newest-first page of ``filtered`` and its cursor pager.

    ``before`` moves to older rows, ``after`` to newer rows and ``last`` to the
    oldest page; without a cursor the first page is shown.
    """
    if page_size not in size_options:
        raise StarletteHTTPException(422, "Quantidade de itens por página inválida")
    if sum((before is not None, after is not None, last)) > 1:
        raise StarletteHTTPException(422, "Use apenas um cursor de paginação")
    if page > 1 and before is None and after is None and not last:
        raise StarletteHTTPException(422, "Cursor de paginação ausente")

    total = int(db.scalar(select(func.count()).select_from(filtered.subquery())) or 0)
    pages = max(1, (total + page_size - 1) // page_size)
    if last:
        page = pages
    elif before is None and after is None:
        page = 1
    pager = paginate(total, page, page_size)
    pager["size_options"] = size_options
    pager["keep"] = dict(keep)

    stmt = filtered.options(*options) if options else filtered
    if last:
        stmt = stmt.order_by(id_column.asc())
    elif before is not None:
        stmt = stmt.where(id_column < before).order_by(id_column.desc())
    elif after is not None:
        stmt = stmt.where(id_column > after).order_by(id_column.asc())
    else:
        stmt = stmt.order_by(id_column.desc())
    result_limit = (total - pager["offset"]) if last else pager["size"]
    rows = list(db.scalars(stmt.limit(result_limit)))
    if after is not None or last:
        rows.reverse()

    def _exists(condition) -> bool:
        return (
            db.scalar(filtered.where(condition).with_only_columns(id_column).limit(1))
            is not None
        )

    first_id = getattr(rows[0], id_column.key) if rows else None
    last_id = getattr(rows[-1], id_column.key) if rows else None
    pager.update(
        cursor=True,
        has_prev=first_id is not None and _exists(id_column > first_id),
        has_next=last_id is not None and _exists(id_column < last_id),
        prev_cursor=first_id,
        next_cursor=last_id,
    )
    pager["page_links"] = cursor_page_links(
        pager,
        page_cursors=cursor_page_cursors(db, filtered, id_column, pager),
    )
    return rows, pager


def query_keep(**params) -> str:
    cleaned = {k: v for k, v in params.items() if v not in (None, "", 0)}
    return urlencode(cleaned, doseq=True)
