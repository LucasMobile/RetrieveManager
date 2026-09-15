from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlencode


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


def query_keep(**params) -> str:
    cleaned = {k: v for k, v in params.items() if v not in (None, "", 0)}
    return urlencode(cleaned, doseq=True)
