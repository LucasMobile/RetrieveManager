from __future__ import annotations

from urllib.parse import urlencode


def paginate(total: int, page: int, size: int) -> dict:
    size = max(1, size)
    pages = max(1, (total + size - 1) // size) if total else 1
    page = max(1, min(int(page or 1), pages))
    offset = (page - 1) * size
    return {
        "page": page,
        "pages": pages,
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


def query_keep(**params) -> str:
    cleaned = {k: v for k, v in params.items() if v not in (None, "", 0)}
    return urlencode(cleaned, doseq=True)
