"""Helpers shared by every pipeline stage."""

from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError

from app.models import (
    Order,
    Unit,
)
from app.observability import (
    new_correlation_id,
    safe_error_detail,
)

log = logging.getLogger("worker")


def ensure_order_correlation(order: Order) -> str:
    if not order.correlation_id:
        order.correlation_id = new_correlation_id()
    return order.correlation_id


def bounded_db_text(value: object, limit: int) -> str:
    """Normalize external PACS/file text before writing bounded DB columns."""
    return str(value or "").replace("\x00", " ").strip()[:limit]


def database_error_detail(exc: SQLAlchemyError) -> str:
    """Backward-compatible alias used by existing operational logging."""
    return safe_error_detail(exc)


def append_diagnostic(parts: list[str], label: str, output: str) -> None:
    """Keep useful command tails without retaining unbounded PACS output in RAM."""
    parts.append(f"{label}:\n{output[-4000:]}")
    while len(parts) > 1 and sum(map(len, parts)) > 20_000:
        parts.pop(0)


def folder_counts(unit: Unit) -> dict[str, int]:
    def count(p: str) -> int:
        path = Path(p)
        try:
            if not path.is_dir():
                return 0
            return sum(
                1
                for file in path.iterdir()
                if file.is_file() and not file.name.startswith(".")
            )
        except OSError:
            return 0

    return {
        "receive": count(unit.receive_dir),
        "send": count(unit.send_dir),
        "error": count(unit.error_dir),
    }
