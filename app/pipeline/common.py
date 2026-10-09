"""Helpers shared by every pipeline stage."""

from __future__ import annotations

import logging
import os

from app.models import (
    Order,
    Unit,
)
from app.observability import (
    new_correlation_id,
)

log = logging.getLogger("worker")


def ensure_order_correlation(order: Order) -> str:
    if not order.correlation_id:
        order.correlation_id = new_correlation_id()
    return order.correlation_id


def bounded_db_text(value: object, limit: int) -> str:
    """Normalize external PACS/file text before writing bounded DB columns."""
    return str(value or "").replace("\x00", " ").strip()[:limit]


def append_diagnostic(parts: list[str], label: str, output: str) -> None:
    """Keep useful command tails without retaining unbounded PACS output in RAM."""
    parts.append(f"{label}:\n{output[-4000:]}")
    while len(parts) > 1 and sum(map(len, parts)) > 20_000:
        parts.pop(0)


def folder_counts(unit: Unit) -> dict[str, int]:
    def count(p: str) -> int:
        # scandir reads the file type with each name: no stat() per file, which
        # matters with tens of thousands of files waiting in a folder.
        try:
            with os.scandir(p) as entries:
                return sum(
                    1
                    for entry in entries
                    if not entry.name.startswith(".") and entry.is_file()
                )
        except OSError:
            return 0

    return {
        "receive": count(unit.receive_dir),
        "send": count(unit.send_dir),
        "error": count(unit.error_dir),
    }
