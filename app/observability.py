from __future__ import annotations

import json
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from time import perf_counter
from typing import Any
from uuid import uuid4

from app.config import APP_ENV, LOG_LEVEL, SERVICE_NAME

correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="-")
user_id_var: ContextVar[int | None] = ContextVar("user_id", default=None)

_STANDARD_FIELDS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "taskName",
}
_RESERVED_EXTRA_FIELDS = _STANDARD_FIELDS | {"asctime", "message"}


class JsonFormatter(logging.Formatter):
    """One JSON object per line; values are deliberately operational, not clinical."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "correlation_id": getattr(
                record, "correlation_id", correlation_id_var.get()
            ),
            "service": getattr(record, "service", SERVICE_NAME),
            "env": APP_ENV,
            "action": getattr(record, "action", record.getMessage()),
            "resource": getattr(record, "resource", None),
            "user_id": getattr(record, "user_id", user_id_var.get()),
            "duration_ms": getattr(record, "duration_ms", None),
            "status": getattr(record, "status", None),
            "error_type": getattr(record, "error_type", None),
        }
        for key, value in record.__dict__.items():
            if (
                key not in _STANDARD_FIELDS
                and key not in payload
                and not key.startswith("_")
            ):
                payload[key] = value
        if record.exc_info:
            payload["error_type"] = record.exc_info[0].__name__
        return json.dumps(
            payload, ensure_ascii=False, default=str, separators=(",", ":")
        )


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(LOG_LEVEL)


def new_correlation_id() -> str:
    return str(uuid4())


@contextmanager
def log_context(correlation_id: str, *, user_id: int | None = None) -> Iterator[None]:
    correlation_token = correlation_id_var.set(correlation_id)
    user_token = user_id_var.set(user_id)
    try:
        yield
    finally:
        user_id_var.reset(user_token)
        correlation_id_var.reset(correlation_token)


def log_event(
    logger: logging.Logger,
    level: int,
    action: str,
    *,
    resource: str | int | None = None,
    status: str,
    started_at: float | None = None,
    error: BaseException | None = None,
    **fields: Any,
) -> None:
    extra: dict[str, Any] = {
        "action": action,
        "resource": resource,
        "status": status,
        "duration_ms": (
            round((perf_counter() - started_at) * 1000, 2)
            if started_at is not None
            else None
        ),
        "error_type": type(error).__name__ if error else None,
    }
    for key, value in fields.items():
        safe_key = f"event_{key}" if key in _RESERVED_EXTRA_FIELDS else key
        extra[safe_key] = value
    logger.log(level, action, extra=extra, exc_info=error is not None)
