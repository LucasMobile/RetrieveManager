"""PostgreSQL LISTEN on a dedicated connection, with polling as the fallback."""

from __future__ import annotations

import logging
import threading
from contextlib import suppress
from time import monotonic

import psycopg
from sqlalchemy import URL

from app.config import DATABASE_URL
from app.observability import log_event


class NotificationListener:
    """Waits for a NOTIFY on one channel; the caller's timeout stays the cap.

    Notifications sent while the caller is busy queue on the listening
    connection, so none is lost. While the connection is down, the wait falls
    back to plain polling and reconnects on the next call.
    """

    def __init__(
        self,
        channel: str,
        *,
        url: URL = DATABASE_URL,
        logger: logging.Logger,
        action: str,
    ) -> None:
        self._channel = channel
        self._url = url
        self._log = logger
        self._action = action
        self._connection: psycopg.Connection | None = None
        self._failing = False

    def _connect(self) -> psycopg.Connection:
        if self._connection is None or self._connection.closed:
            connection = psycopg.connect(
                host=self._url.host,
                port=self._url.port,
                dbname=self._url.database,
                user=self._url.username,
                password=self._url.password,
                autocommit=True,
                connect_timeout=5,
            )
            connection.execute(f"LISTEN {self._channel}")
            self._connection = connection
        return self._connection

    def wait(self, stop: threading.Event, timeout: float) -> str | None:
        """The payload of the next notification; None after ``timeout`` or on stop."""
        deadline = monotonic() + timeout
        while not stop.is_set():
            remaining = deadline - monotonic()
            if remaining <= 0:
                return None
            # Short slices keep SIGTERM handling as prompt as stop.wait().
            slice_seconds = min(remaining, 1.0)
            try:
                connection = self._connect()
                for notify in connection.notifies(timeout=slice_seconds, stop_after=1):
                    self._recovered()
                    return notify.payload
                self._recovered()
            except psycopg.Error as exc:
                self.close()
                if not self._failing:
                    self._failing = True
                    log_event(
                        self._log,
                        logging.WARNING,
                        self._action,
                        status="failure",
                        error=exc,
                    )
                stop.wait(slice_seconds)
        return None

    def _recovered(self) -> None:
        if self._failing:
            self._failing = False
            log_event(self._log, logging.INFO, self._action, status="success")

    def close(self) -> None:
        if self._connection is not None:
            with suppress(Exception):
                self._connection.close()
            self._connection = None
