from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from ipaddress import ip_address
from math import ceil
from threading import Lock
from time import monotonic, time

from fastapi import Request
from fastapi.responses import Response

from app.config import (
    GLOBAL_RATE_LIMIT_REQUESTS,
    GLOBAL_RATE_LIMIT_WINDOW_SECONDS,
    LOGIN_RATE_LIMIT_FAILURES,
    LOGIN_RATE_LIMIT_WINDOW_SECONDS,
)


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    allowed: bool
    limit: int
    remaining: int
    window_seconds: int
    reset_after: int


class SlidingWindowRateLimiter:
    """Thread-safe, bounded sliding-window limiter for one application process."""

    def __init__(
        self,
        limit: int,
        window_seconds: int,
        *,
        clock: Callable[[], float] = monotonic,
        max_keys: int = 100_000,
    ) -> None:
        if limit < 1 or window_seconds < 1 or max_keys < 1:
            raise ValueError("limit, window_seconds e max_keys devem ser positivos")
        self.limit = limit
        self.window_seconds = window_seconds
        self._clock = clock
        self._max_keys = max_keys
        self._events: dict[str, deque[float]] = {}
        self._lock = Lock()
        self._last_cleanup = 0.0

    def check(self, key: str, *, consume: bool = False) -> RateLimitDecision:
        now = self._clock()
        with self._lock:
            self._cleanup(now)
            events = self._events.get(key)
            if events is not None:
                self._prune(events, now)
                if not events:
                    self._events.pop(key, None)
                    events = None

            allowed = events is None or len(events) < self.limit
            if allowed and consume:
                if events is None:
                    if len(self._events) >= self._max_keys:
                        return RateLimitDecision(
                            allowed=False,
                            limit=self.limit,
                            remaining=0,
                            window_seconds=self.window_seconds,
                            reset_after=self.window_seconds,
                        )
                    events = deque()
                    self._events[key] = events
                events.append(now)

            used = len(events) if events is not None else 0
            reset_after = (
                max(1, ceil(self.window_seconds - (now - events[0]))) if events else 0
            )
            return RateLimitDecision(
                allowed=allowed,
                limit=self.limit,
                remaining=max(0, self.limit - used),
                window_seconds=self.window_seconds,
                reset_after=reset_after,
            )

    def reset(self, key: str | None = None) -> None:
        with self._lock:
            if key is None:
                self._events.clear()
                self._last_cleanup = 0.0
            else:
                self._events.pop(key, None)

    def _prune(self, events: deque[float], now: float) -> None:
        cutoff = now - self.window_seconds
        while events and events[0] <= cutoff:
            events.popleft()

    def _cleanup(self, now: float) -> None:
        if now - self._last_cleanup < self.window_seconds:
            return
        for key, events in list(self._events.items()):
            self._prune(events, now)
            if not events:
                self._events.pop(key, None)
        self._last_cleanup = now


global_rate_limiter = SlidingWindowRateLimiter(
    GLOBAL_RATE_LIMIT_REQUESTS,
    GLOBAL_RATE_LIMIT_WINDOW_SECONDS,
)
login_failure_rate_limiter = SlidingWindowRateLimiter(
    LOGIN_RATE_LIMIT_FAILURES,
    LOGIN_RATE_LIMIT_WINDOW_SECONDS,
)


def client_ip(request: Request) -> str:
    """Use the ASGI server's validated client address, never raw proxy headers."""
    host = request.client.host if request.client else "unknown"
    try:
        return ip_address(host).compressed
    except ValueError:
        return host.strip().lower()[:255] or "unknown"


def apply_rate_limit_headers(
    response: Response,
    decision: RateLimitDecision,
    *,
    scope: str,
    overwrite: bool = True,
) -> Response:
    headers = {
        "X-RateLimit-Limit": str(decision.limit),
        "X-RateLimit-Remaining": str(decision.remaining),
        "X-RateLimit-Reset": str(ceil(time() + decision.reset_after)),
        "X-RateLimit-Window": str(decision.window_seconds),
        "X-RateLimit-Scope": scope,
    }
    for name, value in headers.items():
        if overwrite or name not in response.headers:
            response.headers[name] = value
    if not decision.allowed:
        response.headers["Retry-After"] = str(max(1, decision.reset_after))
    return response


def reset_rate_limiters() -> None:
    """Clear process-local state, primarily for isolated tests."""
    global_rate_limiter.reset()
    login_failure_rate_limiter.reset()
