"""Requests to run a worker stage for a unit before the next scheduler tick.

The worker polls every unit every WORKER_INTERVAL_SECONDS; a stage that just
produced work for the next one (the receiver committing instances, the
compaction publishing files) asks for that stage here, and the worker's main
loop starts it at once.
"""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import Future
from threading import Event, Lock

COMPACT = "compact"
SEND = "send"


class Wakeups:
    def __init__(self) -> None:
        self._lock = Lock()
        self._event = Event()
        self._requests: dict[str, set[int]] = defaultdict(set)
        self._deferred: set[tuple[str, int]] = set()

    def request(self, stage: str, unit_id: int) -> None:
        with self._lock:
            self._requests[stage].add(unit_id)
            self._event.set()

    def request_after(self, stage: str, unit_id: int, job: Future) -> None:
        """Ask again when ``job`` ends: a running job may have missed the work.

        One request per stage and unit is kept, however often it is deferred.
        """
        key = (stage, unit_id)
        with self._lock:
            if key in self._deferred:
                return
            self._deferred.add(key)

        def again(_job: Future) -> None:
            with self._lock:
                self._deferred.discard(key)
            self.request(stage, unit_id)

        job.add_done_callback(again)

    def poke(self) -> None:
        """Wake the waiting loop without a request (shutdown)."""
        self._event.set()

    def wait(self, timeout: float) -> bool:
        return self._event.wait(max(0.0, timeout))

    def take(self) -> dict[str, set[int]]:
        """The pending requests, by stage; the next wait blocks until a new one."""
        with self._lock:
            self._event.clear()
            requests, self._requests = self._requests, defaultdict(set)
        return dict(requests)


wakeups = Wakeups()
