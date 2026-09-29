"""Persistent, isolated processes for the compaction codec.

The native JPEG 2000 encoder can crash (segfault) or hang on a malformed
image. Each worker is a separate process that imports pydicom once and serves
jobs over a pipe; a crash or timeout only kills that worker, the caller gets
an exception and the next job starts a fresh process.
"""

from __future__ import annotations

import importlib
import logging
import multiprocessing
import threading
import time
from contextlib import suppress
from multiprocessing.connection import Connection

from app.observability import log_event

log = logging.getLogger("worker")

_CONTEXT = multiprocessing.get_context("spawn")
STARTUP_TIMEOUT_SECONDS = 120
# Repeated crashes usually mean a deterministic cause: wait before respawning.
_CRASH_WINDOW_SECONDS = 60.0
_CRASH_THRESHOLD = 3
_CRASH_DELAYS = (2.0, 5.0, 15.0, 30.0)


class CodecCrash(RuntimeError):
    """The worker process died while handling the job."""


class CodecTimeout(RuntimeError):
    """The job did not finish in time; the worker was killed."""


def _serve(conn: Connection, target: str) -> None:
    module_name, function_name = target.split(":", 1)
    function = getattr(importlib.import_module(module_name), function_name)
    conn.send(("ready", None))
    while True:
        try:
            job = conn.recv()
        except EOFError, OSError:
            return  # parent went away
        if job is None:
            return
        try:
            conn.send(("ok", function(job)))
        except Exception as exc:
            conn.send(("error", type(exc).__name__))


class _Worker:
    def __init__(self, target: str) -> None:
        self._conn, child_conn = _CONTEXT.Pipe()
        self._process = _CONTEXT.Process(
            target=_serve, args=(child_conn, target), name="codec-worker", daemon=True
        )
        self._process.start()
        child_conn.close()
        try:
            status, _ = self._receive(STARTUP_TIMEOUT_SECONDS)
        except Exception:
            self.kill()
            raise
        if status != "ready":
            self.kill()
            raise CodecCrash("worker did not start")

    def _receive(self, timeout: float):
        if not self._conn.poll(timeout):
            raise CodecTimeout
        try:
            return self._conn.recv()
        except (EOFError, OSError) as exc:
            raise CodecCrash(f"exit={self._process.exitcode}") from exc

    def call(self, job, timeout: float):
        try:
            self._conn.send(job)
        except (BrokenPipeError, OSError) as exc:
            raise CodecCrash(f"exit={self._process.exitcode}") from exc
        status, value = self._receive(timeout)
        if status != "ok":
            raise RuntimeError(f"codec job failed: {value}")
        return value

    def alive(self) -> bool:
        return self._process.is_alive()

    def kill(self) -> None:
        if self._process.is_alive():
            self._process.kill()
        self._process.join(5)
        self._conn.close()

    def close(self) -> None:
        with suppress(OSError):
            self._conn.send(None)
        self._process.join(5)
        self.kill()


class CodecPool:
    """Reuses idle workers; at most `size` stay alive between jobs."""

    def __init__(self, target: str, size: int) -> None:
        self._target = target
        self._size = max(1, size)
        self._idle: list[_Worker] = []
        self._lock = threading.Lock()
        self._crashes: list[float] = []
        self._crash_streak = 0

    def run(self, job, timeout: float):
        worker = self._acquire()
        try:
            result = worker.call(job, timeout)
        except CodecCrash, CodecTimeout:
            worker.kill()
            self._note_crash()
            raise
        except BaseException:
            worker.kill()
            raise
        self._release(worker)
        return result

    def close(self) -> None:
        with self._lock:
            idle, self._idle = self._idle, []
        for worker in idle:
            worker.close()

    def _acquire(self) -> _Worker:
        with self._lock:
            while self._idle:
                worker = self._idle.pop()
                if worker.alive():
                    return worker
                worker.kill()
        return _Worker(self._target)

    def _release(self, worker: _Worker) -> None:
        with self._lock:
            if len(self._idle) < self._size:
                self._idle.append(worker)
                self._crash_streak = 0
                return
        worker.close()

    def _note_crash(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._crashes = [
                moment
                for moment in self._crashes
                if now - moment < _CRASH_WINDOW_SECONDS
            ]
            self._crashes.append(now)
            if len(self._crashes) < _CRASH_THRESHOLD:
                return
            delay = _CRASH_DELAYS[min(self._crash_streak, len(_CRASH_DELAYS) - 1)]
            self._crash_streak += 1
            recent = len(self._crashes)
        log_event(
            log,
            logging.ERROR,
            "dicom.compact.codec.respawn",
            status="backoff",
            crash_count=recent,
            window_seconds=_CRASH_WINDOW_SECONDS,
            delay_seconds=delay,
        )
        time.sleep(delay)
