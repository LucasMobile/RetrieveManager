from __future__ import annotations

import logging
import signal
from concurrent.futures import ThreadPoolExecutor
from threading import Event

from sqlalchemy import select

from app.config import WORKER_HEALTH_FILE, WORKER_INTERVAL_SECONDS
from app.db import SessionLocal, init_db
from app.models import Unit
from app.observability import configure_logging, log_event
from app.pipeline import (
    claim_due_moves,
    compact_unit,
    find_pending,
    ingest_unit,
    recover_stale_locks,
    run_claimed_move,
    send_unit,
)
from app.rules import get_settings
from app.storescp import StoreSupervisor

log = logging.getLogger("worker")

stop_event = Event()


def _stop(*_args) -> None:
    stop_event.set()


def _move_job(order_id: int, second: bool) -> None:
    with SessionLocal() as db:
        try:
            run_claimed_move(db, order_id, second)
        except Exception as exc:
            log_event(
                log,
                logging.ERROR,
                "dicom.move.job",
                resource=f"order:{order_id}",
                status="failure",
                error=exc,
                order_id=order_id,
            )


def main() -> None:
    configure_logging()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    init_db()
    supervisor = StoreSupervisor()
    pool = ThreadPoolExecutor(max_workers=16)
    with SessionLocal() as db:
        units = list(db.scalars(select(Unit)))
        log_event(
            log,
            logging.INFO,
            "worker.start",
            resource="worker",
            status="success",
            unit_count=len(units),
            enabled_unit_count=sum(1 for unit in units if unit.enabled),
        )
    try:
        while not stop_event.is_set():
            try:
                _tick(supervisor, pool)
                WORKER_HEALTH_FILE.touch()
            except Exception as exc:
                log_event(
                    log,
                    logging.ERROR,
                    "worker.tick",
                    resource="worker",
                    status="failure",
                    error=exc,
                )
            stop_event.wait(WORKER_INTERVAL_SECONDS)
    finally:
        supervisor.stop_all()
        pool.shutdown(wait=False)
        WORKER_HEALTH_FILE.unlink(missing_ok=True)
        log_event(
            log,
            logging.INFO,
            "worker.stop",
            resource="worker",
            status="success",
        )


def _tick(supervisor: StoreSupervisor, pool: ThreadPoolExecutor) -> None:
    with SessionLocal() as db:
        recover_stale_locks(db)
        units = list(db.scalars(select(Unit)))
        supervisor.reconcile(units)
        settings = get_settings(db)
        for unit in units:
            if not unit.enabled:
                continue
            ingest_unit(db, unit)
            find_pending(db, unit)
            for order_id, second in claim_due_moves(db, unit):
                pool.submit(_move_job, order_id, second)
            compact_unit(db, unit)
            send_unit(db, unit, settings.cloud_url, settings.file_settle_seconds)


if __name__ == "__main__":
    main()
