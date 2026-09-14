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
    archive_completed_orders,
    claim_due_moves,
    cleanup_unmatched_orders,
    compact_unit,
    find_pending,
    ingest_unit,
    recover_stale_locks,
    run_claimed_move,
    send_unit,
)
from app.storescp import StoreSupervisor

log = logging.getLogger("worker")

stop_event = Event()


def _stop(*_args) -> None:
    stop_event.set()


def _move_job(resource_id: int, kind: str) -> None:
    with SessionLocal() as db:
        try:
            run_claimed_move(db, resource_id, kind)
        except Exception as exc:
            log_event(
                log,
                logging.ERROR,
                "dicom.move.job",
                resource=f"{kind}:{resource_id}",
                status="failure",
                error=exc,
                resource_id=resource_id,
                move_kind=kind,
            )


def main() -> None:
    configure_logging()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    init_db()
    supervisor = StoreSupervisor()
    pool = ThreadPoolExecutor(max_workers=16)
    with SessionLocal() as db:
        units = list(db.scalars(select(Unit).where(Unit.deleted_at.is_(None))))
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
                    error_detail=str(getattr(exc, "orig", type(exc).__name__))[:500],
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
        cleanup_unmatched_orders(db)
        archive_completed_orders(db)
        units = list(db.scalars(select(Unit).where(Unit.deleted_at.is_(None))))
        supervisor.reconcile(units)
        for unit in units:
            if not unit.enabled:
                continue
            ingest_unit(db, unit)
            find_pending(db, unit)
            for resource_id, kind in claim_due_moves(db, unit):
                pool.submit(_move_job, resource_id, kind)
            compact_unit(db, unit)
            send_unit(db, unit, unit.cloud_url, unit.file_settle_seconds)


if __name__ == "__main__":
    main()
