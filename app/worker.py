from __future__ import annotations

import logging
import signal
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event
from time import perf_counter

from sqlalchemy import select

from app.config import (
    ORDERS_API_ACK_UNIT_WORKERS,
    WORKER_HEALTH_FILE,
    WORKER_INTERVAL_SECONDS,
)
from app.db import SessionLocal, init_db
from app.models import Unit
from app.observability import configure_logging, log_event, safe_error_detail
from app.pipeline import (
    acknowledge_pending_orders,
    archive_completed_orders,
    claim_due_moves,
    cleanup_unmatched_orders,
    compact_unit,
    fail_claimed_move,
    find_pending,
    ingest_unit,
    recover_stale_locks,
    run_claimed_move,
    send_unit,
)
from app.storescp import StoreSupervisor

log = logging.getLogger("worker")

stop_event = Event()
_health_touch_error_logged = False


def _stop(*_args) -> None:
    stop_event.set()


def _touch_health() -> None:
    global _health_touch_error_logged
    try:
        WORKER_HEALTH_FILE.touch()
        _health_touch_error_logged = False
    except OSError as exc:
        if not _health_touch_error_logged:
            log_event(
                log,
                logging.ERROR,
                "worker.health",
                resource="worker",
                status="failure",
                error=exc,
                error_detail=safe_error_detail(exc),
            )
            _health_touch_error_logged = True


def _move_job(resource_id: int, kind: str) -> None:
    with SessionLocal() as db:
        try:
            run_claimed_move(db, resource_id, kind)
        except Exception as exc:
            db.rollback()
            try:
                fail_claimed_move(db, resource_id, kind, exc)
            except Exception as persist_exc:
                db.rollback()
                log_event(
                    log,
                    logging.CRITICAL,
                    "dicom.move.job.persist_failure",
                    resource=f"{kind}:{resource_id}",
                    status="failure",
                    error=persist_exc,
                    error_detail=safe_error_detail(persist_exc),
                    original_error_type=type(exc).__name__,
                    resource_id=resource_id,
                    move_kind=kind,
                )
            log_event(
                log,
                logging.ERROR,
                "dicom.move.job",
                resource=f"{kind}:{resource_id}",
                status="failure",
                error=exc,
                error_detail=safe_error_detail(exc),
                resource_id=resource_id,
                move_kind=kind,
            )


def _ack_job(unit_id: int) -> None:
    """Run one bounded ACK batch without occupying the worker's main loop."""
    with SessionLocal() as db:
        unit = db.get(Unit, unit_id)
        if unit is None or unit.deleted_at is not None or not unit.enabled:
            return
        try:
            acknowledge_pending_orders(db, unit)
        except Exception as exc:
            db.rollback()
            log_event(
                log,
                logging.ERROR,
                "worker.stage",
                resource=f"unit:{unit_id}",
                status="failure",
                error=exc,
                error_detail=safe_error_detail(exc),
                stage="orders.ack",
                unit_id=unit_id,
            )


def _schedule_ack_job(
    pool: ThreadPoolExecutor,
    jobs: dict[int, Future],
    unit_id: int,
) -> None:
    existing = jobs.get(unit_id)
    if existing is not None and not existing.done():
        return
    if existing is not None:
        # _ack_job contains its own errors, but consuming the result also keeps
        # unexpected executor failures observable and releases references.
        try:
            existing.result()
        except Exception as exc:
            log_event(
                log,
                logging.ERROR,
                "orders.api.ack.job",
                resource=f"unit:{unit_id}",
                status="failure",
                error=exc,
                error_detail=safe_error_detail(exc),
                unit_id=unit_id,
            )
    jobs[unit_id] = pool.submit(_ack_job, unit_id)


def main() -> None:
    configure_logging()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    init_db()
    supervisor = StoreSupervisor()
    pool = ThreadPoolExecutor(max_workers=16, thread_name_prefix="dicom-move")
    ack_pool = ThreadPoolExecutor(
        max_workers=ORDERS_API_ACK_UNIT_WORKERS,
        thread_name_prefix="orders-ack",
    )
    ack_jobs: dict[int, Future] = {}
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
            # A healthcheck deve provar que o loop está vivo mesmo quando uma etapa
            # legítima (por exemplo, compactação) ocupa vários segundos.
            _touch_health()
            try:
                _tick(supervisor, pool, ack_pool, ack_jobs)
                _touch_health()
            except Exception as exc:
                log_event(
                    log,
                    logging.ERROR,
                    "worker.tick",
                    resource="worker",
                    status="failure",
                    error=exc,
                    error_detail=safe_error_detail(exc),
                )
            stop_event.wait(WORKER_INTERVAL_SECONDS)
    finally:
        supervisor.stop_all()
        pool.shutdown(wait=False)
        ack_pool.shutdown(wait=False, cancel_futures=True)
        try:
            WORKER_HEALTH_FILE.unlink(missing_ok=True)
        except OSError as exc:
            log_event(
                log,
                logging.WARNING,
                "worker.health.cleanup",
                resource="worker",
                status="failure",
                error=exc,
                error_detail=safe_error_detail(exc),
            )
        log_event(
            log,
            logging.INFO,
            "worker.stop",
            resource="worker",
            status="success",
        )


def _tick(
    supervisor: StoreSupervisor,
    pool: ThreadPoolExecutor,
    ack_pool: ThreadPoolExecutor | None = None,
    ack_jobs: dict[int, Future] | None = None,
) -> None:
    _run_db_stage("locks.recover", recover_stale_locks)
    _run_db_stage("orders.cleanup_unmatched", cleanup_unmatched_orders)
    _run_db_stage("orders.archive_completed", archive_completed_orders)

    with SessionLocal() as db:
        units = list(db.scalars(select(Unit).where(Unit.deleted_at.is_(None))))
        unit_ids = [unit.id for unit in units if unit.enabled]
        try:
            supervisor.reconcile(units)
        except Exception as exc:
            log_event(
                log,
                logging.ERROR,
                "worker.stage",
                resource="storescp",
                status="failure",
                error=exc,
                error_detail=safe_error_detail(exc),
                stage="storescp.reconcile",
            )

    ack_executor = ack_pool or pool
    active_ack_jobs = ack_jobs if ack_jobs is not None else {}
    for unit_id in unit_ids:
        _run_unit_stage(unit_id, "orders.ingest", ingest_unit)
        _schedule_ack_job(
            ack_executor,
            active_ack_jobs,
            unit_id,
        )
        _run_unit_stage(unit_id, "dicom.find.batch", find_pending)
        claims = _run_unit_stage(unit_id, "dicom.move.claim", claim_due_moves) or []
        for resource_id, kind in claims:
            try:
                pool.submit(_move_job, resource_id, kind)
            except Exception as exc:
                _persist_dispatch_failure(resource_id, kind, exc)
        _run_unit_stage(unit_id, "dicom.compact.batch", compact_unit)
        _run_unit_stage(
            unit_id,
            "cloud.send.batch",
            lambda db, unit: send_unit(
                db, unit, unit.cloud_url, unit.file_settle_seconds
            ),
        )


def _run_db_stage(stage: str, callback):
    _touch_health()
    started_at = perf_counter()
    with SessionLocal() as db:
        try:
            result = callback(db)
        except Exception as exc:
            db.rollback()
            log_event(
                log,
                logging.ERROR,
                "worker.stage",
                resource="worker",
                status="failure",
                started_at=started_at,
                error=exc,
                error_detail=safe_error_detail(exc),
                stage=stage,
            )
            return None
    log_event(
        log,
        logging.DEBUG,
        "worker.stage",
        resource="worker",
        status="success",
        started_at=started_at,
        stage=stage,
    )
    return result


def _run_unit_stage(unit_id: int, stage: str, callback):
    _touch_health()
    def invoke(db):
        unit = db.get(Unit, unit_id)
        if unit is None or unit.deleted_at is not None or not unit.enabled:
            return None
        return callback(db, unit)

    started_at = perf_counter()
    with SessionLocal() as db:
        try:
            result = invoke(db)
        except Exception as exc:
            db.rollback()
            log_event(
                log,
                logging.ERROR,
                "worker.stage",
                resource=f"unit:{unit_id}",
                status="failure",
                started_at=started_at,
                error=exc,
                error_detail=safe_error_detail(exc),
                stage=stage,
                unit_id=unit_id,
            )
            return None
    log_event(
        log,
        logging.DEBUG,
        "worker.stage",
        resource=f"unit:{unit_id}",
        status="success",
        started_at=started_at,
        stage=stage,
        unit_id=unit_id,
    )
    return result


def _persist_dispatch_failure(resource_id: int, kind: str, exc: Exception) -> None:
    with SessionLocal() as db:
        try:
            fail_claimed_move(db, resource_id, kind, exc)
        except Exception as persist_exc:
            db.rollback()
            log_event(
                log,
                logging.CRITICAL,
                "dicom.move.dispatch.persist_failure",
                resource=f"{kind}:{resource_id}",
                status="failure",
                error=persist_exc,
                error_detail=safe_error_detail(persist_exc),
                resource_id=resource_id,
                move_kind=kind,
            )


if __name__ == "__main__":
    main()
