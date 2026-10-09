from __future__ import annotations

import logging
import signal
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event, Thread
from time import monotonic, perf_counter

from sqlalchemy import select

from app.config import (
    COMPACT_UNIT_SCHEDULERS,
    FIND_BATCH_SIZE,
    FIND_ORDERS_PER_UNIT,
    FIND_UNIT_SCHEDULERS,
    MONITOR_BATCH_SIZE,
    MONITOR_CHECKS_PER_UNIT,
    MONITOR_UNIT_SCHEDULERS,
    ORDERS_API_ACK_UNIT_WORKERS,
    ORDERS_API_INGEST_UNIT_WORKERS,
    SEND_UNIT_SCHEDULERS,
    WORKER_HEALTH_FILE,
    WORKER_INTERVAL_SECONDS,
)
from app.db import INSTANCES_RECEIVED_CHANNEL, SessionLocal, init_db
from app.models import Unit
from app.notify import NotificationListener
from app.observability import configure_logging, log_event
from app.pipeline import (
    acknowledge_pending_orders,
    archive_completed_orders,
    check_monitoring,
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
from app.pipeline.send import close_upload_runtimes
from app.wakeup import COMPACT, SEND, wakeups

log = logging.getLogger("worker")

stop_event = Event()
_health_touch_error_logged = False

# Orphan locks (minutes old before they count), unmatched orders (24 h) and
# completed orders (14 days) need no faster sweep than this.
MAINTENANCE_INTERVAL_SECONDS = 60
_maintenance_due_at = 0.0


def _stop(*_args) -> None:
    stop_event.set()
    wakeups.poke()


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
                stage="orders.ack",
                unit_id=unit_id,
            )


def _ingest_job(unit_id: int) -> None:
    """Keep a slow PLERES GET off the batch-dispatch loop."""
    _run_unit_stage(unit_id, "orders.ingest", ingest_unit)


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
                unit_id=unit_id,
            )
    jobs[unit_id] = pool.submit(_ack_job, unit_id)


def _compact_job(unit_id: int) -> None:
    """Drain one unit's receive queue without blocking the orchestration loop."""
    while not stop_event.is_set():
        has_more = _run_unit_stage(
            unit_id,
            "dicom.compact.batch",
            lambda db, unit: compact_unit(db, unit, stop=stop_event),
        )
        if has_more is not True:
            return


def _find_job(unit_id: int) -> None:
    """Search the unit's due orders one at a time, each in its own session.

    The slot keeps searching while orders are due (up to FIND_BATCH_SIZE), so
    the C-FIND rate is not capped at one order per scheduler interval.
    """
    for _ in range(max(1, FIND_BATCH_SIZE)):
        if stop_event.is_set():
            return
        claimed = _run_unit_stage(
            unit_id,
            "dicom.find.order",
            lambda db, unit: find_pending(db, unit, max_orders=1),
        )
        if not claimed:
            return


def _monitor_job(unit_id: int) -> None:
    """Run the due monitoring checks of a unit, one order per session."""
    for _ in range(max(1, MONITOR_BATCH_SIZE)):
        if stop_event.is_set():
            return
        claimed = _run_unit_stage(
            unit_id,
            "dicom.monitor.check",
            lambda db, unit: check_monitoring(db, unit, max_orders=1),
        )
        if not claimed:
            return


def _schedule_find_jobs(
    pool: ThreadPoolExecutor,
    jobs: dict[tuple[int, int], Future],
    *,
    job=None,
    slots: int | None = None,
    stage: str = "dicom.find.order",
) -> None:
    """Keep a bounded number of C-FIND requests active per unit."""
    job = job or _find_job
    slots = FIND_ORDERS_PER_UNIT if slots is None else slots
    with SessionLocal() as db:
        unit_ids = list(
            db.scalars(
                select(Unit.id).where(Unit.deleted_at.is_(None), Unit.enabled.is_(True))
            )
        )
    for unit_id in unit_ids:
        for slot in range(max(1, slots)):
            key = (unit_id, slot)
            existing = jobs.get(key)
            if existing is not None and not existing.done():
                continue
            if existing is not None:
                try:
                    existing.result()
                except Exception as exc:
                    log_event(
                        log,
                        logging.ERROR,
                        "worker.stage.job",
                        resource=f"unit:{unit_id}",
                        status="failure",
                        error=exc,
                        stage=stage,
                        unit_id=unit_id,
                    )
            jobs[key] = pool.submit(job, unit_id)
    active_units = set(unit_ids)
    for key in list(jobs):
        if key[0] not in active_units and jobs[key].done():
            jobs.pop(key)


def _find_scheduler_loop(
    pool: ThreadPoolExecutor, monitor_pool: ThreadPoolExecutor | None = None
) -> None:
    """Schedule finds independently of API ingestion, compression and sending."""
    jobs: dict[tuple[int, int], Future] = {}
    monitor_jobs: dict[tuple[int, int], Future] = {}
    while not stop_event.is_set():
        try:
            _schedule_find_jobs(pool, jobs)
            if monitor_pool is not None:
                _schedule_find_jobs(
                    monitor_pool,
                    monitor_jobs,
                    job=_monitor_job,
                    slots=MONITOR_CHECKS_PER_UNIT,
                    stage="dicom.monitor.check",
                )
        except Exception as exc:
            log_event(
                log,
                logging.ERROR,
                "dicom.find.scheduler",
                resource="worker",
                status="failure",
                error=exc,
            )
        stop_event.wait(max(1, WORKER_INTERVAL_SECONDS))


def _send_job(unit_id: int) -> None:
    """Drain one unit's upload queue without blocking the orchestration loop."""
    while not stop_event.is_set():
        has_more = _run_unit_stage(
            unit_id,
            "cloud.send.batch",
            lambda db, unit: send_unit(db, unit, unit.cloud_url),
        )
        if has_more is not True:
            return


def _schedule_unit_job(
    pool: ThreadPoolExecutor,
    jobs: dict[int, Future],
    unit_id: int,
    callback,
    stage: str,
) -> bool:
    """Keep at most one background job of a given stage active per unit.

    Returns False when the unit's job is still running (or cannot start).
    """
    existing = jobs.get(unit_id)
    if existing is not None and not existing.done():
        return False
    if existing is not None:
        try:
            existing.result()
        except Exception as exc:
            log_event(
                log,
                logging.ERROR,
                "worker.stage.job",
                resource=f"unit:{unit_id}",
                status="failure",
                error=exc,
                stage=stage,
                unit_id=unit_id,
            )
    try:
        jobs[unit_id] = pool.submit(callback, unit_id)
    except RuntimeError as exc:
        log_event(
            log,
            logging.WARNING,
            "worker.stage.job",
            resource=f"unit:{unit_id}",
            status="skipped",
            error=exc,
            stage=stage,
            unit_id=unit_id,
        )
        return False
    return True


def _dispatch_wakeups(
    requests: dict[str, set[int]],
    compact_pool: ThreadPoolExecutor,
    compact_jobs: dict[int, Future],
    send_pool: ThreadPoolExecutor,
    send_jobs: dict[int, Future],
) -> None:
    """Start the stages other stages asked for, without waiting for a tick.

    A unit whose job is still running is asked again when it ends: the job
    may have read its queue before the new work was committed.
    """
    targets = {
        COMPACT: (compact_pool, compact_jobs, _compact_job, "dicom.compact.batch"),
        SEND: (send_pool, send_jobs, _send_job, "cloud.send.batch"),
    }
    for stage, unit_ids in requests.items():
        pool, jobs, callback, stage_name = targets[stage]
        for unit_id in unit_ids:
            if stop_event.is_set():
                return
            if not _schedule_unit_job(pool, jobs, unit_id, callback, stage_name):
                running = jobs.get(unit_id)
                if running is not None and not running.done():
                    wakeups.request_after(stage, unit_id, running)


def _received_listener_loop(listener: NotificationListener) -> None:
    """Turn the receiver's NOTIFY of new instances into compaction wakeups."""
    try:
        while not stop_event.is_set():
            payload = listener.wait(stop_event, WORKER_INTERVAL_SECONDS)
            if payload and payload.isdecimal():
                wakeups.request(COMPACT, int(payload))
    finally:
        listener.close()


def main() -> None:
    configure_logging()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    init_db()
    pool = ThreadPoolExecutor(max_workers=16, thread_name_prefix="dicom-move")
    ack_pool = ThreadPoolExecutor(
        max_workers=ORDERS_API_ACK_UNIT_WORKERS,
        thread_name_prefix="orders-ack",
    )
    ingest_pool = ThreadPoolExecutor(
        max_workers=max(1, ORDERS_API_INGEST_UNIT_WORKERS),
        thread_name_prefix="orders-ingest",
    )
    find_pool = ThreadPoolExecutor(
        max_workers=max(1, FIND_UNIT_SCHEDULERS),
        thread_name_prefix="unit-find",
    )
    monitor_pool = ThreadPoolExecutor(
        max_workers=max(1, MONITOR_UNIT_SCHEDULERS),
        thread_name_prefix="unit-monitor",
    )
    find_scheduler = Thread(
        target=_find_scheduler_loop,
        args=(find_pool, monitor_pool),
        name="find-scheduler",
        daemon=True,
    )
    compact_pool = ThreadPoolExecutor(
        max_workers=max(1, COMPACT_UNIT_SCHEDULERS),
        thread_name_prefix="unit-compact",
    )
    send_pool = ThreadPoolExecutor(
        max_workers=max(1, SEND_UNIT_SCHEDULERS),
        thread_name_prefix="unit-send",
    )
    ack_jobs: dict[int, Future] = {}
    ingest_jobs: dict[int, Future] = {}
    compact_jobs: dict[int, Future] = {}
    send_jobs: dict[int, Future] = {}
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
    received_listener = Thread(
        target=_received_listener_loop,
        args=(
            NotificationListener(
                INSTANCES_RECEIVED_CHANNEL, logger=log, action="worker.listen"
            ),
        ),
        name="received-listener",
        daemon=True,
    )
    find_scheduler.start()
    received_listener.start()
    next_tick = 0.0
    try:
        while not stop_event.is_set():
            # A healthcheck deve provar que o loop está vivo mesmo quando uma etapa
            # legítima (por exemplo, compactação) ocupa vários segundos.
            _touch_health()
            try:
                if monotonic() >= next_tick:
                    _tick(
                        pool,
                        ack_pool,
                        ack_jobs,
                        compact_pool,
                        compact_jobs,
                        send_pool,
                        send_jobs,
                        ingest_pool=ingest_pool,
                        ingest_jobs=ingest_jobs,
                    )
                    next_tick = monotonic() + WORKER_INTERVAL_SECONDS
                    _touch_health()
                # Work announced by the receiver or by the compaction starts at
                # once; the tick above remains the guarantee for everything.
                _dispatch_wakeups(
                    wakeups.take(), compact_pool, compact_jobs, send_pool, send_jobs
                )
            except Exception as exc:
                log_event(
                    log,
                    logging.ERROR,
                    "worker.tick",
                    resource="worker",
                    status="failure",
                    error=exc,
                )
                next_tick = monotonic() + WORKER_INTERVAL_SECONDS
            wakeups.wait(next_tick - monotonic())
    finally:
        stop_event.set()
        find_scheduler.join(timeout=5)
        received_listener.join(timeout=5)
        pool.shutdown(wait=True)
        ack_pool.shutdown(wait=True, cancel_futures=True)
        ingest_pool.shutdown(wait=True, cancel_futures=True)
        find_pool.shutdown(wait=True, cancel_futures=True)
        monitor_pool.shutdown(wait=True, cancel_futures=True)
        compact_pool.shutdown(wait=True, cancel_futures=True)
        send_pool.shutdown(wait=True, cancel_futures=True)
        close_upload_runtimes()
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
            )
        log_event(
            log,
            logging.INFO,
            "worker.stop",
            resource="worker",
            status="success",
        )


def _tick(
    pool: ThreadPoolExecutor,
    ack_pool: ThreadPoolExecutor | None = None,
    ack_jobs: dict[int, Future] | None = None,
    compact_pool: ThreadPoolExecutor | None = None,
    compact_jobs: dict[int, Future] | None = None,
    send_pool: ThreadPoolExecutor | None = None,
    send_jobs: dict[int, Future] | None = None,
    ingest_pool: ThreadPoolExecutor | None = None,
    ingest_jobs: dict[int, Future] | None = None,
) -> None:
    global _maintenance_due_at
    if monotonic() >= _maintenance_due_at:
        _run_db_stage("locks.recover", recover_stale_locks)
        _run_db_stage("orders.cleanup_unmatched", cleanup_unmatched_orders)
        _run_db_stage("orders.archive_completed", archive_completed_orders)
        _maintenance_due_at = monotonic() + MAINTENANCE_INTERVAL_SECONDS

    with SessionLocal() as db:
        units = list(db.scalars(select(Unit).where(Unit.deleted_at.is_(None))))
        unit_ids = [unit.id for unit in units if unit.enabled]
        db.commit()

    ack_executor = ack_pool or pool
    active_ack_jobs = ack_jobs if ack_jobs is not None else {}
    for unit_id in unit_ids:
        if ingest_pool is not None and ingest_jobs is not None:
            _schedule_unit_job(
                ingest_pool,
                ingest_jobs,
                unit_id,
                _ingest_job,
                "orders.ingest",
            )
        else:
            _run_unit_stage(unit_id, "orders.ingest", ingest_unit)
        _schedule_ack_job(
            ack_executor,
            active_ack_jobs,
            unit_id,
        )
        claims = _run_unit_stage(unit_id, "dicom.move.claim", claim_due_moves) or []
        for resource_id, kind in claims:
            try:
                pool.submit(_move_job, resource_id, kind)
            except Exception as exc:
                _persist_dispatch_failure(resource_id, kind, exc)
        if compact_pool is not None and compact_jobs is not None:
            _schedule_unit_job(
                compact_pool,
                compact_jobs,
                unit_id,
                _compact_job,
                "dicom.compact.batch",
            )
        else:
            _run_unit_stage(unit_id, "dicom.compact.batch", compact_unit)
        if send_pool is not None and send_jobs is not None:
            _schedule_unit_job(
                send_pool,
                send_jobs,
                unit_id,
                _send_job,
                "cloud.send.batch",
            )
        else:
            _run_unit_stage(
                unit_id,
                "cloud.send.batch",
                lambda db, unit: send_unit(db, unit, unit.cloud_url),
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
                resource_id=resource_id,
                move_kind=kind,
            )


if __name__ == "__main__":
    main()
