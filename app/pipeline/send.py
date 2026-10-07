"""Cloud upload: FIFO stream, retries, exhausted-upload probe and resend."""

from __future__ import annotations

import asyncio
import logging
import random
import stat
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import BoundedSemaphore
from time import monotonic, perf_counter

import aiohttp
from sqlalchemy import and_, or_, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.config import (
    CIRCUIT_BREAKER_FAILURES,
    CIRCUIT_BREAKER_SECONDS,
    HTTP_CONNECT_TIMEOUT_SECONDS,
    HTTP_TOTAL_TIMEOUT_SECONDS,
    SEND_DB_BATCH_SIZE,
    SEND_DB_FLUSH_MILLISECONDS,
    SEND_DRAIN_SECONDS,
    SEND_GLOBAL_CONCURRENCY,
    SEND_MAX_ATTEMPTS,
)
from app.models import (
    ImageTransfer,
    Unit,
)
from app.observability import (
    log_context,
    log_event,
)
from app.pipeline.common import bounded_db_text, log

# After the attempts run out, one exhausted upload is tried this often; when it
# succeeds the unit's send_error transfers return to the queue.
SEND_ERROR_PROBE_SECONDS = 300


# Wait (seconds) after each failed upload; the last value repeats.
SEND_RETRY_DELAYS = (10, 30, 60, 120, 300, 600, 1800)


_send_slots = BoundedSemaphore(max(1, SEND_GLOBAL_CONCURRENCY))


@dataclass(frozen=True)
class SendResult:
    transfer_id: int
    correlation_id: str
    success: bool
    http_status: int | None = None
    error_type: str = ""
    duration_ms: float = 0.0


@dataclass(frozen=True)
class DeliveryRoute:
    """Immutable input shared with the result writer, without an ORM session."""

    id: int
    send_dir: str


@dataclass
class CircuitState:
    failures: int = 0
    open_until: datetime | None = None


@dataclass
class SendErrorProbe:
    next_at: float = 0.0
    after_id: int = 0


_cloud_circuits: dict[int, CircuitState] = {}


_send_error_probes: dict[int, SendErrorProbe] = {}


def _due_send_transfers(
    db: Session,
    unit_id: int,
    current_time: datetime,
    after_id: int,
    limit: int,
) -> list[ImageTransfer]:
    """Due uploads in FIFO order; retry cooldowns are filtered in SQL."""
    return list(
        db.scalars(
            select(ImageTransfer)
            .where(
                ImageTransfer.unit_id == unit_id,
                ImageTransfer.id > after_id,
                # Redundant with the OR below; lets PostgreSQL match the
                # partial pending-queue index.
                ImageTransfer.status.in_(("compressed", "upload_error")),
                or_(
                    ImageTransfer.status == "compressed",
                    and_(
                        ImageTransfer.status == "upload_error",
                        or_(
                            ImageTransfer.next_attempt_at.is_(None),
                            ImageTransfer.next_attempt_at <= current_time,
                        ),
                    ),
                ),
            )
            .order_by(ImageTransfer.id)
            .limit(limit)
        )
    )


def _next_send_jobs(
    db: Session, unit: Unit, after_id: int, limit: int
) -> tuple[list[tuple[int, Path, str]], int, bool, int]:
    """Take the next due transfers after `after_id`.

    Returns the upload jobs, the new cursor, whether the queue was exhausted and
    how many transfers had no file. No transaction stays open afterwards.
    """
    origin = Path(unit.send_dir)
    rows = _due_send_transfers(db, unit.id, datetime.now(), after_id, limit)
    jobs: list[tuple[int, Path, str]] = []
    missing = 0
    for transfer in rows:
        path = origin / transfer.filename
        try:
            is_file = stat.S_ISREG(path.stat().st_mode)
        except FileNotFoundError:
            is_file = False
        except OSError as exc:
            # Left for the next drain: the cursor already moved past it.
            log_event(
                log,
                logging.WARNING,
                "cloud.file.inspect",
                resource=f"transfer:{transfer.id}",
                status="retry",
                error=exc,
                transfer_id=transfer.id,
                unit_id=unit.id,
            )
            continue
        if not is_file:
            transfer.status = "file_missing"
            transfer.last_error = "arquivo não encontrado na pasta de envio"
            transfer.next_attempt_at = None
            missing += 1
            continue
        jobs.append((transfer.id, path, transfer.correlation_id))
    cursor = rows[-1].id if rows else after_id
    exhausted = len(rows) < limit
    db.commit()
    return jobs, cursor, exhausted, missing


def _circuit_open(circuit: CircuitState) -> bool:
    return bool(circuit.open_until and datetime.now() < circuit.open_until)


def send_unit(db: Session, unit: Unit, cloud_url: str) -> bool:
    """Upload the unit's due transfers; report whether more may be waiting."""
    origin = Path(unit.send_dir)
    if not cloud_url:
        return False
    # Uma indisponibilidade em uma unidade não deve abrir o circuito das demais,
    # mesmo quando elas usam o mesmo endpoint de nuvem.
    circuit = _cloud_circuits.setdefault(unit.id, CircuitState())
    if _circuit_open(circuit):
        return False
    try:
        if not stat.S_ISDIR(origin.stat().st_mode):
            raise NotADirectoryError(str(origin))
    except OSError as exc:
        log_event(
            log,
            logging.ERROR,
            "cloud.send.directory",
            resource=f"unit:{unit.id}",
            status="failure",
            error=exc,
            unit_id=unit.id,
            directory=str(origin),
        )
        return False
    return asyncio.run(_stream_uploads(db, unit, cloud_url, circuit))


async def _stream_uploads(
    db: Session, unit: Unit, cloud_url: str, circuit: CircuitState
) -> bool:
    """Keep the unit's upload slots busy, refilling from the FIFO queue.

    A slow upload holds only its own slot. Up to twice the slots are queued so a
    finished upload is replaced at once; the queue is read again when fewer than
    one slot's worth remains. Refilling stops after SEND_DRAIN_SECONDS or when
    the circuit opens; uploads already started are allowed to finish.
    """
    workers = max(1, unit.send_workers or 16)
    semaphore = asyncio.Semaphore(workers)
    deadline = monotonic() + SEND_DRAIN_SECONDS
    chunk_size = max(1, SEND_DB_BATCH_SIZE)
    flush_seconds = SEND_DB_FLUSH_MILLISECONDS / 1000
    bind = db.get_bind()
    unit_id = unit.id
    delivery_route = DeliveryRoute(unit_id, unit.send_dir)
    totals = {"files": 0, "success": 0, "failure": 0, "missing": 0}
    started_at = perf_counter()
    cursor = 0
    exhausted = False
    pending: set[asyncio.Task] = set()
    buffer: list[SendResult] = []
    buffered_at = monotonic()

    def persist_results(results: list[SendResult]) -> tuple[int, int]:
        # Runs in a writer thread. Never pass the caller's Session
        # or ORM instances across threads; keep HTTP's event loop responsive.
        with Session(bind=bind, expire_on_commit=False) as result_db:
            return _record_send_results(
                result_db, delivery_route, results, circuit, update_circuit=False
            )

    async def flush() -> None:
        results = list(buffer)
        buffer.clear()
        for offset in range(0, len(results), chunk_size):
            chunk = results[offset : offset + chunk_size]
            failures, successes = await asyncio.to_thread(persist_results, chunk)
            _update_send_circuit(unit_id, circuit, failures, len(chunk))
            totals["files"] += len(chunk)
            totals["failure"] += failures
            totals["success"] += successes

    async with _upload_session() as session:
        await _probe_send_errors(db, unit, cloud_url, session)
        try:
            while True:
                if (
                    len(pending) < workers
                    and not exhausted
                    and (cursor == 0 or monotonic() < deadline)
                    and not _circuit_open(circuit)
                ):
                    jobs, cursor, exhausted, missing = _next_send_jobs(
                        db, unit, cursor, 2 * workers - len(pending)
                    )
                    totals["missing"] += missing
                    if not jobs and not pending:
                        continue  # only absent files in this slice: read on
                    for transfer_id, path, correlation_id in jobs:
                        pending.add(
                            asyncio.create_task(
                                _send_one(
                                    session,
                                    semaphore,
                                    transfer_id,
                                    path,
                                    cloud_url,
                                    correlation_id,
                                )
                            )
                        )
                if not pending:
                    break
                done, pending = await asyncio.wait(
                    pending,
                    timeout=max(0, flush_seconds - (monotonic() - buffered_at))
                    if buffer
                    else None,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if done and not buffer:
                    buffered_at = monotonic()
                buffer.extend(task.result() for task in done)
                if buffer and (
                    not pending
                    or len(buffer) >= chunk_size
                    or monotonic() - buffered_at >= flush_seconds
                ):
                    await flush()
        finally:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if buffer:
                # Confirmed uploads must be recorded, or they would be sent again.
                await flush()
    if totals["files"] or totals["missing"]:
        duration_seconds = max(perf_counter() - started_at, 0.001)
        log_event(
            log,
            logging.WARNING if totals["failure"] else logging.INFO,
            "cloud.upload.drain",
            resource=f"unit:{unit_id}",
            status="partial" if totals["failure"] else "success",
            started_at=started_at,
            unit_id=unit_id,
            file_count=totals["files"],
            success_count=totals["success"],
            failure_count=totals["failure"],
            missing_count=totals["missing"],
            unit_concurrency=workers,
            global_concurrency=SEND_GLOBAL_CONCURRENCY,
            files_per_second=round(totals["files"] / duration_seconds, 2),
        )
    return not exhausted and not _circuit_open(circuit)


def _next_probe_candidate(
    db: Session, unit_id: int, after_id: int
) -> ImageTransfer | None:
    """The next send_error transfer after `after_id`, wrapping around.

    Rotating keeps one file the cloud always rejects from blocking the rest.
    """
    for lower_bound in (after_id, 0):
        candidate = db.scalar(
            select(ImageTransfer)
            .where(
                ImageTransfer.unit_id == unit_id,
                ImageTransfer.status == "send_error",
                ImageTransfer.id > lower_bound,
            )
            .order_by(ImageTransfer.id)
            .limit(1)
        )
        if candidate is not None or lower_bound == 0:
            return candidate
    return None


async def _probe_send_errors(
    db: Session, unit: Unit, cloud_url: str, session: aiohttp.ClientSession
) -> int:
    """Every SEND_ERROR_PROBE_SECONDS, try a single exhausted upload.

    On success the unit's send_error transfers return to the queue and are sent
    right after; otherwise they wait for the next check. Returns how many
    transfers were resumed.
    """
    now = monotonic()
    probe = _send_error_probes.get(unit.id)
    if probe is not None and now < probe.next_at:
        return 0
    candidate = _next_probe_candidate(db, unit.id, probe.after_id if probe else 0)
    if candidate is None:
        _send_error_probes.pop(unit.id, None)
        db.commit()
        return 0
    if probe is None:
        # First sighting: the checks start one interval from now.
        _send_error_probes[unit.id] = SendErrorProbe(now + SEND_ERROR_PROBE_SECONDS)
        db.commit()
        return 0
    probe.next_at = now + SEND_ERROR_PROBE_SECONDS
    probe.after_id = candidate.id
    path = Path(unit.send_dir) / candidate.filename
    if not path.is_file():
        candidate.status = "file_missing"
        candidate.last_error = "arquivo não encontrado na pasta de envio"
        db.commit()
        return 0
    transfer_id, correlation_id = candidate.id, candidate.correlation_id
    db.commit()
    result = await _send_one(
        session, asyncio.Semaphore(1), transfer_id, path, cloud_url, correlation_id
    )
    if not result.success:
        log_event(
            log,
            logging.WARNING,
            "cloud.upload.probe",
            resource=f"unit:{unit.id}",
            status="failure",
            error_type=result.error_type or None,
            unit_id=unit.id,
            transfer_id=transfer_id,
            http_status=result.http_status,
        )
        return 0
    _record_send_results(
        db,
        DeliveryRoute(unit.id, unit.send_dir),
        [result],
        _cloud_circuits.setdefault(unit.id, CircuitState()),
        update_circuit=False,
    )
    resumed = resend_failed_transfers(db, ImageTransfer.unit_id == unit.id)
    db.commit()
    _send_error_probes.pop(unit.id, None)
    log_event(
        log,
        logging.INFO,
        "cloud.upload.probe",
        resource=f"unit:{unit.id}",
        status="resumed",
        unit_id=unit.id,
        transfer_id=transfer_id,
        resumed_count=resumed,
    )
    return resumed


def resend_failed_transfers(db: Session, scope) -> int:
    """Return exhausted uploads (send_error) in `scope` to the queue."""
    result = db.execute(
        update(ImageTransfer)
        .where(scope, ImageTransfer.status == "send_error")
        .values(
            status="compressed",
            attempts=0,
            next_attempt_at=None,
            last_http_status=None,
            last_error="",
        ),
        execution_options={"synchronize_session": False},
    )
    return int(result.rowcount or 0)


def _upload_session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(
            total=HTTP_TOTAL_TIMEOUT_SECONDS,
            connect=HTTP_CONNECT_TIMEOUT_SECONDS,
        )
    )


async def _send_one(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    transfer_id: int,
    path: Path,
    url: str,
    correlation_id: str,
) -> SendResult:
    async with semaphore:
        # The global slot is shared with other units' threads; back off instead
        # of polling every few milliseconds while the aggregate limit is full.
        delay = 0.005
        while not _send_slots.acquire(blocking=False):
            await asyncio.sleep(delay)
            delay = min(delay * 2, 0.1)
        started_at = perf_counter()
        try:
            with log_context(correlation_id):
                log_event(
                    log,
                    logging.DEBUG,
                    "cloud.upload",
                    resource=f"transfer:{transfer_id}",
                    status="started",
                    transfer_id=transfer_id,
                )
            with path.open("rb") as stream:
                data = aiohttp.FormData()
                data.add_field(
                    "file",
                    stream,
                    filename=path.name,
                    content_type="application/dicom",
                )
                async with session.post(
                    url,
                    data=data,
                    headers={"X-Request-ID": correlation_id},
                ) as response:
                    success = response.status == 200
                    return SendResult(
                        transfer_id,
                        correlation_id,
                        success,
                        http_status=response.status,
                        error_type="" if success else "CloudHttpError",
                        duration_ms=round((perf_counter() - started_at) * 1000, 2),
                    )
        except Exception as exc:
            return SendResult(
                transfer_id,
                correlation_id,
                False,
                error_type=type(exc).__name__,
                duration_ms=round((perf_counter() - started_at) * 1000, 2),
            )
        finally:
            _send_slots.release()


def _record_send_results(
    db: Session,
    unit: Unit | DeliveryRoute,
    results: list[SendResult],
    circuit: CircuitState,
    *,
    update_circuit: bool = True,
) -> tuple[int, int]:
    now = datetime.now()
    failures = 0
    successful_filenames: list[tuple[int, str]] = []
    db_batch_size = max(1, SEND_DB_BATCH_SIZE)
    for offset in range(0, len(results), db_batch_size):
        chunk = results[offset : offset + db_batch_size]
        try:
            persisted, chunk_failures = _persist_send_result_chunk(db, unit, chunk, now)
            successful_filenames.extend(persisted)
            failures += chunk_failures
        except SQLAlchemyError as exc:
            db.rollback()
            log_event(
                log,
                logging.WARNING,
                "cloud.upload.persist_batch",
                resource=f"unit:{unit.id}",
                status="fallback",
                error=exc,
                unit_id=unit.id,
                file_count=len(chunk),
            )
            for result in chunk:
                persisted = _persist_send_result_with_retry(db, unit, result, now)
                if persisted is not None and result.success:
                    successful_filenames.append(persisted)
                if persisted is None or not result.success:
                    failures += 1

    if update_circuit:
        _update_send_circuit(unit.id, circuit, failures, len(results))
    for transfer_id, filename in successful_filenames:
        try:
            (Path(unit.send_dir) / filename).unlink(missing_ok=True)
        except OSError as exc:
            log_event(
                log,
                logging.WARNING,
                "cloud.upload.cleanup",
                resource=f"transfer:{transfer_id}",
                status="retry",
                error=exc,
                transfer_id=transfer_id,
                unit_id=unit.id,
            )
    return failures, len(successful_filenames)


def _update_send_circuit(
    unit_id: int, circuit: CircuitState, failures: int, result_count: int
) -> None:
    now = datetime.now()
    if result_count and failures == result_count:
        # Conte falhas consecutivas de lote, não cada imagem do mesmo incidente.
        circuit.failures += 1
        if circuit.failures >= CIRCUIT_BREAKER_FAILURES:
            circuit.open_until = now + timedelta(seconds=CIRCUIT_BREAKER_SECONDS)
            log_event(
                log,
                logging.ERROR,
                "cloud.circuit",
                resource=f"unit:{unit_id}",
                status="open",
                error_type="CloudUnavailable",
                unit_id=unit_id,
            )
    else:
        circuit.failures = 0
        circuit.open_until = None


def _apply_send_result(
    transfer: ImageTransfer,
    result: SendResult,
    now: datetime,
) -> tuple[int, str]:
    transfer.attempts += 1
    transfer.last_http_status = result.http_status
    transfer.last_error = bounded_db_text(result.error_type, 500)
    if result.success:
        transfer.status = "uploaded"
        transfer.next_attempt_at = None
        return logging.DEBUG, "success"
    if transfer.attempts >= max(1, SEND_MAX_ATTEMPTS):
        # Waits for a manual resend instead of retrying forever.
        transfer.status = "send_error"
        transfer.next_attempt_at = None
        return logging.ERROR, "exhausted"
    transfer.status = "upload_error"
    delay = SEND_RETRY_DELAYS[min(transfer.attempts, len(SEND_RETRY_DELAYS)) - 1]
    # ±20% jitter spreads retries of images that failed together.
    transfer.next_attempt_at = now + timedelta(seconds=delay * random.uniform(0.8, 1.2))
    return logging.WARNING, "retry"


def _log_persisted_send_result(
    unit: Unit | DeliveryRoute,
    result: SendResult,
    transfer_id: int,
    order_id: int | None,
    attempt: int,
    level: int,
    status: str,
) -> None:
    with log_context(result.correlation_id):
        log_event(
            log,
            level,
            "cloud.upload",
            resource=f"transfer:{transfer_id}",
            status=status,
            error_type=result.error_type or None,
            transfer_id=transfer_id,
            order_id=order_id,
            unit_id=unit.id,
            attempt=attempt,
            http_status=result.http_status,
            duration_ms=result.duration_ms,
        )


def _persist_send_result_chunk(
    db: Session,
    unit: Unit | DeliveryRoute,
    results: list[SendResult],
    now: datetime,
) -> tuple[list[tuple[int, str]], int]:
    """Persist one result chunk atomically, reducing commits under high load."""
    result_by_id = {result.transfer_id: result for result in results}
    transfers = {
        transfer.id: transfer
        for transfer in db.scalars(
            select(ImageTransfer).where(
                ImageTransfer.unit_id == unit.id,
                ImageTransfer.id.in_(result_by_id),
            )
        )
    }
    log_rows: list[tuple[SendResult, int, int | None, int, int, str]] = []
    successful: list[tuple[int, str]] = []
    failures = 0
    for result in results:
        transfer = transfers.get(result.transfer_id)
        if transfer is None:
            failures += 1
            continue
        level, status = _apply_send_result(transfer, result, now)
        log_rows.append(
            (
                result,
                transfer.id,
                transfer.order_id,
                transfer.attempts,
                level,
                status,
            )
        )
        if result.success:
            successful.append((transfer.id, transfer.filename))
        else:
            failures += 1
    db.commit()
    for result, transfer_id, order_id, attempt, level, status in log_rows:
        _log_persisted_send_result(
            unit, result, transfer_id, order_id, attempt, level, status
        )
    return successful, failures


def _persist_send_result_with_retry(
    db: Session,
    unit: Unit | DeliveryRoute,
    result: SendResult,
    now: datetime,
) -> tuple[int, str] | None:
    """Isolate a problematic row if the efficient chunk commit fails."""
    for persist_attempt in range(1, 4):
        try:
            transfer = db.get(ImageTransfer, result.transfer_id)
            if transfer is None or transfer.unit_id != unit.id:
                return None
            level, status = _apply_send_result(transfer, result, now)
            transfer_id = transfer.id
            filename = transfer.filename
            order_id = transfer.order_id
            attempt = transfer.attempts
            db.commit()
            _log_persisted_send_result(
                unit, result, transfer_id, order_id, attempt, level, status
            )
            return transfer_id, filename
        except SQLAlchemyError as exc:
            db.rollback()
            exhausted = persist_attempt == 3
            log_event(
                log,
                logging.ERROR if exhausted else logging.WARNING,
                "cloud.upload.persist",
                resource=f"transfer:{result.transfer_id}",
                status="failure" if exhausted else "retry",
                error=exc,
                transfer_id=result.transfer_id,
                unit_id=unit.id,
                attempt=persist_attempt,
            )
    return None
