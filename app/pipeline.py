from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from time import perf_counter

import aiohttp
import pydicom
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.config import (
    CIRCUIT_BREAKER_FAILURES,
    CIRCUIT_BREAKER_SECONDS,
    FIND_BATCH_SIZE,
    HTTP_CONNECT_TIMEOUT_SECONDS,
    HTTP_TOTAL_TIMEOUT_SECONDS,
    SEND_BATCH_SIZE,
    SEND_RETRY_BASE_SECONDS,
    SEND_RETRY_MAX_SECONDS,
)
from app.dicom_tools import (
    ToolMissing,
    c_find,
    c_move,
    dcmcjpeg,
    redact_dicom_output,
)
from app.events import add_event
from app.models import CompressRule, ImageTransfer, Order, Unit
from app.observability import log_context, log_event, new_correlation_id
from app.parse import parse_findscu_output, parse_order_file
from app.rules import drop_codes, get_settings, schedule_from_now

log = logging.getLogger("worker")
STALE_LOCK = timedelta(minutes=20)


@dataclass(frozen=True)
class CompactResult:
    source_name: str
    output_name: str
    study_uid: str
    status: str
    error_type: str = ""


@dataclass(frozen=True)
class SendResult:
    transfer_id: int
    correlation_id: str
    success: bool
    http_status: int | None = None
    error_type: str = ""
    duration_ms: float = 0.0


@dataclass
class CircuitState:
    failures: int = 0
    open_until: datetime | None = None


_cloud_circuits: dict[str, CircuitState] = {}


def _ensure_order_correlation(order: Order) -> str:
    if not order.correlation_id:
        order.correlation_id = new_correlation_id()
    return order.correlation_id


def recover_stale_locks(db: Session) -> None:
    cutoff = datetime.now() - STALE_LOCK
    rows = list(
        db.scalars(
            select(Order).where(
                Order.status.in_(("retrieving", "retrieving_second")),
                (Order.heartbeat_at.is_(None)) | (Order.heartbeat_at < cutoff),
            )
        )
    )
    for order in rows:
        correlation_id = _ensure_order_correlation(order)
        if order.status == "retrieving_second":
            order.status = "wait_second"
        else:
            order.status = "wait_retrieve"
        order.last_error = "lock órfão recuperado"
        add_event(db, order, "Lock órfão recuperado após queda do worker", level="warn")
        with log_context(correlation_id):
            log_event(
                log,
                logging.WARNING,
                "dicom.move.lock_recovered",
                resource=f"order:{order.id}",
                status="retry",
                order_id=order.id,
                unit_id=order.unit_id,
            )
    if rows:
        db.commit()


def ingest_unit(db: Session, unit: Unit) -> int:
    folder = Path(unit.input_dir)
    if not folder.is_dir():
        return 0
    created = 0
    for path in folder.iterdir():
        try:
            if (
                not path.is_file()
                or path.name.startswith(".")
                or path.stat().st_size == 0
            ):
                continue
            if path.stat().st_size > 4096:
                _quarantine_order_file(path, unit, "OrderFileTooLarge")
                continue
        except OSError as exc:
            log_event(
                log,
                logging.ERROR,
                "order.file.inspect",
                resource=f"unit:{unit.id}",
                status="failure",
                error=exc,
                unit_id=unit.id,
            )
            continue
        exists = db.scalar(
            select(Order).where(Order.unit_id == unit.id, Order.filename == path.name)
        )
        if exists:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            log_event(
                log,
                logging.ERROR,
                "order.file.read",
                resource=f"unit:{unit.id}",
                status="failure",
                error=exc,
                unit_id=unit.id,
            )
            continue
        parsed = parse_order_file(text)
        if parsed is None:
            _quarantine_order_file(path, unit, "InvalidOrderFile")
            continue
        correlation_id = new_correlation_id()
        order = Order(
            unit_id=unit.id,
            filename=path.name,
            pat_id=parsed.pat_id,
            acc=parsed.acc,
            birth_date=parsed.birth_date,
            exam_date=parsed.exam_date,
            status="watching",
            correlation_id=correlation_id,
        )
        db.add(order)
        db.flush()
        add_event(db, order, f"Pedido lido: acc={parsed.acc} nasc={parsed.birth_date}")
        with log_context(correlation_id):
            log_event(
                log,
                logging.INFO,
                "order.ingest",
                resource=f"order:{order.id}",
                status="success",
                order_id=order.id,
                unit_id=unit.id,
            )
        created += 1
    if created:
        db.commit()
    return created


def _quarantine_order_file(path: Path, unit: Unit, error_type: str) -> None:
    destination_dir = Path(unit.error_dir) / "orders"
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / path.name
    if destination.exists():
        destination = destination.with_name(
            f"{destination.name}.rejected-{new_correlation_id()}"
        )
    shutil.move(str(path), str(destination))
    log_event(
        log,
        logging.WARNING,
        "order.ingest",
        resource=f"unit:{unit.id}",
        status="rejected",
        error_type=error_type,
        unit_id=unit.id,
    )


def find_pending(db: Session, unit: Unit) -> None:
    now = datetime.now()
    interval = timedelta(seconds=unit.find_interval_seconds or 30)
    orders = list(
        db.scalars(
            select(Order)
            .where(
                Order.unit_id == unit.id,
                Order.status == "watching",
                or_(
                    Order.last_find_at.is_(None),
                    Order.last_find_at <= now - interval,
                ),
            )
            .order_by(Order.last_find_at, Order.id)
            .limit(FIND_BATCH_SIZE)
        )
    )
    for order in orders:
        _find_one(db, unit, order, now)


def _find_one(db: Session, unit: Unit, order: Order, now: datetime) -> None:
    order.last_find_at = now
    order.heartbeat_at = now
    correlation_id = _ensure_order_correlation(order)
    started_at = perf_counter()
    with log_context(correlation_id):
        log_event(
            log,
            logging.INFO,
            "dicom.find",
            resource=f"order:{order.id}",
            status="started",
            order_id=order.id,
            unit_id=unit.id,
        )
        try:
            code, output = c_find(
                unit.calling_aet,
                unit.pacs_aet,
                unit.pacs_ip,
                unit.pacs_port,
                order.acc,
                order.birth_date,
            )
        except ToolMissing as exc:
            order.status = "error"
            order.last_error = str(exc)
            add_event(db, order, "findscu indisponível", level="error")
            db.commit()
            log_event(
                log,
                logging.ERROR,
                "dicom.find",
                resource=f"order:{order.id}",
                status="failure",
                started_at=started_at,
                error=exc,
                order_id=order.id,
                unit_id=unit.id,
            )
            return

        study_uid, modalities, patient_name = parse_findscu_output(output)
        order.attempts += 1
        safe_output = redact_dicom_output(output)
        if not study_uid:
            add_event(
                db,
                order,
                "C-FIND sem StudyInstanceUID (exame ainda não no PACS)",
                safe_output,
            )
            db.commit()
            log_event(
                log,
                logging.INFO,
                "dicom.find",
                resource=f"order:{order.id}",
                status="not_found",
                started_at=started_at,
                order_id=order.id,
                unit_id=unit.id,
                command_status=code,
            )
            return

        modality, retrieve_at, second_at = schedule_from_now(db, modalities)
        order.study_uid = study_uid
        order.modality = modality
        order.patient_name = patient_name
        order.retrieve_at = retrieve_at
        order.second_retrieve_at = second_at
        order.found_at = now
        order.status = "wait_retrieve"
        order.last_error = ""
        add_event(
            db,
            order,
            f"Exame encontrado UID={study_uid} mod={modality}. "
            f"1º retrieve em {retrieve_at:%H:%M}",
            safe_output,
        )
        _move_to_sent(unit, order.filename)
        db.commit()
        log_event(
            log,
            logging.INFO,
            "dicom.find",
            resource=f"order:{order.id}",
            status="success",
            started_at=started_at,
            order_id=order.id,
            unit_id=unit.id,
            modality=modality,
        )


def claim_due_moves(db: Session, unit: Unit) -> list[tuple[int, bool]]:
    now = datetime.now()
    inflight = (
        db.scalar(
            select(func.count()).where(
                Order.unit_id == unit.id,
                Order.status.in_(("retrieving", "retrieving_second")),
            )
        )
        or 0
    )
    slots = max(1, unit.max_parallel_moves) - inflight
    claimed: list[tuple[int, bool]] = []
    if slots <= 0:
        return claimed

    first = list(
        db.scalars(
            select(Order)
            .where(
                Order.unit_id == unit.id,
                Order.status == "wait_retrieve",
                Order.retrieve_at.is_not(None),
                Order.retrieve_at <= now,
            )
            .order_by(Order.retrieve_at)
            .limit(slots)
        )
    )
    for order in first:
        order.status = "retrieving"
        order.heartbeat_at = now
        claimed.append((order.id, False))
        slots -= 1
    if slots <= 0:
        db.commit()
        return claimed

    second = list(
        db.scalars(
            select(Order)
            .where(
                Order.unit_id == unit.id,
                Order.status == "wait_second",
                Order.second_retrieve_at.is_not(None),
                Order.second_retrieve_at <= now,
            )
            .order_by(Order.second_retrieve_at)
            .limit(slots)
        )
    )
    for order in second:
        order.status = "retrieving_second"
        order.heartbeat_at = now
        claimed.append((order.id, True))
    if claimed:
        db.commit()
    return claimed


def run_claimed_move(db: Session, order_id: int, second: bool) -> None:
    order = db.get(Order, order_id)
    if order is None:
        return
    unit = db.get(Unit, order.unit_id)
    if unit is None:
        return
    _run_move(db, unit, order, second)


def _run_move(db: Session, unit: Unit, order: Order, second: bool) -> None:
    correlation_id = _ensure_order_correlation(order)
    started_at = perf_counter()
    order.status = "retrieving_second" if second else "retrieving"
    order.heartbeat_at = datetime.now()
    order.attempts += 1
    db.commit()
    timeout = unit.move_timeout_second if second else unit.move_timeout_first
    label = "2º" if second else "1º"
    with log_context(correlation_id):
        log_event(
            log,
            logging.INFO,
            "dicom.move",
            resource=f"order:{order.id}",
            status="started",
            order_id=order.id,
            unit_id=unit.id,
            retrieve_number=2 if second else 1,
        )
        try:
            code, output = c_move(
                unit.calling_aet,
                unit.pacs_aet,
                unit.pacs_ip,
                unit.pacs_port,
                order.study_uid,
                unit.dest_aet,
                timeout,
            )
        except ToolMissing as exc:
            order.status = "error"
            order.last_error = str(exc)
            add_event(db, order, f"{label} C-MOVE: movescu indisponível", level="error")
            db.commit()
            log_event(
                log,
                logging.ERROR,
                "dicom.move",
                resource=f"order:{order.id}",
                status="failure",
                started_at=started_at,
                error=exc,
                order_id=order.id,
                unit_id=unit.id,
            )
            return
        safe_output = redact_dicom_output(output)
        if code == 0:
            if second or order.second_retrieve_at is None:
                order.status = "done"
                order.done_at = datetime.now()
                add_event(db, order, f"{label} C-MOVE concluído", safe_output)
            else:
                order.status = "wait_second"
                add_event(
                    db,
                    order,
                    f"1º C-MOVE concluído. 2º às {order.second_retrieve_at:%H:%M}",
                    safe_output,
                )
            order.last_error = ""
            event_level = logging.INFO
            event_status = "success"
        else:
            order.status = "error"
            order.last_error = f"C-MOVE exit {code}"
            add_event(
                db,
                order,
                f"{label} C-MOVE falhou (exit {code})",
                safe_output,
                "error",
            )
            event_level = logging.ERROR
            event_status = "failure"
        order.heartbeat_at = datetime.now()
        db.commit()
        log_event(
            log,
            event_level,
            "dicom.move",
            resource=f"order:{order.id}",
            status=event_status,
            started_at=started_at,
            order_id=order.id,
            unit_id=unit.id,
            retrieve_number=2 if second else 1,
            command_status=code,
        )


def _move_to_sent(unit: Unit, filename: str) -> None:
    src = Path(unit.input_dir) / filename
    dest_dir = Path(unit.sent_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename
    if src.exists():
        shutil.move(str(src), str(dest))


def compact_unit(db: Session, unit: Unit) -> None:
    settings = get_settings(db)
    origin = Path(unit.receive_dir)
    dest_dir = Path(unit.send_dir)
    error_dir = Path(unit.error_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    error_dir.mkdir(parents=True, exist_ok=True)
    if not origin.is_dir():
        return
    drops = drop_codes(db)
    prefix = (settings.drop_study_prefix or "SLRX").upper()
    settle = settings.file_settle_seconds or 3
    now = datetime.now().timestamp()
    jobs: list[Path] = []
    for path in origin.iterdir():
        try:
            if not path.is_file() or path.name.startswith("."):
                continue
            if now - path.stat().st_mtime < settle:
                continue
            jobs.append(path)
        except OSError as exc:
            log_event(
                log,
                logging.ERROR,
                "dicom.file.inspect",
                resource=f"unit:{unit.id}",
                status="failure",
                error=exc,
                unit_id=unit.id,
            )
    if not jobs:
        return
    batch_correlation = new_correlation_id()
    started_at = perf_counter()
    with log_context(batch_correlation):
        log_event(
            log,
            logging.INFO,
            "dicom.compact.batch",
            resource=f"unit:{unit.id}",
            status="started",
            unit_id=unit.id,
            file_count=len(jobs),
        )
    compress_map = {}
    for row in db.scalars(select(CompressRule)):
        compress_map[row.modality.upper()] = row.jpeg_flag
    workers = max(1, unit.compact_workers or 8)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [
            pool.submit(
                _compact_one,
                str(p),
                str(dest_dir / (p.name + ".dcm")),
                str(error_dir / p.name),
                unit.token,
                drops,
                prefix,
                compress_map,
            )
            for p in jobs
        ]
        for future in as_completed(futs):
            result = future.result()
            _record_compact_result(db, unit, result)
    db.commit()
    with log_context(batch_correlation):
        log_event(
            log,
            logging.INFO,
            "dicom.compact.batch",
            resource=f"unit:{unit.id}",
            status="success",
            started_at=started_at,
            unit_id=unit.id,
            file_count=len(jobs),
        )


def _filename_mod(name: str) -> str:
    return name[:2].upper() if len(name) >= 2 else ""


def _compact_one(
    filepath: str,
    dest: str,
    error: str,
    token: str,
    drops: set[str],
    study_prefix: str,
    compress_map: dict[str, str],
) -> CompactResult:
    name = os.path.basename(filepath)
    prefix = _filename_mod(name)
    try:
        image = pydicom.dcmread(filepath, defer_size="1 MB")
    except Exception as exc:
        shutil.move(filepath, error)
        return CompactResult(name, name, "", "compression_error", type(exc).__name__)

    modality = str(getattr(image, "Modality", "") or prefix).upper()
    study_uid = str(getattr(image, "StudyInstanceUID", "") or "")
    if prefix in drops or modality in drops:
        os.remove(filepath)
        return CompactResult(name, "", study_uid, "discarded_modality")

    study_id = str(getattr(image, "StudyID", "") or "")
    if study_prefix and re.match(rf"^{re.escape(study_prefix)}\d+", study_id, re.I):
        os.remove(filepath)
        return CompactResult(name, "", study_uid, "discarded_study")

    image.SpecificCharacterSet = "ISO_IR 100"
    image.InstitutionalDepartmentName = token
    image.save_as(filepath)

    flag = compress_map.get(modality, compress_map.get("*", "+e1"))
    try:
        code, _out = dcmcjpeg(flag, filepath, dest)
    except ToolMissing as exc:
        shutil.move(filepath, error)
        return CompactResult(
            name, name, study_uid, "compression_error", type(exc).__name__
        )
    if code == 0:
        os.remove(filepath)
        return CompactResult(name, os.path.basename(dest), study_uid, "compressed")
    shutil.move(filepath, error)
    return CompactResult(name, name, study_uid, "compression_error", "DcmcjpegError")


def _record_compact_result(db: Session, unit: Unit, result: CompactResult) -> None:
    order = None
    if result.study_uid:
        order = db.scalar(
            select(Order)
            .where(Order.unit_id == unit.id, Order.study_uid == result.study_uid)
            .order_by(Order.id.desc())
            .limit(1)
        )
    correlation_id = (
        _ensure_order_correlation(order) if order is not None else new_correlation_id()
    )
    transfer = db.scalar(
        select(ImageTransfer).where(
            ImageTransfer.unit_id == unit.id,
            ImageTransfer.filename == (result.output_name or result.source_name),
        )
    )
    if transfer is None:
        transfer = ImageTransfer(
            unit_id=unit.id,
            order_id=order.id if order else None,
            filename=result.output_name or result.source_name,
            correlation_id=correlation_id,
            study_uid=result.study_uid,
        )
        db.add(transfer)
        db.flush()
    transfer.status = result.status
    transfer.last_error = result.error_type
    level = logging.ERROR if result.error_type else logging.INFO
    with log_context(correlation_id):
        log_event(
            log,
            level,
            "dicom.compact",
            resource=f"transfer:{transfer.id}",
            status="failure" if result.error_type else result.status,
            error_type=result.error_type or None,
            transfer_id=transfer.id,
            order_id=order.id if order else None,
            unit_id=unit.id,
        )


def send_unit(db: Session, unit: Unit, cloud_url: str, settle: int) -> None:
    origin = Path(unit.send_dir)
    if not origin.is_dir() or not cloud_url:
        return
    circuit = _cloud_circuits.setdefault(cloud_url, CircuitState())
    current_time = datetime.now()
    if circuit.open_until and current_time < circuit.open_until:
        return

    timestamp = current_time.timestamp()
    paths: dict[str, Path] = {}
    for path in origin.iterdir():
        try:
            if (
                path.is_file()
                and not path.name.startswith(".")
                and timestamp - path.stat().st_mtime > settle
            ):
                paths[path.name] = path
        except OSError:
            continue
    if not paths:
        return

    existing = {
        transfer.filename: transfer
        for transfer in db.scalars(
            select(ImageTransfer).where(
                ImageTransfer.unit_id == unit.id,
                ImageTransfer.filename.in_(paths),
            )
        )
    }
    for filename in paths:
        if filename not in existing:
            transfer = ImageTransfer(
                unit_id=unit.id,
                filename=filename,
                correlation_id=new_correlation_id(),
                status="compressed",
            )
            db.add(transfer)
            existing[filename] = transfer
    db.flush()

    jobs = [
        (transfer.id, paths[filename], transfer.correlation_id)
        for filename, transfer in existing.items()
        if transfer.status in {"compressed", "upload_error"}
        and (
            transfer.next_attempt_at is None or transfer.next_attempt_at <= current_time
        )
    ][:SEND_BATCH_SIZE]
    if not jobs:
        db.commit()
        return
    workers = max(1, unit.send_workers or 16)
    results = asyncio.run(_send_all(jobs, cloud_url, workers))
    _record_send_results(db, unit, results, circuit)


async def _send_all(
    jobs: list[tuple[int, Path, str]], url: str, limit: int
) -> list[SendResult]:
    sem = asyncio.Semaphore(limit)
    timeout = aiohttp.ClientTimeout(
        total=HTTP_TOTAL_TIMEOUT_SECONDS,
        connect=HTTP_CONNECT_TIMEOUT_SECONDS,
    )
    async with aiohttp.ClientSession(timeout=timeout) as session:
        return await asyncio.gather(
            *[
                _send_one(session, sem, transfer_id, path, url, correlation_id)
                for transfer_id, path, correlation_id in jobs
            ]
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
        started_at = perf_counter()
        with log_context(correlation_id):
            log_event(
                log,
                logging.INFO,
                "cloud.upload",
                resource=f"transfer:{transfer_id}",
                status="started",
                transfer_id=transfer_id,
            )
        try:
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
                    if success:
                        path.unlink(missing_ok=True)
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


def _record_send_results(
    db: Session,
    unit: Unit,
    results: list[SendResult],
    circuit: CircuitState,
) -> None:
    now = datetime.now()
    failures = 0
    for result in results:
        transfer = db.get(ImageTransfer, result.transfer_id)
        if transfer is None:
            continue
        transfer.attempts += 1
        transfer.last_http_status = result.http_status
        transfer.last_error = result.error_type
        if result.success:
            transfer.status = "uploaded"
            transfer.next_attempt_at = None
            level = logging.INFO
            status = "success"
        else:
            failures += 1
            transfer.status = "upload_error"
            delay = min(
                SEND_RETRY_MAX_SECONDS,
                SEND_RETRY_BASE_SECONDS * (2 ** min(transfer.attempts - 1, 10)),
            )
            transfer.next_attempt_at = now + timedelta(seconds=delay)
            level = logging.WARNING
            status = "retry"
        with log_context(result.correlation_id):
            log_event(
                log,
                level,
                "cloud.upload",
                resource=f"transfer:{transfer.id}",
                status=status,
                error_type=result.error_type or None,
                transfer_id=transfer.id,
                order_id=transfer.order_id,
                unit_id=unit.id,
                attempt=transfer.attempts,
                http_status=result.http_status,
                duration_ms=result.duration_ms,
            )

    if failures == len(results):
        circuit.failures += failures
        if circuit.failures >= CIRCUIT_BREAKER_FAILURES:
            circuit.open_until = now + timedelta(seconds=CIRCUIT_BREAKER_SECONDS)
            log_event(
                log,
                logging.ERROR,
                "cloud.circuit",
                resource=f"unit:{unit.id}",
                status="open",
                error_type="CloudUnavailable",
                unit_id=unit.id,
            )
    else:
        circuit.failures = 0
        circuit.open_until = None
    db.commit()


def folder_counts(unit: Unit) -> dict[str, int]:
    def count(p: str) -> int:
        path = Path(p)
        try:
            if not path.is_dir():
                return 0
            return sum(
                1
                for file in path.iterdir()
                if file.is_file() and not file.name.startswith(".")
            )
        except OSError:
            return 0

    return {
        "input": count(unit.input_dir),
        "receive": count(unit.receive_dir),
        "send": count(unit.send_dir),
        "error": count(unit.error_dir),
    }
