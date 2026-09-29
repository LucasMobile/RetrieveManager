"""Compaction: instance claims, isolated codec, publication and recovery."""

from __future__ import annotations

import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from threading import BoundedSemaphore, Lock
from time import monotonic, perf_counter

import pydicom
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.codec import METHOD_COPY, METHOD_LOSSLESS, METHOD_LOSSY
from app.codec_worker import CodecCrash, CodecPool, CodecTimeout
from app.compaction import CompactJob, CompactResult, file_sha256
from app.compression import compression_runtime_settings
from app.config import (
    COMPACT_BATCH_SIZE,
    COMPACT_DB_BATCH_SIZE,
    COMPACT_FILE_TIMEOUT_SECONDS,
    COMPACT_GLOBAL_WORKERS,
    COMPACT_MAX_ENCODE_BYTES,
    COMPACT_TEMP_MAX_AGE_SECONDS,
    RECEIVE_ADOPT_BATCH_SIZE,
    RECEIVE_ADOPT_INTERVAL_SECONDS,
    RECEIVE_ADOPT_MIN_AGE_SECONDS,
)
from app.dicom_rules import (
    RuleSpec,
    load_rule_specs,
)
from app.instances import (
    ACTIVE_STATES,
    ReceivedObject,
    apply_decision,
    dataset_sha256,
    record_instance,
)
from app.models import (
    DicomInstance,
    ImageTransfer,
    Unit,
)
from app.observability import (
    log_context,
    log_event,
    new_correlation_id,
    safe_error_detail,
)
from app.pipeline.common import bounded_db_text, log
from app.pipeline.records import (
    InboundDicomRejected,
    RecordedResult,
    persist_compact_chunk,
)
from app.validation import store_allowed_senders

_compact_slots = BoundedSemaphore(max(1, COMPACT_GLOBAL_WORKERS))


def _quarantine_failed_source(source: Path, error_dir: Path) -> Path | None:
    """Move a poison input aside so it cannot starve the next queue batch."""
    if not source.is_file():
        return None
    error_dir.mkdir(parents=True, exist_ok=True)
    target = error_dir / source.name
    if target.exists():
        target = error_dir / (
            f"{target.stem}.failed-{new_correlation_id()[:8]}{target.suffix}"
        )
    shutil.move(str(source), str(target))
    return target


def _compact_one_limited(*args) -> CompactResult:
    with _compact_slots:
        return _compact_one(*args)


def _cleanup_compaction_temps(work_dir: Path) -> list[tuple[Path, OSError]]:
    """Remove abandoned codec files without touching active queue entries."""
    if not work_dir.is_dir():
        return []
    cutoff = datetime.now().timestamp() - max(
        60,
        COMPACT_TEMP_MAX_AGE_SECONDS,
        COMPACT_FILE_TIMEOUT_SECONDS * 2,
    )
    errors: list[tuple[Path, OSError]] = []
    try:
        with os.scandir(work_dir) as entries:
            for entry in entries:
                path = Path(entry.path)
                try:
                    if (
                        entry.is_file(follow_symlinks=False)
                        and entry.stat(follow_symlinks=False).st_mtime < cutoff
                    ):
                        path.unlink(missing_ok=True)
                except OSError as exc:
                    errors.append((path, exc))
    except OSError as exc:
        errors.append((work_dir, exc))
    return errors


def _publish_outputs(
    db: Session,
    unit: Unit,
    results: list[CompactResult],
    recorded: list[RecordedResult],
) -> None:
    """Move recorded artifacts into send_dir, then release them to the sender.

    Crash windows: before the move the transfer stays "publishing" with the
    source intact; after it, the file matches the recorded hash. Both are
    resolved by _recover_publishing_transfers. Results that could not be
    recorded drop their output and keep the source for the next claim.
    """
    send_dir = Path(unit.send_dir)
    recorded_by_source = {
        id(result): (transfer_id, status) for result, transfer_id, status in recorded
    }
    published: list[int] = []
    for result in results:
        temp_output = Path(result.temp_output) if result.temp_output else None
        transfer_id, status = recorded_by_source.get(id(result), (None, ""))
        if status == "publishing" and temp_output is not None:
            try:
                os.replace(temp_output, send_dir / result.output_name)
            except OSError as exc:
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.compact.publish",
                    resource=f"transfer:{transfer_id}",
                    status="retry",
                    error=exc,
                    error_detail=safe_error_detail(exc),
                    transfer_id=transfer_id,
                    unit_id=unit.id,
                )
                continue
            published.append(transfer_id)
            _remove_compacted_source(unit, result, transfer_id)
            continue
        if temp_output is not None:
            with suppress(OSError):
                temp_output.unlink(missing_ok=True)
        if status == "metadata_error":
            # The output was quarantined; the source is no longer needed.
            _remove_compacted_source(unit, result, transfer_id)
    if not published:
        return
    try:
        db.execute(
            update(ImageTransfer)
            .where(
                ImageTransfer.id.in_(published),
                ImageTransfer.status == "publishing",
            )
            .values(status="compressed"),
            execution_options={"synchronize_session": False},
        )
        db.commit()
    except SQLAlchemyError as exc:
        db.rollback()
        log_event(
            log,
            logging.WARNING,
            "dicom.compact.publish",
            resource=f"unit:{unit.id}",
            status="retry",
            error=exc,
            error_detail=safe_error_detail(exc),
            unit_id=unit.id,
            file_count=len(published),
        )


def _remove_compacted_source(
    unit: Unit, result: CompactResult, transfer_id: int | None
) -> None:
    if not result.source_path:
        return
    try:
        Path(result.source_path).unlink(missing_ok=True)
    except OSError as exc:
        # A leftover source is recognized as an identical copy when adopted.
        log_event(
            log,
            logging.WARNING,
            "dicom.compact.source.cleanup",
            resource=f"transfer:{transfer_id}",
            status="failure",
            error=exc,
            error_detail=safe_error_detail(exc),
            transfer_id=transfer_id,
            unit_id=unit.id,
        )


def _recover_publishing_transfers(db: Session, unit: Unit) -> None:
    """Finish artifacts left between their commit and their move to send_dir."""
    cutoff = datetime.now() - INSTANCE_CLAIM_STALE_AFTER
    transfers = list(
        db.scalars(
            select(ImageTransfer)
            .where(
                ImageTransfer.unit_id == unit.id,
                ImageTransfer.status == "publishing",
                ImageTransfer.updated_at < cutoff,
            )
            .order_by(ImageTransfer.id)
            .limit(COMPACT_BATCH_SIZE)
        )
    )
    if not transfers:
        return
    instances = {
        instance.transfer_id: instance
        for instance in db.scalars(
            select(DicomInstance).where(
                DicomInstance.transfer_id.in_([t.id for t in transfers])
            )
        )
    }
    send_dir = Path(unit.send_dir)
    for transfer in transfers:
        instance = instances.get(transfer.id)
        source = Path(instance.source_path) if instance is not None else None
        artifact = send_dir / transfer.filename
        if artifact.is_file() and file_sha256(artifact) == transfer.sha256:
            transfer.status = "compressed"
            outcome = "published"
            if source is not None:
                with suppress(OSError):
                    source.unlink(missing_ok=True)
        elif instance is not None and source is not None and source.is_file():
            # Compacted again from the intact source; the same transfer row is
            # reused because the output name is the same.
            transfer.status = "recompact"
            instance.state = "received"
            instance.claimed_at = None
            outcome = "requeued"
        else:
            transfer.status = "file_missing"
            transfer.last_error = "artefato não publicado e origem ausente"
            if instance is not None:
                instance.state = "missing"
                instance.claimed_at = None
            outcome = "missing"
        log_event(
            log,
            logging.WARNING,
            "dicom.compact.publish.recover",
            resource=f"transfer:{transfer.id}",
            status=outcome,
            transfer_id=transfer.id,
            unit_id=unit.id,
        )
    db.commit()


INSTANCE_CLAIM_STALE_AFTER = timedelta(
    seconds=max(600, COMPACT_FILE_TIMEOUT_SECONDS * 2)
)


@dataclass
class ReceiveAdoptState:
    directory: str
    entries: object | None = None
    next_scan_at: float = 0.0


_receive_adopt_states: dict[int, ReceiveAdoptState] = {}


_receive_adopt_lock = Lock()


def _mark_instance_failed(db: Session, instance_id: int, error_type: str) -> None:
    try:
        db.execute(
            update(DicomInstance)
            .where(DicomInstance.id == instance_id)
            .values(
                state="error",
                claimed_at=None,
                last_error=bounded_db_text(error_type, 500),
            ),
            execution_options={"synchronize_session": False},
        )
        db.commit()
    except SQLAlchemyError:
        # The claim expires and is recovered; the source is already quarantined.
        db.rollback()


def _claim_received_instances(
    db: Session, unit: Unit, limit: int
) -> list[tuple[int, Path]]:
    """Reserve received instances; the database row is the compaction queue."""
    rows = list(
        db.execute(
            select(DicomInstance.id, DicomInstance.source_path)
            .where(
                DicomInstance.unit_id == unit.id,
                DicomInstance.state == "received",
            )
            .order_by(DicomInstance.id)
            .limit(max(1, limit))
            .with_for_update(skip_locked=True)
        )
    )
    if not rows:
        db.commit()
        return []
    missing = [row.id for row in rows if not Path(row.source_path).is_file()]
    db.execute(
        update(DicomInstance)
        .where(DicomInstance.id.in_([row.id for row in rows]))
        .values(state="compacting", claimed_at=datetime.now()),
        execution_options={"synchronize_session": False},
    )
    if missing:
        db.execute(
            update(DicomInstance)
            .where(DicomInstance.id.in_(missing))
            .values(
                state="missing",
                claimed_at=None,
                last_error="arquivo recebido não encontrado",
            ),
            execution_options={"synchronize_session": False},
        )
        log_event(
            log,
            logging.ERROR,
            "dicom.compact.claim",
            resource=f"unit:{unit.id}",
            status="missing",
            unit_id=unit.id,
            instance_count=len(missing),
        )
    db.commit()
    return [(row.id, Path(row.source_path)) for row in rows if row.id not in missing]


def _recover_stale_instance_claims(db: Session, unit: Unit) -> None:
    """Return instances claimed by a crashed compaction to the queue."""
    cutoff = datetime.now() - INSTANCE_CLAIM_STALE_AFTER
    stale = list(
        db.scalars(
            select(DicomInstance).where(
                DicomInstance.unit_id == unit.id,
                DicomInstance.state == "compacting",
                DicomInstance.claimed_at < cutoff,
            )
        )
    )
    for instance in stale:
        instance.claimed_at = None
        if Path(instance.source_path).is_file():
            instance.state = "received"
        else:
            # Compacted before the crash (the output is reconciled by the
            # sender) or lost; either way there is nothing left to compact.
            instance.state = "missing"
            instance.last_error = "arquivo ausente ao recuperar a compactação"
    if stale:
        log_event(
            log,
            logging.WARNING,
            "dicom.compact.claim",
            resource=f"unit:{unit.id}",
            status="recovered",
            unit_id=unit.id,
            instance_count=len(stale),
        )
    db.commit()


def _next_receive_adopt_paths(
    unit_id: int, directory: Path, *, min_age: int, timestamp: float
) -> list[Path]:
    """Walk the receive folder a slice at a time, like the send reconciliation."""
    paths: list[Path] = []
    with _receive_adopt_lock:
        state = _receive_adopt_states.get(unit_id)
        if state is None or state.directory != str(directory):
            if state is not None and state.entries is not None:
                with suppress(Exception):
                    state.entries.close()  # type: ignore[attr-defined]
            state = ReceiveAdoptState(directory=str(directory))
            _receive_adopt_states[unit_id] = state
        if state.entries is None:
            if monotonic() < state.next_scan_at:
                return paths
            try:
                state.entries = os.scandir(directory)
            except OSError:
                state.next_scan_at = monotonic() + RECEIVE_ADOPT_INTERVAL_SECONDS
                return paths
        inspected = 0
        while inspected < RECEIVE_ADOPT_BATCH_SIZE * 10:
            try:
                entry = next(state.entries)  # type: ignore[arg-type]
            except StopIteration, OSError:
                with suppress(Exception):
                    state.entries.close()  # type: ignore[attr-defined]
                state.entries = None
                state.next_scan_at = monotonic() + RECEIVE_ADOPT_INTERVAL_SECONDS
                break
            inspected += 1
            if entry.name.startswith("."):
                continue
            try:
                if not entry.is_file(follow_symlinks=False):
                    continue
                if timestamp - entry.stat(follow_symlinks=False).st_mtime < min_age:
                    continue
            except OSError:
                continue
            paths.append(Path(entry.path))
            if len(paths) >= RECEIVE_ADOPT_BATCH_SIZE:
                break
    return paths


def _received_object_from_file(
    unit: Unit, path: Path, error_dir: Path
) -> ReceivedObject:
    image = pydicom.dcmread(path, stop_before_pixels=True)
    sop_uid = str(getattr(image, "SOPInstanceUID", "") or "").strip()
    study_uid = str(getattr(image, "StudyInstanceUID", "") or "").strip()
    if not sop_uid or not study_uid or max(len(sop_uid), len(study_uid)) > 64:
        raise InboundDicomRejected("SOPInstanceUID/StudyInstanceUID ausente")
    meta = getattr(image, "file_meta", None)
    digest, size = dataset_sha256(path)
    return ReceivedObject(
        unit_id=unit.id,
        study_uid=study_uid,
        series_uid=str(getattr(image, "SeriesInstanceUID", "") or "")[:64],
        sop_uid=sop_uid,
        sop_class_uid=str(getattr(image, "SOPClassUID", "") or "")[:64],
        transfer_syntax=str(getattr(meta, "TransferSyntaxUID", "") or "")[:64],
        modality=str(getattr(image, "Modality", "") or "")[:16],
        sha256=digest,
        size=size,
        path=str(path),
        conflict_path=str(error_dir / path.name),
        calling_aet=str(
            getattr(meta, "SourceApplicationEntityTitle", "") or ""
        ).strip()[:16],
    )


def _adopt_receive_files(db: Session, unit: Unit, origin: Path, error_dir: Path) -> int:
    """Register files in the receive folder that have no queue row.

    The receiver records every object it writes; this only catches files it
    did not write (left by the former storescp, or moved back by "reprocessar
    erros") and files whose commit was interrupted.
    """
    paths = _next_receive_adopt_paths(
        unit.id,
        origin,
        min_age=RECEIVE_ADOPT_MIN_AGE_SECONDS,
        timestamp=datetime.now().timestamp(),
    )
    if not paths:
        return 0
    tracked = set(
        db.scalars(
            select(DicomInstance.source_path).where(
                DicomInstance.unit_id == unit.id,
                DicomInstance.source_path.in_([str(path) for path in paths]),
                DicomInstance.state.in_(ACTIVE_STATES),
            )
        )
    )
    db.commit()
    adopted = 0
    for path in paths:
        if str(path) in tracked:
            continue
        try:
            obj = _received_object_from_file(unit, path, error_dir)
        except FileNotFoundError:
            continue
        except Exception as exc:
            target = None
            with suppress(OSError):
                target = _quarantine_failed_source(path, error_dir)
            log_event(
                log,
                logging.WARNING,
                "dicom.receive.adopt",
                resource=f"unit:{unit.id}",
                status="quarantined",
                error=exc,
                error_detail=safe_error_detail(exc),
                unit_id=unit.id,
                quarantined=target is not None,
            )
            continue
        try:
            decision = record_instance(db, obj)
            db.commit()
        except IntegrityError:
            # The receiver registered the same object meanwhile; next scan.
            db.rollback()
            continue
        try:
            apply_decision(decision, path)
        except OSError as exc:
            log_event(
                log,
                logging.WARNING,
                "dicom.receive.adopt",
                resource=f"instance:{decision.instance_id}",
                status="failure",
                error=exc,
                error_detail=safe_error_detail(exc),
                unit_id=unit.id,
            )
            continue
        adopted += int(decision.outcome in ("new", "revived", "conflict"))
        log_event(
            log,
            logging.INFO,
            "dicom.receive.adopt",
            resource=f"instance:{decision.instance_id}",
            status=decision.outcome,
            unit_id=unit.id,
            instance_id=decision.instance_id,
        )
    return adopted


def compact_unit(db: Session, unit: Unit) -> bool:
    """Process one bounded batch and report whether more work may be available."""
    origin = Path(unit.receive_dir)
    dest_dir = Path(unit.send_dir)
    error_dir = Path(unit.error_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    error_dir.mkdir(parents=True, exist_ok=True)
    work_dir = dest_dir / ".retrieve-compact-tmp"
    work_dir.mkdir(parents=True, exist_ok=True)
    for path, exc in _cleanup_compaction_temps(work_dir):
        log_event(
            log,
            logging.WARNING,
            "dicom.compact.temp.cleanup",
            resource=f"unit:{unit.id}",
            status="failure",
            error=exc,
            error_detail=safe_error_detail(exc),
            unit_id=unit.id,
            path_name=path.name,
        )
    drops, compress_map = compression_runtime_settings(db, unit.id)
    dicom_rules = load_rule_specs(db, unit.id)
    allowed_senders = store_allowed_senders(unit.store_allowed_aets)
    _recover_stale_instance_claims(db, unit)
    _recover_publishing_transfers(db, unit)
    _adopt_receive_files(db, unit, origin, error_dir)
    claims = _claim_received_instances(db, unit, COMPACT_BATCH_SIZE)
    if not claims:
        return False
    jobs = [path for _instance_id, path in claims]
    instance_by_path = {path: instance_id for instance_id, path in claims}
    batch_correlation = new_correlation_id()
    started_at = perf_counter()
    workers = max(1, unit.compact_workers or 8)
    with log_context(batch_correlation):
        log_event(
            log,
            logging.INFO,
            "dicom.compact.batch",
            resource=f"unit:{unit.id}",
            status="started",
            unit_id=unit.id,
            file_count=len(jobs),
            unit_worker_limit=workers,
            global_worker_limit=max(1, COMPACT_GLOBAL_WORKERS),
        )
    processed_count = 0
    failed_count = 0
    codec_counts = {METHOD_LOSSLESS: 0, METHOD_LOSSY: 0, METHOD_COPY: 0}
    persistence_buffer: list[CompactResult] = []
    # As regras já foram materializadas em valores simples. Devolva a conexão
    # ao pool enquanto o codec processa os arquivos deste lote.
    db.commit()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {
            pool.submit(
                _compact_one_limited,
                str(p),
                str(dest_dir / (p.name + ".dcm")),
                str(error_dir / p.name),
                str(work_dir),
                unit.token,
                drops,
                compress_map,
                dicom_rules,
                allowed_senders,
            ): p
            for p in jobs
        }
        for future in as_completed(futs):
            try:
                result = future.result()
            except Exception as exc:
                failed_count += 1
                source = futs[future]
                quarantine_error = None
                try:
                    _quarantine_failed_source(source, error_dir)
                except OSError as quarantine_exc:
                    quarantine_error = safe_error_detail(quarantine_exc)
                _mark_instance_failed(db, instance_by_path[source], type(exc).__name__)
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.compact.file",
                    resource=f"unit:{unit.id}",
                    status="failure",
                    error=exc,
                    error_detail=safe_error_detail(exc),
                    unit_id=unit.id,
                    filename=source.name,
                    quarantine_error=quarantine_error,
                )
                continue
            if result.codec_method in codec_counts:
                codec_counts[result.codec_method] += 1
            persistence_buffer.append(
                replace(result, instance_id=instance_by_path[futs[future]])
            )
            if len(persistence_buffer) >= max(1, COMPACT_DB_BATCH_SIZE):
                recorded, persist_failures = persist_compact_chunk(
                    db,
                    unit,
                    persistence_buffer,
                )
                _publish_outputs(db, unit, persistence_buffer, recorded)
                processed_count += len(recorded)
                failed_count += persist_failures
                persistence_buffer.clear()
    if persistence_buffer:
        recorded, persist_failures = persist_compact_chunk(
            db,
            unit,
            persistence_buffer,
        )
        _publish_outputs(db, unit, persistence_buffer, recorded)
        processed_count += len(recorded)
        failed_count += persist_failures
    elapsed_seconds = max(perf_counter() - started_at, 0.001)
    with log_context(batch_correlation):
        log_event(
            log,
            logging.INFO,
            "dicom.compact.batch",
            resource=f"unit:{unit.id}",
            status="partial" if failed_count else "success",
            started_at=started_at,
            unit_id=unit.id,
            file_count=len(jobs),
            processed_count=processed_count,
            failed_count=failed_count,
            backlog_hint=len(jobs) >= COMPACT_BATCH_SIZE,
            files_per_second=round(processed_count / elapsed_seconds, 2),
            lossless_count=codec_counts[METHOD_LOSSLESS],
            lossy_count=codec_counts[METHOD_LOSSY],
            copy_count=codec_counts[METHOD_COPY],
            db_batch_size=max(1, COMPACT_DB_BATCH_SIZE),
        )
    return len(jobs) >= COMPACT_BATCH_SIZE


_codec_pool = CodecPool("app.compaction:prepare", COMPACT_GLOBAL_WORKERS)


# A crash may come from the worker rather than the file: try once more on a
# fresh process before quarantining.
_CODEC_CRASH_ATTEMPTS = 2


def _run_codec_job(job: CompactJob) -> CompactResult:
    result = _run_isolated_job(job)
    if result.status != "compression_error" or job.force_copy:
        return result
    # The encoder crashed, hung or failed: deliver the image as received
    # (metadata, token and rules still applied) instead of quarantining it.
    fallback = _run_isolated_job(replace(job, force_copy=True))
    if fallback.status != "compressed":
        return result
    return replace(fallback, codec_reason=f"fallback:{result.error_type}")


def _run_isolated_job(job: CompactJob) -> CompactResult:
    name = Path(job.source).name
    for attempt in range(1, _CODEC_CRASH_ATTEMPTS + 1):
        try:
            return _codec_pool.run(job, timeout=max(1, COMPACT_FILE_TIMEOUT_SECONDS))
        except CodecTimeout:
            return CompactResult(name, name, "", "compression_error", "CodecTimeout")
        except CodecCrash:
            if attempt == _CODEC_CRASH_ATTEMPTS:
                return CompactResult(name, name, "", "compression_error", "CodecCrash")
    raise AssertionError("unreachable")


def _compact_one(
    filepath: str,
    dest: str,
    error: str,
    work_dir: str,
    token: str,
    drops: set[str],
    compress_map: dict[str, str],
    dicom_rules: tuple[RuleSpec, ...],
    allowed_senders: frozenset[str] = frozenset(),
) -> CompactResult:
    source_path = Path(filepath)
    dest_path = Path(dest)
    error_dir = Path(error).parent
    temp_root = Path(work_dir)
    temp_root.mkdir(parents=True, exist_ok=True)
    # UUID-only names avoid filesystem length failures when a PACS sends an
    # unusually long SOP-based filename.
    temp_dest = temp_root / f".{new_correlation_id()}.output.tmp"
    job = CompactJob(
        source=str(source_path),
        output_name=dest_path.name,
        temp_output=str(temp_dest),
        token=token,
        drops=frozenset(drops),
        compress_map=dict(compress_map),
        dicom_rules=tuple(dicom_rules),
        allowed_senders=frozenset(allowed_senders),
        max_encode_bytes=COMPACT_MAX_ENCODE_BYTES,
    )
    result = _run_codec_job(job)
    if result.status in {"discarded_rule", "discarded_modality"}:
        os.remove(source_path)
        return result
    if result.status != "compressed":
        with suppress(OSError):
            temp_dest.unlink(missing_ok=True)
        _quarantine_failed_source(source_path, error_dir)
        return result
    # The output stays in temp_dest: it is published only after its
    # transfer row, with path and hash, is committed (_publish_outputs).
    return result
