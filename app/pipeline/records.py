"""Persisting compaction results and linking images to their orders."""

from __future__ import annotations

import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import delete, func, or_, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.compaction import CompactResult
from app.config import (
    COMPACT_DB_BATCH_SIZE,
)
from app.events import add_event
from app.models import (
    AuditLog,
    DicomInstance,
    DicomRuleApplication,
    HistoricalImageLink,
    HistoricalStudy,
    ImageTransfer,
    Order,
    Unit,
)
from app.observability import (
    log_context,
    log_event,
    new_correlation_id,
    safe_error_detail,
)
from app.parse import (
    patient_id_matches,
)
from app.pipeline.common import bounded_db_text, ensure_order_correlation, log
from app.pipeline.find import prior_date_range
from app.pipeline.monitor import reset_monitoring, start_monitoring
from app.rules import monitor_plan_for, prior_skip_reason, schedule_from_now


class InboundDicomRejected(RuntimeError):
    """The received object cannot safely create or identify an order."""


_REQUIRED_INBOUND_FIELDS = {
    "study_uid": "StudyInstanceUID",
    "accession": "AccessionNumber",
    "patient_id": "PatientID",
    "birth_date": "PatientBirthDate",
}


def _prefetch_compact_state(
    db: Session,
    unit: Unit,
    results: list[CompactResult],
) -> tuple[dict[str, Order], dict[str, ImageTransfer]]:
    """Load common associations once for a persistence chunk."""
    study_uids = {result.study_uid for result in results if result.study_uid}
    filenames = {
        result.output_name or result.source_name
        for result in results
        if result.output_name or result.source_name
    }
    active_orders: dict[str, Order] = {}
    if study_uids:
        rows = db.scalars(
            select(Order)
            .where(
                Order.unit_id == unit.id,
                Order.study_uid.in_(study_uids),
                Order.archived_at.is_(None),
                Order.status.notin_(("done", "cancelled")),
            )
            .order_by(Order.id.desc())
        )
        for order in rows:
            active_orders.setdefault(order.study_uid, order)
    transfers: dict[str, ImageTransfer] = {}
    if filenames:
        transfers = {
            transfer.filename: transfer
            for transfer in db.scalars(
                select(ImageTransfer).where(
                    ImageTransfer.unit_id == unit.id,
                    ImageTransfer.filename.in_(filenames),
                )
            )
        }
    return active_orders, transfers


def _persist_compact_result_with_retry(
    db: Session,
    unit: Unit,
    result: CompactResult,
) -> tuple[int, str] | None | bool:
    """Persist one result; False when it could not be recorded."""
    for persist_attempt in range(1, 4):
        try:
            recorded = _record_compact_result(db, unit, result)
            db.commit()
            return recorded
        except Exception as exc:
            db.rollback()
            retryable = isinstance(exc, SQLAlchemyError)
            exhausted = persist_attempt == 3 or not retryable
            log_event(
                log,
                logging.ERROR if exhausted else logging.WARNING,
                "dicom.compact.persist",
                resource=f"unit:{unit.id}",
                status="failure" if exhausted else "retry",
                error=exc,
                error_detail=safe_error_detail(exc),
                unit_id=unit.id,
                filename=result.output_name or result.source_name,
                attempt=persist_attempt,
            )
            if exhausted:
                return False
    return False


RecordedResult = tuple[CompactResult, int | None, str]


def persist_compact_chunk(
    db: Session,
    unit: Unit,
    results: list[CompactResult],
) -> tuple[list[RecordedResult], int]:
    """Commit a small chunk, falling back per file on any conflict.

    Returns the recorded results with their transfer id and status, and the
    number of failures (processing errors or results that could not be saved).
    """
    if not results:
        return [], 0
    if len(results) > 1 and COMPACT_DB_BATCH_SIZE > 1:
        try:
            active_orders, transfers = _prefetch_compact_state(db, unit, results)
            recorded = [
                _record_compact_result(
                    db,
                    unit,
                    result,
                    active_orders=active_orders,
                    transfers=transfers,
                )
                for result in results
            ]
            db.commit()
            return (
                [
                    (result, *(row or (None, result.status)))
                    for result, row in zip(results, recorded, strict=True)
                ],
                sum(bool(result.error_type) for result in results),
            )
        except Exception as exc:
            db.rollback()
            log_event(
                log,
                logging.WARNING,
                "dicom.compact.persist.batch",
                resource=f"unit:{unit.id}",
                status="fallback",
                error=exc,
                error_detail=safe_error_detail(exc),
                unit_id=unit.id,
                file_count=len(results),
            )

    recorded_results: list[RecordedResult] = []
    failed_count = 0
    for result in results:
        row = _persist_compact_result_with_retry(db, unit, result)
        if row is False:
            failed_count += 1
            continue
        recorded_results.append((result, *(row or (None, result.status))))
        failed_count += int(bool(result.error_type))
    return recorded_results, failed_count


def _instance_state_for(transfer_status: str) -> str:
    if transfer_status in {"publishing", "compressed"}:
        return "compacted"
    if transfer_status.startswith("discarded"):
        return "discarded"
    if transfer_status == "rejected_sender":
        return "rejected"
    return "error"


def _finish_instance(
    db: Session, result: CompactResult, transfer: ImageTransfer
) -> None:
    """Close the queue row in the same transaction as its transfer."""
    if result.instance_id is None:
        return
    instance = db.get(DicomInstance, result.instance_id)
    if instance is None:
        return
    instance.state = _instance_state_for(transfer.status)
    instance.transfer_id = transfer.id
    instance.claimed_at = None
    instance.last_error = bounded_db_text(transfer.last_error, 500)


def _record_compact_result(
    db: Session,
    unit: Unit,
    result: CompactResult,
    *,
    active_orders: dict[str, Order] | None = None,
    transfers: dict[str, ImageTransfer] | None = None,
) -> tuple[int, str] | None:
    """Record the result; returns the transfer id and status it ended in."""
    # An exact active current-study match always wins. Completed/archived orders
    # are considered only after the historical matcher, otherwise a study that
    # is currently being retrieved as history could be attached to an old main
    # order instead of the active historical flow.
    order = None
    if result.status == "rejected_sender":
        _record_rejected_sender(db, unit, result, transfers)
        return None
    if result.study_uid:
        if active_orders is not None:
            order = active_orders.get(result.study_uid)
        else:
            order = db.scalar(
                select(Order)
                .where(
                    Order.unit_id == unit.id,
                    Order.study_uid == result.study_uid,
                    Order.archived_at.is_(None),
                    Order.status.notin_(("done", "cancelled")),
                )
                .order_by(Order.id.desc())
                .limit(1)
            )
    if order is not None:
        _enrich_order_from_received(order, result)
    correlation_id = (
        ensure_order_correlation(order) if order is not None else new_correlation_id()
    )
    filename = result.output_name or result.source_name
    if transfers is not None:
        transfer = transfers.get(filename)
    else:
        transfer = db.scalar(
            select(ImageTransfer).where(
                ImageTransfer.unit_id == unit.id,
                ImageTransfer.filename == filename,
            )
        )
    if transfer is None:
        transfer = ImageTransfer(
            unit_id=unit.id,
            order_id=order.id if order else None,
            filename=bounded_db_text(result.output_name or result.source_name, 500),
            correlation_id=correlation_id,
            study_uid=bounded_db_text(result.study_uid, 128),
        )
        db.add(transfer)
        db.flush()
        if transfers is not None:
            transfers[filename] = transfer
    else:
        transfer.order_id = order.id if order else transfer.order_id
        transfer.correlation_id = correlation_id
        transfer.study_uid = (
            bounded_db_text(result.study_uid, 128) or transfer.study_uid
        )
        transfer.attempts = 0
        transfer.next_attempt_at = None
        transfer.last_http_status = None

    association = "current" if order is not None else ""
    rejection_type = ""
    rejection_detail = ""
    if order is None:
        historical_order = _associate_historical_transfer(db, unit, result, transfer)
        if historical_order is not None:
            order = historical_order
            association = "historical"
            transfer.order_id = order.id
            transfer.correlation_id = ensure_order_correlation(order)
        elif not result.status.startswith("discarded"):
            try:
                order, association = _register_store_received_order(db, unit, result)
            except InboundDicomRejected as exc:
                rejection_type = type(exc).__name__
                rejection_detail = str(exc)
                transfer.order_id = None
                _quarantine_compacted_output(unit, result)
            else:
                transfer.order_id = order.id
                transfer.correlation_id = ensure_order_correlation(order)

    if (
        active_orders is not None
        and order is not None
        and order.study_uid == result.study_uid
        and association != "historical"
    ):
        active_orders[result.study_uid] = order

    if rejection_type:
        transfer.status = "metadata_error"
    elif result.status == "compressed":
        # Not visible to the sender until the file is in send_dir.
        transfer.status = "publishing"
    else:
        transfer.status = result.status
    transfer.sha256 = result.output_sha256 if result.status == "compressed" else ""
    transfer.last_error = bounded_db_text(
        rejection_detail or result.error_type,
        500,
    )
    db.flush()
    _finish_instance(db, result, transfer)
    db.execute(
        delete(DicomRuleApplication).where(
            DicomRuleApplication.transfer_id == transfer.id
        )
    )
    db.add_all(
        DicomRuleApplication(
            rule_id=match.rule_id,
            unit_id=unit.id,
            transfer_id=transfer.id,
            rule_name=match.rule_name,
            action=match.action,
        )
        for match in result.rule_matches
    )

    if rejection_type:
        with log_context(transfer.correlation_id):
            log_event(
                log,
                logging.WARNING,
                "dicom.inbound.reject",
                resource=f"transfer:{transfer.id}",
                status="rejected",
                error_type=rejection_type,
                error_detail=rejection_detail,
                transfer_id=transfer.id,
                unit_id=unit.id,
            )
    level = (
        logging.ERROR
        if result.error_type or rejection_type
        else logging.DEBUG
        if result.status == "compressed"
        else logging.INFO
    )
    with log_context(transfer.correlation_id):
        log_event(
            log,
            level,
            "dicom.compact",
            resource=f"transfer:{transfer.id}",
            status="failure" if result.error_type or rejection_type else result.status,
            error_type=rejection_type or result.error_type or None,
            transfer_id=transfer.id,
            order_id=order.id if order else None,
            unit_id=unit.id,
            association=association or None,
            applied_rule_ids=[match.rule_id for match in result.rule_matches],
        )
    return transfer.id, transfer.status


def _record_rejected_sender(
    db: Session,
    unit: Unit,
    result: CompactResult,
    transfers: dict[str, ImageTransfer] | None,
) -> None:
    """Audit a quarantined object from a calling AE outside the allowlist."""
    filename = bounded_db_text(result.source_name, 500)
    transfer = (
        transfers.get(filename)
        if transfers is not None
        else db.scalar(
            select(ImageTransfer).where(
                ImageTransfer.unit_id == unit.id,
                ImageTransfer.filename == filename,
            )
        )
    )
    if transfer is None:
        transfer = ImageTransfer(
            unit_id=unit.id,
            filename=filename,
            correlation_id=new_correlation_id(),
        )
        db.add(transfer)
        if transfers is not None:
            transfers[filename] = transfer
    transfer.order_id = None
    transfer.status = result.status
    transfer.last_error = result.error_type
    transfer.next_attempt_at = None
    db.flush()
    _finish_instance(db, result, transfer)
    with log_context(transfer.correlation_id):
        log_event(
            log,
            logging.WARNING,
            "dicom.inbound.reject",
            resource=f"transfer:{transfer.id}",
            status="rejected",
            error_type=result.error_type,
            transfer_id=transfer.id,
            unit_id=unit.id,
            calling_aet=result.sender_aet or None,
        )


def _missing_inbound_fields(result: CompactResult) -> list[str]:
    return [
        dicom_name
        for attribute, dicom_name in _REQUIRED_INBOUND_FIELDS.items()
        if not bounded_db_text(getattr(result, attribute), 128)
    ]


def _enrich_order_from_received(order: Order, result: CompactResult) -> None:
    """Fill information missing from an order as later series arrive."""
    order.pat_id = order.pat_id or bounded_db_text(result.patient_id, 64)
    order.birth_date = order.birth_date or bounded_db_text(result.birth_date, 16)
    order.exam_date = order.exam_date or bounded_db_text(result.study_date, 16)
    order.modality = order.modality or bounded_db_text(result.modality, 32)
    order.body_part = order.body_part or bounded_db_text(result.body_part, 64)


def _quarantine_compacted_output(unit: Unit, result: CompactResult) -> None:
    """Keep an invalid inbound object out of the cloud upload queue."""
    filename = result.output_name or result.source_name
    if not filename or not result.temp_output:
        # Compression/rule failures are already moved to the error directory.
        return
    source = Path(result.temp_output)
    if not source.is_file():
        return
    error_dir = Path(unit.error_dir)
    error_dir.mkdir(parents=True, exist_ok=True)
    target = error_dir / filename
    if target.exists():
        target = error_dir / (
            f"{target.stem}.metadata-{new_correlation_id()[:8]}{target.suffix}"
        )
    try:
        shutil.move(str(source), str(target))
    except OSError as exc:
        log_event(
            log,
            logging.ERROR,
            "dicom.inbound.quarantine",
            resource=f"unit:{unit.id}",
            status="failure",
            error=exc,
            error_detail=safe_error_detail(exc),
            unit_id=unit.id,
            filename=filename,
        )


def _register_store_received_order(
    db: Session,
    unit: Unit,
    result: CompactResult,
) -> tuple[Order, str]:
    """Create/reuse one order for a valid study received directly by Store SCP."""
    missing = _missing_inbound_fields(result)
    if missing:
        raise InboundDicomRejected(
            "Tags DICOM obrigatórias ausentes: " + ", ".join(missing)
        )

    study_uid = bounded_db_text(result.study_uid, 128)
    accession = bounded_db_text(result.accession, 64)
    # Results from the compression pool are persisted serially today, but this
    # database lock keeps the invariant if more workers are deployed.
    db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"storescp:{unit.id}:{study_uid}"},
    )
    order = db.scalar(
        select(Order)
        .where(Order.unit_id == unit.id, Order.study_uid == study_uid)
        .order_by(Order.archived_at.is_(None).desc(), Order.id.desc())
        .limit(1)
    )
    matched_by = "study_uid"
    if order is None:
        order = db.scalar(
            select(Order).where(Order.unit_id == unit.id, Order.acc == accession)
        )
        matched_by = "accession"
    if order is not None and order.study_uid and order.study_uid != study_uid:
        raise InboundDicomRejected(
            "AccessionNumber já pertence a outro StudyInstanceUID nesta unidade"
        )

    now = datetime.now()
    modality, _ignored_first_at = schedule_from_now(db, result.modality)
    monitor = monitor_plan_for(db, modality)
    if order is None:
        correlation_id = new_correlation_id()
        order = Order(
            unit_id=unit.id,
            source_id=bounded_db_text(
                f"storescp:{unit.id}:{accession}",
                64,
            ),
            pat_id=bounded_db_text(result.patient_id, 64),
            acc=accession,
            birth_date=bounded_db_text(result.birth_date, 16),
            exam_date=bounded_db_text(result.study_date, 16),
            correlation_id=correlation_id,
            api_read_status="confirmed",
            api_read_at=now,
            status="done",
            study_uid=study_uid,
            modality=bounded_db_text(modality, 32),
            body_part=bounded_db_text(result.body_part, 64),
            prior_status="disabled",
            retrieve_at=None,
            found_at=now,
            done_at=now,
        )
        db.add(order)
        db.flush()
        window = start_monitoring(order, monitor, now) if monitor else ""
        db.add(
            AuditLog(
                actor_id=None,
                actor_username="Sistema",
                actor_role="system",
                action="create",
                resource_type="order",
                resource_id=str(order.id),
                resource_name=order.acc,
                summary=(
                    "Pedido criado automaticamente a partir de exame recebido "
                    f"diretamente pelo Store SCP da unidade {unit.name}."
                ),
                ip_address="",
            )
        )
        if window:
            message = f"Exame recebido diretamente pelo Store SCP. {window}"
        else:
            message = (
                "Exame recebido diretamente pelo Store SCP; modalidade sem "
                "monitoramento de novas imagens"
            )
        add_event(db, order, message)
        action = "created"
    else:
        was_archived = order.archived_at is not None
        was_watching = order.status == "watching"
        order.study_uid = order.study_uid or study_uid
        _enrich_order_from_received(order, result)
        order.modality = order.modality or bounded_db_text(modality, 32)
        order.found_at = order.found_at or now
        ensure_order_correlation(order)

        # A direct delivery can satisfy a queued order that had not yet found
        # the study. An archived order is restored as requested. A completed,
        # non-archived order is merely reused, so every image in the same batch
        # cannot repeatedly restart the monitoring window.
        if order.status == "watching" or was_archived:
            order.retrieve_at = None
            order.attempts = 0
            order.last_error = ""
            order.heartbeat_at = None
            reset_monitoring(order)
            if monitor:
                add_event(
                    db,
                    order,
                    "Exame recebido diretamente pelo Store SCP. "
                    + start_monitoring(order, monitor, now),
                )
            else:
                order.status = "done"
                order.done_at = now
        prior_skip = prior_skip_reason(db, unit, order.modality) if was_watching else ""
        if prior_skip is None:
            prior_from, prior_to = prior_date_range(now.date())
            order.prior_status = "queued"
            order.prior_date_from = prior_from
            order.prior_date_to = prior_to
            order.prior_due_at = now
            order.prior_started_at = None
            order.prior_completed_at = None
            order.prior_heartbeat_at = None
            order.prior_attempts = 0
            order.prior_last_error = ""
        elif prior_skip:
            add_event(db, order, prior_skip)
        if was_archived:
            order.archived_at = None
            order.archive_reason = ""
            order.archived_by_user_id = None
            order.archived_by_username = ""
            db.add(
                AuditLog(
                    actor_id=None,
                    actor_username="Sistema",
                    actor_role="system",
                    action="restore",
                    resource_type="order",
                    resource_id=str(order.id),
                    resource_name=order.acc,
                    summary=(
                        "Pedido restaurado automaticamente após novo recebimento "
                        "do mesmo Study Instance UID pelo Store SCP."
                    ),
                    ip_address="",
                )
            )
            add_event(
                db,
                order,
                "Pedido arquivado restaurado após recebimento direto do estudo",
            )
            action = "restored"
        elif order.status in ("monitoring", "done") and matched_by == "accession":
            add_event(
                db,
                order,
                "Exame recebido diretamente e associado ao pedido pelo accession",
            )
            if prior_skip is None:
                add_event(
                    db,
                    order,
                    "Retrieve histórico mantido e enfileirado para execução imediata",
                )
            action = "matched"
        else:
            action = "reused"

    with log_context(ensure_order_correlation(order)):
        log_event(
            log,
            logging.INFO,
            "order.storescp.ingest",
            resource=f"order:{order.id}",
            status="success",
            order_id=order.id,
            unit_id=unit.id,
            result=action,
            matched_by=matched_by if action != "created" else None,
            modality=order.modality,
            monitor_until=(
                order.monitor_until.isoformat() if order.monitor_until else None
            ),
        )
    return order, f"storescp_{action}"


def _associate_historical_transfer(
    db: Session,
    unit: Unit,
    result: CompactResult,
    transfer: ImageTransfer,
) -> Order | None:
    if not result.study_uid or result.observed_at is None:
        return None
    known_studies = list(
        db.scalars(
            select(HistoricalStudy)
            .join(Order, Order.id == HistoricalStudy.order_id)
            .where(
                HistoricalStudy.unit_id == unit.id,
                HistoricalStudy.study_uid == result.study_uid,
                Order.archived_at.is_(None),
                Order.prior_status != "disabled",
                Order.prior_started_at.is_not(None),
                Order.prior_started_at <= result.observed_at,
                or_(
                    Order.prior_completed_at.is_(None),
                    Order.prior_completed_at
                    >= result.observed_at - timedelta(minutes=1),
                ),
            )
        )
    )
    if known_studies:
        for study in known_studies:
            _link_historical_transfer(db, study, transfer, result)
        return known_studies[0].order

    if not (result.birth_date and result.modality and result.study_date):
        return None
    # Push the selective identity/date predicates into SQL: this runs for every
    # file without a current order, so it must not load all active histories.
    filters = [
        Order.unit_id == unit.id,
        Order.archived_at.is_(None),
        Order.prior_status != "disabled",
        Order.prior_started_at.is_not(None),
        Order.prior_started_at <= result.observed_at,
        or_(
            Order.prior_completed_at.is_(None),
            Order.prior_completed_at >= result.observed_at - timedelta(minutes=1),
        ),
        Order.birth_date == result.birth_date,
        func.upper(Order.modality) == result.modality.upper(),
        Order.study_uid != result.study_uid,
        Order.prior_date_from <= result.study_date,
        Order.prior_date_to >= result.study_date,
    ]
    if not unit.pacs_patient_id_wildcard:
        filters.append(Order.pat_id == result.patient_id.strip())
    candidates = list(db.scalars(select(Order).where(*filters)))
    matched_order = None
    for order in candidates:
        if result.study_uid == order.study_uid:
            continue
        if order.prior_completed_at and result.observed_at > (
            order.prior_completed_at + timedelta(minutes=1)
        ):
            continue
        if not patient_id_matches(
            result.patient_id,
            order.pat_id,
            allow_suffix=unit.pacs_patient_id_wildcard,
        ):
            continue
        if not result.birth_date or result.birth_date != order.birth_date:
            continue
        if not result.modality or not order.modality:
            continue
        if result.modality.upper() != order.modality.upper():
            continue
        if order.body_part and result.body_part.upper() != order.body_part.upper():
            continue
        if not result.study_date or not (
            order.prior_date_from <= result.study_date <= order.prior_date_to
        ):
            continue
        study = db.scalar(
            select(HistoricalStudy).where(
                HistoricalStudy.order_id == order.id,
                HistoricalStudy.study_uid == result.study_uid,
            )
        )
        if study is None:
            study = HistoricalStudy(
                order_id=order.id,
                unit_id=unit.id,
                study_uid=bounded_db_text(result.study_uid, 128),
                accession=bounded_db_text(result.accession, 64),
                study_date=bounded_db_text(result.study_date, 16),
                modality=bounded_db_text(result.modality, 32),
                body_part=bounded_db_text(result.body_part, 64),
                description=bounded_db_text(result.description, 255),
            )
            db.add(study)
            db.flush()
        _link_historical_transfer(db, study, transfer, result)
        matched_order = matched_order or order
    return matched_order


def _link_historical_transfer(
    db: Session,
    study: HistoricalStudy,
    transfer: ImageTransfer,
    result: CompactResult,
) -> None:
    study.accession = study.accession or bounded_db_text(result.accession, 64)
    study.study_date = study.study_date or bounded_db_text(result.study_date, 16)
    study.modality = study.modality or bounded_db_text(result.modality, 32)
    study.body_part = study.body_part or bounded_db_text(result.body_part, 64)
    study.description = study.description or bounded_db_text(result.description, 255)
    link = db.scalar(
        select(HistoricalImageLink).where(
            HistoricalImageLink.historical_study_id == study.id,
            HistoricalImageLink.transfer_id == transfer.id,
        )
    )
    if link is None:
        db.add(
            HistoricalImageLink(
                historical_study_id=study.id,
                transfer_id=transfer.id,
            )
        )
