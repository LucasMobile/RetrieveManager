from __future__ import annotations

import asyncio
import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import BoundedSemaphore
from time import monotonic, perf_counter

import aiohttp
import pydicom
from sqlalchemy import delete, func, or_, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.compression import compression_runtime_settings, find_modalities_for_unit
from app.config import (
    CIRCUIT_BREAKER_FAILURES,
    CIRCUIT_BREAKER_SECONDS,
    COMPACT_BATCH_SIZE,
    COMPACT_GLOBAL_WORKERS,
    FIND_BATCH_SIZE,
    HTTP_CONNECT_TIMEOUT_SECONDS,
    HTTP_TOTAL_TIMEOUT_SECONDS,
    ORDERS_API_ACK_BATCH_SIZE,
    ORDERS_API_ACK_CONCURRENCY,
    ORDERS_API_POLL_SECONDS,
    SEND_BATCH_SIZE,
    SEND_RETRY_BASE_SECONDS,
    SEND_RETRY_MAX_SECONDS,
)
from app.dicom_rules import (
    RuleExecutionError,
    RuleMatch,
    RuleSpec,
    apply_rule_specs,
    load_rule_specs,
)
from app.dicom_tools import (
    ToolMissing,
    c_find,
    c_find_prior,
    c_find_series_body_part,
    c_move,
    c_move_prior_series,
    dcmcjpeg,
    redact_dicom_output,
)
from app.events import add_event
from app.models import (
    AuditLog,
    DicomRuleApplication,
    HistoricalImageLink,
    HistoricalSeries,
    HistoricalStudy,
    ImageTransfer,
    ManualMoveRequest,
    Order,
    Unit,
)
from app.observability import (
    log_context,
    log_event,
    new_correlation_id,
    safe_error_detail,
)
from app.order_state import ACTIVE_ORDER_STATUSES
from app.orders_api import (
    InvalidApiOrder,
    OrdersApiError,
    fetch_orders,
    iter_acknowledgements,
    parse_api_order,
)
from app.parse import (
    PriorSeriesResult,
    first_allowed_modality,
    parse_findscu_output,
    parse_prior_findscu_output,
    parse_series_body_part,
    parse_series_metadata,
    series_response_has_modality,
)
from app.retention import archive_order
from app.rules import schedule_from_now

log = logging.getLogger("worker")
STALE_LOCK = timedelta(minutes=20)
PRIOR_RETRY_DELAYS = (60, 300)
CURRENT_MOVE_RETRY_DELAYS = (60, 300)
UNMATCHED_ORDER_RETENTION = timedelta(days=1)
UNMATCHED_ORDER_CLEANUP_BATCH = 500
COMPLETED_ORDER_RETENTION = timedelta(weeks=2)
COMPLETED_ORDER_CLEANUP_BATCH = 500
_compact_slots = BoundedSemaphore(max(1, COMPACT_GLOBAL_WORKERS))


@dataclass(frozen=True)
class CompactResult:
    source_name: str
    output_name: str
    study_uid: str
    status: str
    error_type: str = ""
    patient_id: str = ""
    birth_date: str = ""
    study_date: str = ""
    accession: str = ""
    modality: str = ""
    body_part: str = ""
    description: str = ""
    observed_at: datetime | None = None
    rule_matches: tuple[RuleMatch, ...] = ()


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


_cloud_circuits: dict[int, CircuitState] = {}
_last_orders_api_poll: dict[int, float] = {}


def _ensure_order_correlation(order: Order) -> str:
    if not order.correlation_id:
        order.correlation_id = new_correlation_id()
    return order.correlation_id


def _bounded_db_text(value: object, limit: int) -> str:
    """Normalize external PACS/file text before writing bounded DB columns."""
    return str(value or "").replace("\x00", " ").strip()[:limit]


class InboundDicomRejected(RuntimeError):
    """The received object cannot safely create or identify an order."""


_REQUIRED_INBOUND_FIELDS = {
    "study_uid": "StudyInstanceUID",
    "accession": "AccessionNumber",
    "patient_id": "PatientID",
    "birth_date": "PatientBirthDate",
}


def recover_stale_locks(db: Session) -> None:
    now = datetime.now()
    cutoff = now - STALE_LOCK
    rows = list(
        db.scalars(
            select(Order).where(
                Order.archived_at.is_(None),
                Order.status.in_(ACTIVE_ORDER_STATUSES),
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
    # O C-MOVE histórico pode legitimamente durar mais que o lock padrão.
    # Como o subprocesso é bloqueante, o heartbeat não avança durante a execução;
    # respeite o timeout configurado na unidade antes de considerar o job órfão.
    prior_candidates = list(
        db.scalars(
            select(Order).where(
                Order.archived_at.is_(None),
                Order.prior_status == "retrieving",
            )
        )
    )
    prior_rows = [
        order
        for order in prior_candidates
        if order.prior_heartbeat_at is None
        or order.prior_heartbeat_at
        < now
        - max(
            STALE_LOCK,
            timedelta(seconds=max(order.unit.move_timeout_prior, 60) + 120),
        )
    ]
    for order in prior_rows:
        order.prior_status = "queued"
        order.prior_due_at = now
        order.prior_last_error = "lock órfão do histórico recuperado"
        add_event(
            db,
            order,
            "Lock órfão do retrieve histórico recuperado após queda do worker",
            level="warn",
        )
    if prior_rows:
        db.commit()

    manual_candidates = list(
        db.execute(
            select(ManualMoveRequest, Unit.move_timeout_first)
            .join(Unit, Unit.id == ManualMoveRequest.unit_id)
            .where(ManualMoveRequest.status == "running")
        )
    )
    manual_rows = [
        request
        for request, timeout in manual_candidates
        if request.started_at is None
        or request.started_at
        < now - timedelta(seconds=max(int(timeout), 60) + 120)
    ]
    for request in manual_rows:
        request.status = "queued"
        request.started_at = None
        request.last_error = "lock órfão do C-MOVE manual recuperado"
    if manual_rows:
        db.commit()


def prior_date_range(today: date | None = None) -> tuple[str, str]:
    current = today or date.today()
    try:
        start = current.replace(year=current.year - 3)
    except ValueError:
        start = current.replace(year=current.year - 3, day=28)
    end = current - timedelta(days=1)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def ingest_unit(db: Session, unit: Unit) -> int:
    now = monotonic()
    last_poll = _last_orders_api_poll.get(unit.id)
    if last_poll is not None and now - last_poll < ORDERS_API_POLL_SECONDS:
        return 0
    _last_orders_api_poll[unit.id] = now

    if not unit.orders_api_url or not unit.orders_api_token:
        log_event(
            log,
            logging.WARNING,
            "orders.api.configuration",
            resource=f"unit:{unit.id}",
            status="failure",
            error_type="MissingConfiguration",
            unit_id=unit.id,
        )
        return 0

    payload: list[dict] = []
    try:
        payload = asyncio.run(fetch_orders(unit.orders_api_url, unit.orders_api_token))
        log_event(
            log,
            logging.INFO,
            "orders.api.get",
            resource=f"unit:{unit.id}",
            status="success",
            unit_id=unit.id,
            record_count=len(payload),
        )
    except OrdersApiError as exc:
        log_event(
            log,
            logging.ERROR,
            "orders.api.get",
            resource=f"unit:{unit.id}",
            status="failure",
            error=exc,
            error_detail=str(exc),
            unit_id=unit.id,
        )

    created = 0
    seen: set[str] = set()
    station_id = (unit.orders_api_station_id or "").strip()
    for item in payload:
        if not isinstance(item, dict):
            log_event(
                log,
                logging.WARNING,
                "order.api.ingest",
                resource=f"unit:{unit.id}",
                status="rejected",
                error_type="InvalidApiOrder",
                unit_id=unit.id,
            )
            continue
        if item.get("mirthReaded") is True:
            continue
        if station_id and str(item.get("idPosto") or "").strip() != station_id:
            continue
        try:
            parsed = parse_api_order(item)
        except InvalidApiOrder as exc:
            log_event(
                log,
                logging.WARNING,
                "order.api.ingest",
                resource=f"unit:{unit.id}",
                status="rejected",
                error=exc,
                error_type="InvalidApiOrder",
                unit_id=unit.id,
            )
            continue
        if parsed.accession_number in seen:
            continue
        seen.add(parsed.accession_number)

        correlation_id = new_correlation_id()
        try:
            with db.begin_nested():
                exists = db.scalar(
                    select(Order).where(
                        Order.unit_id == unit.id,
                        Order.acc == parsed.accession_number,
                    )
                )
                if exists:
                    # A API ainda informou mirthReaded != true; ACK idempotente.
                    exists.api_read_status = "pending"
                    exists.api_read_last_error = ""
                    continue
                order = Order(
                    unit_id=unit.id,
                    source_id=parsed.source_id,
                    filename=parsed.accession_number,
                    pat_id=parsed.patient_id,
                    acc=parsed.accession_number,
                    birth_date=parsed.patient_birthdate,
                    exam_date=parsed.exam_date,
                    status="watching",
                    correlation_id=correlation_id,
                    api_read_status="pending",
                )
                db.add(order)
                db.flush()
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
                            "Pedido recebido automaticamente da unidade "
                            f"{unit.name}."
                        ),
                        ip_address="",
                    )
                )
                add_event(
                    db,
                    order,
                    "Pedido recebido da API PLERES: "
                    f"acc={parsed.accession_number}",
                )
        except SQLAlchemyError as exc:
            log_event(
                log,
                logging.ERROR,
                "order.api.ingest.persist",
                resource=f"unit:{unit.id}",
                status="rejected",
                error=exc,
                error_detail=safe_error_detail(exc),
                unit_id=unit.id,
            )
            continue
        with log_context(correlation_id):
            log_event(
                log,
                logging.INFO,
                "order.api.ingest",
                resource=f"order:{order.id}",
                status="success",
                order_id=order.id,
                unit_id=unit.id,
            )
        created += 1
    if created or seen:
        db.commit()
    return created


def acknowledge_pending_orders(db: Session, unit: Unit) -> None:
    pending = list(
        db.scalars(
            select(Order)
            .where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                Order.api_read_status == "pending",
            )
            .order_by(Order.api_read_attempts, Order.id)
            .limit(ORDERS_API_ACK_BATCH_SIZE)
        )
    )
    if not pending:
        return
    by_accession = {order.acc: order for order in pending}
    started_at = perf_counter()
    log_event(
        log,
        logging.INFO,
        "orders.api.ack.batch",
        resource=f"unit:{unit.id}",
        status="started",
        unit_id=unit.id,
        record_count=len(by_accession),
        concurrency=ORDERS_API_ACK_CONCURRENCY,
    )
    success_count = 0
    failure_count = 0

    async def acknowledge_batch() -> None:
        nonlocal success_count, failure_count
        async for result in iter_acknowledgements(
            unit.orders_api_url,
            unit.orders_api_token,
            list(by_accession),
            ORDERS_API_ACK_CONCURRENCY,
        ):
            order = by_accession[result.accession_number]
            order.api_read_attempts += 1
            order.api_read_at = datetime.now()
            if result.success:
                order.api_read_status = "confirmed"
                order.api_read_last_error = ""
                add_event(db, order, "Pedido confirmado como lido na API PLERES")
                level = logging.INFO
                status = "success"
                success_count += 1
            else:
                order.api_read_status = "pending"
                order.api_read_last_error = result.error[:500]
                if order.api_read_attempts == 1:
                    add_event(
                        db,
                        order,
                        "Falha ao confirmar leitura na API PLERES; "
                        "nova tentativa será feita",
                        level="warn",
                    )
                level = logging.WARNING
                status = "retry"
                failure_count += 1
            # Commit por resultado: uma queda no meio do lote não perde as
            # confirmações que a API já aceitou.
            db.commit()
            with log_context(_ensure_order_correlation(order)):
                log_event(
                    log,
                    level,
                    "orders.api.ack",
                    resource=f"order:{order.id}",
                    status=status,
                    order_id=order.id,
                    unit_id=unit.id,
                    attempt=order.api_read_attempts,
                    error_type="OrdersApiError" if not result.success else None,
                    error_detail=result.error if not result.success else None,
                )

    asyncio.run(acknowledge_batch())
    log_event(
        log,
        logging.WARNING if failure_count else logging.INFO,
        "orders.api.ack.batch",
        resource=f"unit:{unit.id}",
        status="partial" if failure_count else "success",
        started_at=started_at,
        unit_id=unit.id,
        record_count=len(by_accession),
        success_count=success_count,
        failure_count=failure_count,
    )


def cleanup_unmatched_orders(db: Session, now: datetime | None = None) -> int:
    current_time = now or datetime.now()
    cutoff = current_time - UNMATCHED_ORDER_RETENTION
    orders = list(
        db.scalars(
            select(Order)
            .where(
                Order.archived_at.is_(None),
                Order.status == "watching",
                Order.study_uid == "",
                Order.last_find_at.is_not(None),
                Order.last_error == "",
                Order.created_at <= cutoff,
            )
            .order_by(Order.created_at, Order.id)
            .limit(UNMATCHED_ORDER_CLEANUP_BATCH)
        )
    )
    return _archive_order_batch(
        db,
        orders,
        archived_at=current_time,
        reason="Nenhum exame localizado pelo C-FIND após 24 horas.",
        audit_summary=(
            "Pedido arquivado automaticamente após 24 horas sem exame "
            "localizado pelo C-FIND; histórico preservado."
        ),
        event_name="order.unmatched.archive",
        cutoff=cutoff,
    )


def _archive_order_batch(
    db: Session,
    orders: list[Order],
    *,
    archived_at: datetime,
    reason: str,
    audit_summary: str,
    event_name: str,
    cutoff: datetime,
) -> int:
    for order in orders:
        archive_order(db, order, reason=reason, archived_at=archived_at)
        db.add(
            AuditLog(
                actor_id=None,
                actor_username="Sistema",
                actor_role="system",
                action="archive",
                resource_type="order",
                resource_id=str(order.id),
                resource_name=order.acc,
                summary=audit_summary,
                ip_address="",
            )
        )
    if not orders:
        return 0
    db.commit()
    log_event(
        log,
        logging.INFO,
        event_name,
        resource="orders",
        status="success",
        archived_count=len(orders),
        cutoff=cutoff.isoformat(),
    )
    return len(orders)


def archive_completed_orders(db: Session, now: datetime | None = None) -> int:
    current_time = now or datetime.now()
    cutoff = current_time - COMPLETED_ORDER_RETENTION
    orders = list(
        db.scalars(
            select(Order)
            .where(
                Order.archived_at.is_(None),
                Order.status == "done",
                Order.done_at.is_not(None),
                Order.done_at <= cutoff,
                Order.prior_status.in_(("disabled", "done")),
            )
            .order_by(Order.done_at, Order.id)
            .limit(COMPLETED_ORDER_CLEANUP_BATCH)
        )
    )
    return _archive_order_batch(
        db,
        orders,
        archived_at=current_time,
        reason="Pedido concluído há 14 dias.",
        audit_summary=(
            "Pedido arquivado automaticamente 14 dias após a conclusão; "
            "histórico preservado."
        ),
        event_name="order.completed.archive",
        cutoff=cutoff,
    )


def find_pending(db: Session, unit: Unit) -> None:
    now = datetime.now()
    interval = timedelta(seconds=unit.find_interval_seconds or 30)
    orders = list(
        db.scalars(
            select(Order)
            .where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
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
        try:
            _find_one(db, unit, order, now)
        except SQLAlchemyError as exc:
            # One malformed PACS response must not poison the whole unit queue.
            order_id = order.id
            db.rollback()
            failed_order = db.get(Order, order_id)
            if failed_order is not None:
                failed_order.status = "error"
                failed_order.last_find_at = now
                failed_order.heartbeat_at = None
                failed_order.last_error = (
                    "Falha ao salvar o resultado do C-FIND no banco de dados"
                )
                add_event(
                    db,
                    failed_order,
                    "Resultado do C-FIND rejeitado pelo banco de dados",
                    level="error",
                )
                db.commit()
            log_event(
                log,
                logging.ERROR,
                "dicom.find.persist",
                resource=f"order:{order_id}",
                status="failure",
                error=exc,
                error_detail=_database_error_detail(exc),
                order_id=order_id,
                unit_id=unit.id,
            )
        except Exception as exc:
            # Erros de SO/parser/programação ficam contidos neste pedido. O pedido
            # permanece observável e elegível para uma nova consulta.
            order_id = order.id
            db.rollback()
            failed_order = db.get(Order, order_id)
            if failed_order is not None and failed_order.status == "watching":
                failed_order.last_find_at = now
                failed_order.heartbeat_at = None
                failed_order.last_error = (
                    f"Falha interna no C-FIND: {safe_error_detail(exc)}"
                )
                add_event(
                    db,
                    failed_order,
                    "C-FIND interrompido; consulta será repetida",
                    safe_error_detail(exc),
                    "warn",
                )
                db.commit()
            log_event(
                log,
                logging.ERROR,
                "dicom.find.internal",
                resource=f"order:{order_id}",
                status="retry",
                error=exc,
                error_detail=safe_error_detail(exc),
                order_id=order_id,
                unit_id=unit.id,
            )


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
        except OSError as exc:
            code, output = 126, str(exc)

        if code != 0:
            order.heartbeat_at = None
            order.last_error = f"C-FIND falhou (exit {code})"
            safe_output = redact_dicom_output(output)
            add_event(
                db,
                order,
                f"C-FIND falhou (exit {code}); consulta será repetida",
                safe_output,
                "warn",
            )
            db.commit()
            log_event(
                log,
                logging.WARNING,
                "dicom.find",
                resource=f"order:{order.id}",
                status="retry",
                started_at=started_at,
                order_id=order.id,
                unit_id=unit.id,
                command_status=code,
            )
            return

        study_uid, modalities, patient_name, body_part = parse_findscu_output(output)
        allowed_modalities = find_modalities_for_unit(db, unit.id)
        modality = first_allowed_modality(modalities, allowed_modalities)
        diagnostic_output = output
        if study_uid:
            try:
                series_code, series_output = c_find_series_body_part(
                    unit.calling_aet,
                    unit.pacs_aet,
                    unit.pacs_ip,
                    unit.pacs_port,
                    study_uid,
                )
            except ToolMissing as exc:
                series_code, series_output = 127, str(exc)
            diagnostic_output = (
                f"{output}\n\nC-FIND complementar de séries:\n{series_output}"
            )
            if series_code == 0:
                series_modality, series_body_part = parse_series_metadata(
                    series_output, allowed_modalities
                )
                if series_modality:
                    modality = series_modality
                    body_part = series_body_part
                elif series_response_has_modality(series_output):
                    # The PACS returned series, but none belongs to the accepted
                    # compression catalog. Keep watching instead of choosing SR/PR.
                    modality = ""
                    body_part = ""
                elif modality and not body_part:
                    # Compatibility with PACS nodes that omit Modality at SERIES.
                    body_part = parse_series_body_part(series_output)
        safe_output = redact_dicom_output(diagnostic_output)
        if not study_uid:
            order.heartbeat_at = None
            order.last_error = ""
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

        if not modality:
            order.heartbeat_at = None
            order.last_error = ""
            add_event(
                db,
                order,
                "C-FIND sem modalidade clínica válida; consulta será repetida",
                safe_output,
            )
            db.commit()
            log_event(
                log,
                logging.INFO,
                "dicom.find",
                resource=f"order:{order.id}",
                status="not_ready",
                started_at=started_at,
                order_id=order.id,
                unit_id=unit.id,
                command_status=code,
            )
            return

        modality, retrieve_at, second_at = schedule_from_now(db, modality)
        order.study_uid = study_uid
        order.modality = modality[:32]
        order.patient_name = patient_name[:255]
        order.body_part = body_part[:64]
        order.retrieve_at = retrieve_at
        order.second_retrieve_at = second_at
        order.found_at = now
        order.status = "wait_retrieve"
        order.heartbeat_at = None
        # A partir daqui, attempts mede somente tentativas do C-MOVE atual.
        order.attempts = 0
        order.last_error = ""
        if unit.retrieve_prior_enabled:
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
        else:
            order.prior_status = "disabled"
        add_event(
            db,
            order,
            f"Exame encontrado UID={study_uid} mod={modality}. "
            f"1º retrieve em {retrieve_at:%H:%M}",
            safe_output,
        )
        if unit.retrieve_prior_enabled:
            add_event(
                db,
                order,
                "Retrieve histórico enfileirado para execução imediata: "
                f"{order.prior_date_from}-{order.prior_date_to}",
            )
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


def _database_error_detail(exc: SQLAlchemyError) -> str:
    """Backward-compatible alias used by existing operational logging."""
    return safe_error_detail(exc)


def claim_due_moves(db: Session, unit: Unit) -> list[tuple[int, str]]:
    now = datetime.now()
    current_inflight = (
        db.scalar(
            select(func.count()).where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                Order.status.in_(ACTIVE_ORDER_STATUSES),
            )
        )
        or 0
    )
    prior_inflight = (
        db.scalar(
            select(func.count()).where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                Order.prior_status == "retrieving",
            )
        )
        or 0
    )
    manual_inflight = (
        db.scalar(
            select(func.count()).where(
                ManualMoveRequest.unit_id == unit.id,
                ManualMoveRequest.status == "running",
            )
        )
        or 0
    )
    slots = (
        max(1, unit.max_parallel_moves)
        - current_inflight
        - prior_inflight
        - manual_inflight
    )
    claimed: list[tuple[int, str]] = []
    if slots <= 0:
        return claimed

    active_manual_order_ids = select(ManualMoveRequest.order_id).where(
        ManualMoveRequest.status.in_(("queued", "running"))
    )
    manual = list(
        db.scalars(
            select(ManualMoveRequest)
            .join(Order, Order.id == ManualMoveRequest.order_id)
            .where(
                ManualMoveRequest.unit_id == unit.id,
                ManualMoveRequest.status == "queued",
                Order.archived_at.is_(None),
                Order.study_uid != "",
                Order.status.notin_(ACTIVE_ORDER_STATUSES),
            )
            .order_by(ManualMoveRequest.created_at, ManualMoveRequest.id)
            .limit(slots)
            .with_for_update(skip_locked=True)
        )
    )
    first = list(
        db.scalars(
            select(Order)
            .where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                Order.status == "wait_retrieve",
                Order.id.notin_(active_manual_order_ids),
                Order.retrieve_at.is_not(None),
                Order.retrieve_at <= now,
            )
            .order_by(Order.retrieve_at)
            .limit(slots)
            .with_for_update(skip_locked=True)
        )
    )
    second = list(
        db.scalars(
            select(Order)
            .where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                Order.status == "wait_second",
                Order.id.notin_(active_manual_order_ids),
                Order.second_retrieve_at.is_not(None),
                Order.second_retrieve_at <= now,
            )
            .order_by(Order.second_retrieve_at)
            .limit(slots)
            .with_for_update(skip_locked=True)
        )
    )
    prior = list(
        db.scalars(
            select(Order).where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                Order.prior_status.in_(("queued", "retry_wait")),
                Order.prior_due_at.is_not(None),
                Order.prior_due_at <= now,
            )
            .order_by(Order.prior_due_at, Order.id)
            .limit(slots)
            .with_for_update(skip_locked=True)
        )
    )
    candidates = (
        [
            (request.created_at, -1, request.id, "manual", request)
            for request in manual
        ]
        + [(order.retrieve_at, 1, order.id, "first", order) for order in first]
        + [(order.second_retrieve_at, 2, order.id, "second", order) for order in second]
        + [(order.prior_due_at, 0, order.id, "prior", order) for order in prior]
    )
    kind_priority = {"manual": 0, "first": 1, "second": 1, "prior": 2}
    candidates.sort(
        key=lambda item: (kind_priority[item[3]], item[0], item[1], item[2])
    )
    for _due_at, _priority, _resource_id, kind, resource in candidates[:slots]:
        if kind == "manual":
            resource.status = "running"
            resource.started_at = now
            claimed.append((resource.id, kind))
            continue
        if kind == "first":
            resource.status = "retrieving"
            resource.heartbeat_at = now
        elif kind == "second":
            resource.status = "retrieving_second"
            resource.heartbeat_at = now
        else:
            resource.prior_status = "retrieving"
            resource.prior_heartbeat_at = now
        claimed.append((resource.id, kind))
    if claimed:
        db.commit()
        log_event(
            log,
            logging.INFO,
            "dicom.move.claim",
            resource=f"unit:{unit.id}",
            status="success",
            unit_id=unit.id,
            claimed_count=len(claimed),
            move_kinds=[kind for _resource_id, kind in claimed],
        )
    return claimed


def run_claimed_move(db: Session, resource_id: int, kind: str) -> None:
    if kind == "manual":
        request = db.get(ManualMoveRequest, resource_id)
        if request is None or request.status != "running":
            return
        _run_manual_move(db, resource_id)
        return
    order = db.get(Order, resource_id)
    if order is None:
        return
    unit = db.get(Unit, order.unit_id)
    if unit is None:
        return
    if kind == "prior":
        if order.prior_status != "retrieving" or order.archived_at is not None:
            return
        _run_prior_move(db, unit, order)
    else:
        expected = "retrieving_second" if kind == "second" else "retrieving"
        if order.status != expected or order.archived_at is not None:
            return
        _run_move(db, unit, order, kind == "second")


def fail_claimed_move(
    db: Session,
    resource_id: int,
    kind: str,
    error: BaseException,
) -> None:
    """Persist a terminal/retry state after an unexpected executor failure."""
    now = datetime.now()
    detail = safe_error_detail(error)
    if kind == "manual":
        request = db.get(ManualMoveRequest, resource_id)
        if request is None or request.status != "running":
            return
        request.status = "error"
        request.completed_at = now
        request.last_error = f"Falha interna no C-MOVE: {detail}"[:500]
        order = db.get(Order, request.order_id)
        if order is not None:
            add_event(
                db,
                order,
                "C-MOVE manual interrompido por falha interna",
                detail,
                "error",
            )
        db.commit()
        return

    order = db.get(Order, resource_id)
    if order is None or order.archived_at is not None:
        return
    if kind == "prior" and order.prior_status == "retrieving":
        order.prior_heartbeat_at = now
        if order.prior_attempts < 3:
            delay_index = max(0, min(order.prior_attempts - 1, 1))
            delay = PRIOR_RETRY_DELAYS[delay_index]
            order.prior_status = "retry_wait"
            order.prior_due_at = now + timedelta(seconds=delay)
            order.prior_last_error = f"Falha interna: {detail}"
            add_event(
                db,
                order,
                "Retrieve histórico interrompido; nova tentativa agendada",
                detail,
                "warn",
            )
        else:
            order.prior_status = "error"
            order.prior_completed_at = now
            order.prior_last_error = f"Falha interna: {detail}"
            add_event(
                db,
                order,
                "Retrieve histórico interrompido após esgotar as tentativas",
                detail,
                "error",
            )
        db.commit()
        return

    expected = "retrieving_second" if kind == "second" else "retrieving"
    if order.status != expected:
        return
    _schedule_current_move_retry(
        db,
        order,
        second=kind == "second",
        now=now,
        error_message=f"Falha interna: {detail}",
        event_detail=detail,
    )
    db.commit()


def _schedule_current_move_retry(
    db: Session,
    order: Order,
    *,
    second: bool,
    now: datetime,
    error_message: str,
    event_detail: str,
) -> bool:
    """Schedule a bounded C-MOVE retry; return False when exhausted."""
    label = "2º" if second else "1º"
    order.heartbeat_at = now
    if order.attempts >= 3:
        order.status = "error"
        order.last_error = error_message
        add_event(
            db,
            order,
            f"{label} C-MOVE falhou após 3 tentativas",
            event_detail,
            "error",
        )
        return False
    delay = CURRENT_MOVE_RETRY_DELAYS[max(0, order.attempts - 1)]
    due_at = now + timedelta(seconds=delay)
    if second:
        order.status = "wait_second"
        order.second_retrieve_at = due_at
    else:
        order.status = "wait_retrieve"
        order.retrieve_at = due_at
    order.last_error = error_message
    add_event(
        db,
        order,
        f"{label} C-MOVE falhou; nova tentativa em {delay} segundos",
        event_detail,
        "warn",
    )
    return True


def _run_manual_move(db: Session, request_id: int) -> None:
    move_request = db.get(ManualMoveRequest, request_id)
    if move_request is None:
        return
    order = db.get(Order, move_request.order_id)
    unit = db.get(Unit, move_request.unit_id)
    if order is None or unit is None or order.archived_at or not order.study_uid:
        move_request.status = "error"
        move_request.completed_at = datetime.now()
        move_request.last_error = "Pedido indisponível para C-MOVE manual"
        db.commit()
        return

    started_at = perf_counter()
    with log_context(move_request.correlation_id):
        log_event(
            log,
            logging.INFO,
            "dicom.move.manual",
            resource=f"order:{order.id}",
            status="started",
            order_id=order.id,
            unit_id=unit.id,
            manual_move_request_id=move_request.id,
        )
        try:
            code, output = c_move(
                unit.calling_aet,
                unit.pacs_aet,
                unit.pacs_ip,
                unit.pacs_port,
                order.study_uid,
                unit.move_timeout_first,
            )
        except ToolMissing as exc:
            code, output = 127, str(exc)
        move_request.completed_at = datetime.now()
        safe_output = redact_dicom_output(output)
        if code == 0:
            move_request.status = "done"
            move_request.last_error = ""
            add_event(
                db,
                order,
                "C-MOVE manual do exame atual concluído",
                safe_output,
            )
            event_level = logging.INFO
            event_status = "success"
        else:
            move_request.status = "error"
            move_request.last_error = f"C-MOVE manual exit {code}"
            add_event(
                db,
                order,
                f"C-MOVE manual do exame atual falhou (exit {code})",
                safe_output,
                "error",
            )
            event_level = logging.ERROR
            event_status = "failure"
        db.commit()
        log_event(
            log,
            event_level,
            "dicom.move.manual",
            resource=f"order:{order.id}",
            status=event_status,
            started_at=started_at,
            order_id=order.id,
            unit_id=unit.id,
            manual_move_request_id=move_request.id,
            command_status=code,
        )


def _register_prior_studies(
    db: Session,
    unit: Unit,
    order: Order,
    series_results: tuple[PriorSeriesResult, ...],
) -> tuple[PriorSeriesResult, ...]:
    """Persist valid historical studies before requesting their images."""
    valid_series = tuple(
        result
        for result in series_results
        if result.study_uid != order.study_uid
        and result.study_date
        and order.prior_date_from <= result.study_date <= order.prior_date_to
    )
    studies: dict[str, PriorSeriesResult] = {}
    for result in valid_series:
        current = studies.get(result.study_uid)
        if current is None or (not current.body_part and result.body_part):
            studies[result.study_uid] = result

    for study_uid, result in studies.items():
        study = db.scalar(
            select(HistoricalStudy).where(
                HistoricalStudy.order_id == order.id,
                HistoricalStudy.study_uid == study_uid,
            )
        )
        if study is None:
            db.add(
                HistoricalStudy(
                    order_id=order.id,
                    unit_id=unit.id,
                    study_uid=_bounded_db_text(study_uid, 128),
                    accession=_bounded_db_text(result.accession, 64),
                    study_date=_bounded_db_text(result.study_date, 16),
                    modality=_bounded_db_text(result.modality, 32),
                    body_part=_bounded_db_text(result.body_part, 64),
                    description=_bounded_db_text(result.description, 255),
                )
            )
            continue
        study.accession = _bounded_db_text(result.accession, 64) or study.accession
        study.study_date = _bounded_db_text(result.study_date, 16) or study.study_date
        study.modality = _bounded_db_text(result.modality, 32) or study.modality
        study.body_part = _bounded_db_text(result.body_part, 64) or study.body_part
        study.description = (
            _bounded_db_text(result.description, 255) or study.description
        )
    db.flush()
    return valid_series


def _register_prior_series(
    db: Session,
    unit: Unit,
    order: Order,
    series_results: tuple[PriorSeriesResult, ...],
) -> list[HistoricalSeries]:
    """Create idempotent per-series checkpoints and return unfinished jobs."""
    unique_results = {result.series_uid: result for result in series_results}
    existing = {
        series.series_uid: series
        for series in db.scalars(
            select(HistoricalSeries).where(HistoricalSeries.order_id == order.id)
        )
    }
    for series_uid, result in unique_results.items():
        if series_uid in existing:
            continue
        series = HistoricalSeries(
            order_id=order.id,
            unit_id=unit.id,
            study_uid=_bounded_db_text(result.study_uid, 128),
            series_uid=_bounded_db_text(series_uid, 128),
        )
        db.add(series)
        existing[series_uid] = series
    db.flush()
    return [series for series in existing.values() if series.status != "done"]


def _append_diagnostic(parts: list[str], label: str, output: str) -> None:
    """Keep useful command tails without retaining unbounded PACS output in RAM."""
    parts.append(f"{label}:\n{output[-4000:]}")
    while len(parts) > 1 and sum(map(len, parts)) > 20_000:
        parts.pop(0)


def _run_prior_move(db: Session, unit: Unit, order: Order) -> None:
    correlation_id = _ensure_order_correlation(order)
    started_at = perf_counter()
    deadline = monotonic() + max(unit.move_timeout_prior, 1)
    now = datetime.now()
    order.prior_status = "retrieving"
    if order.prior_started_at is None:
        order.prior_started_at = now
    order.prior_heartbeat_at = now
    order.prior_attempts += 1
    db.commit()
    with log_context(correlation_id):
        log_event(
            log,
            logging.INFO,
            "dicom.move.prior",
            resource=f"order:{order.id}",
            status="started",
            order_id=order.id,
            unit_id=unit.id,
            attempt=order.prior_attempts,
        )
        operation = "C-FIND histórico"
        outputs: list[str] = []
        try:
            code, output = c_find_prior(
                unit.calling_aet,
                unit.pacs_aet,
                unit.pacs_ip,
                unit.pacs_port,
                order.body_part,
                order.modality,
                order.pat_id,
                order.birth_date,
                f"{order.prior_date_from}-{order.prior_date_to}",
                max(1, int(deadline - monotonic())),
            )
        except ToolMissing as exc:
            code, output = 127, str(exc)
        _append_diagnostic(outputs, "C-FIND histórico", output)
        discovered_studies = 0
        discovered_series = 0
        if code == 0:
            parsed_series = parse_prior_findscu_output(output)
            if "(Pending)" in output and not parsed_series:
                code = 69
                outputs.append(
                    "O PACS respondeu ao C-FIND sem StudyInstanceUID/SeriesInstanceUID."
                )
            else:
                valid_series = _register_prior_studies(
                    db, unit, order, parsed_series
                )
                discovered_series = len(valid_series)
                discovered_studies = len(
                    {result.study_uid for result in valid_series}
                )
                series_jobs = _register_prior_series(db, unit, order, valid_series)
                all_series = list(
                    db.scalars(
                        select(HistoricalSeries).where(
                            HistoricalSeries.order_id == order.id
                        )
                    )
                )
                discovered_series = len(all_series)
                discovered_studies = len(
                    {series.study_uid for series in all_series}
                )
                # Torna os UIDs e checkpoints visíveis enquanto os C-MOVEs executam.
                db.commit()
                operation = "C-MOVE histórico"
                for series_job in series_jobs:
                    remaining = int(deadline - monotonic())
                    if remaining <= 0:
                        code = 124
                        _append_diagnostic(
                            outputs, "C-MOVE histórico", "TIMEOUT global"
                        )
                        break
                    series_job.status = "running"
                    series_job.attempts += 1
                    series_job.last_error = ""
                    db.commit()
                    try:
                        move_code, move_output = c_move_prior_series(
                            unit.calling_aet,
                            unit.pacs_aet,
                            unit.pacs_ip,
                            unit.pacs_port,
                            series_job.study_uid,
                            series_job.series_uid,
                            remaining,
                        )
                    except ToolMissing as exc:
                        move_code, move_output = 127, str(exc)
                    except OSError as exc:
                        move_code, move_output = 126, str(exc)
                    _append_diagnostic(
                        outputs,
                        "C-MOVE histórico "
                        f"StudyUID={series_job.study_uid} "
                        f"SeriesUID={series_job.series_uid}",
                        move_output,
                    )
                    if move_code == 0:
                        series_job.status = "done"
                        series_job.completed_at = datetime.now()
                        series_job.last_error = ""
                    else:
                        series_job.status = "error"
                        series_job.last_error = f"C-MOVE exit {move_code}"
                    db.commit()
                    if move_code != 0 and code == 0:
                        code = move_code
        safe_output = redact_dicom_output("\n\n".join(outputs))
        finished_at = datetime.now()
        order.prior_heartbeat_at = finished_at
        if code == 0:
            order.prior_status = "done"
            order.prior_completed_at = finished_at
            order.prior_last_error = ""
            if discovered_series:
                message = (
                    "Retrieve histórico concluído: "
                    f"{discovered_studies} exame(s), {discovered_series} série(s)"
                )
            else:
                message = (
                    "Retrieve histórico concluído: nenhum exame anterior localizado"
                )
            add_event(db, order, message, safe_output)
            event_level = logging.INFO
            event_status = "success"
        elif order.prior_attempts < 3:
            delay = PRIOR_RETRY_DELAYS[order.prior_attempts - 1]
            order.prior_status = "retry_wait"
            order.prior_due_at = finished_at + timedelta(seconds=delay)
            order.prior_last_error = f"{operation} exit {code}"
            add_event(
                db,
                order,
                f"{operation} falhou (exit {code}); nova tentativa agendada",
                safe_output,
                "warn",
            )
            event_level = logging.WARNING
            event_status = "retry"
        else:
            order.prior_status = "error"
            order.prior_completed_at = finished_at
            order.prior_last_error = f"{operation} exit {code}"
            add_event(
                db,
                order,
                f"{operation} falhou após 3 tentativas (exit {code})",
                safe_output,
                "error",
            )
            event_level = logging.ERROR
            event_status = "failure"
        db.commit()
        log_event(
            log,
            event_level,
            "dicom.move.prior",
            resource=f"order:{order.id}",
            status=event_status,
            started_at=started_at,
            order_id=order.id,
            unit_id=unit.id,
            attempt=order.prior_attempts,
            command_status=code,
        )


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
            attempt=order.attempts,
        )
        try:
            code, output = c_move(
                unit.calling_aet,
                unit.pacs_aet,
                unit.pacs_ip,
                unit.pacs_port,
                order.study_uid,
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
        except OSError as exc:
            code, output = 126, str(exc)
        safe_output = redact_dicom_output(output)
        if code == 0:
            if second or order.second_retrieve_at is None:
                order.status = "done"
                order.done_at = datetime.now()
                add_event(db, order, f"{label} C-MOVE concluído", safe_output)
            else:
                order.status = "wait_second"
                # As tentativas são limitadas separadamente para cada retrieve.
                order.attempts = 0
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
            retrying = _schedule_current_move_retry(
                db,
                order,
                second=second,
                now=datetime.now(),
                error_message=f"C-MOVE exit {code}",
                event_detail=safe_output,
            )
            event_level = logging.WARNING if retrying else logging.ERROR
            event_status = "retry" if retrying else "failure"
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


def _settled_files(
    directory: Path,
    settle_seconds: int,
    *,
    timestamp: float | None = None,
    limit: int | None = None,
) -> tuple[list[Path], list[tuple[Path, OSError]]]:
    """Return settled visible files without materializing the entire directory."""
    if not directory.is_dir():
        return [], []
    current_timestamp = (
        timestamp if timestamp is not None else datetime.now().timestamp()
    )
    paths: list[Path] = []
    errors: list[tuple[Path, OSError]] = []
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.name.startswith("."):
                    continue
                path = Path(entry.path)
                try:
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    if (
                        current_timestamp
                        - entry.stat(follow_symlinks=False).st_mtime
                        < settle_seconds
                    ):
                        continue
                    paths.append(path)
                    if limit is not None and len(paths) >= limit:
                        break
                except OSError as exc:
                    errors.append((path, exc))
    except OSError as exc:
        errors.append((directory, exc))
    return paths, errors


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


def compact_unit(db: Session, unit: Unit) -> bool:
    """Process one bounded batch and report whether more work may be available."""
    origin = Path(unit.receive_dir)
    dest_dir = Path(unit.send_dir)
    error_dir = Path(unit.error_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    error_dir.mkdir(parents=True, exist_ok=True)
    drops, compress_map = compression_runtime_settings(db, unit.id)
    dicom_rules = load_rule_specs(db, unit.id)
    settle = unit.file_settle_seconds
    jobs, inspection_errors = _settled_files(
        origin,
        settle,
        limit=COMPACT_BATCH_SIZE,
    )
    for _path, exc in inspection_errors:
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
        return False
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
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {
            pool.submit(
                _compact_one_limited,
                str(p),
                str(dest_dir / (p.name + ".dcm")),
                str(error_dir / p.name),
                unit.token,
                drops,
                compress_map,
                dicom_rules,
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
            for persist_attempt in range(1, 4):
                try:
                    _record_compact_result(db, unit, result)
                    db.commit()
                    processed_count += 1
                    if result.error_type:
                        failed_count += 1
                    break
                except SQLAlchemyError as exc:
                    db.rollback()
                    exhausted = persist_attempt == 3
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
                        failed_count += 1
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
        )
    return len(jobs) >= COMPACT_BATCH_SIZE


def _filename_mod(name: str) -> str:
    return name[:2].upper() if len(name) >= 2 else ""


def _compact_one(
    filepath: str,
    dest: str,
    error: str,
    token: str,
    drops: set[str],
    compress_map: dict[str, str],
    dicom_rules: tuple[RuleSpec, ...],
) -> CompactResult:
    name = os.path.basename(filepath)
    prefix = _filename_mod(name)
    try:
        image = pydicom.dcmread(filepath, defer_size="1 MB")
    except Exception as exc:
        _quarantine_failed_source(Path(filepath), Path(error).parent)
        return CompactResult(name, name, "", "compression_error", type(exc).__name__)

    modality = str(getattr(image, "Modality", "") or prefix).upper()
    study_uid = str(getattr(image, "StudyInstanceUID", "") or "")
    identity = {
        "patient_id": str(getattr(image, "PatientID", "") or ""),
        "birth_date": str(getattr(image, "PatientBirthDate", "") or ""),
        "study_date": str(getattr(image, "StudyDate", "") or ""),
        "accession": str(getattr(image, "AccessionNumber", "") or ""),
        "modality": modality,
        "body_part": str(getattr(image, "BodyPartExamined", "") or ""),
        "description": str(getattr(image, "StudyDescription", "") or ""),
        "observed_at": datetime.fromtimestamp(Path(filepath).stat().st_mtime),
    }
    # Add the system-managed values before evaluating rules so a configured
    # replacement/removal remains the final value written to the DICOM file.
    image.SpecificCharacterSet = "ISO_IR 100"
    image.InstitutionalDepartmentName = token
    try:
        rule_result = apply_rule_specs(image, dicom_rules)
    except RuleExecutionError as exc:
        _quarantine_failed_source(Path(filepath), Path(error).parent)
        return CompactResult(
            name,
            name,
            study_uid,
            "rule_error",
            type(exc).__name__,
            **identity,
            rule_matches=exc.matches,
        )
    if rule_result.delete_image:
        os.remove(filepath)
        return CompactResult(
            name,
            "",
            study_uid,
            "discarded_rule",
            **identity,
            rule_matches=rule_result.matches,
        )
    processing_modality = str(getattr(image, "Modality", "") or prefix).upper()
    if prefix in drops or processing_modality in drops:
        os.remove(filepath)
        return CompactResult(
            name,
            "",
            study_uid,
            "discarded_modality",
            **identity,
            rule_matches=rule_result.matches,
        )

    flag = compress_map.get(processing_modality, compress_map.get("*", "+e1"))
    dest_path = Path(dest)
    temp_dest = dest_path.with_name(
        f".{dest_path.name}.{new_correlation_id()[:8]}.tmp"
    )
    promoted = False
    try:
        image.save_as(filepath)
        code, _out = dcmcjpeg(flag, filepath, str(temp_dest))
        if code != 0:
            temp_dest.unlink(missing_ok=True)
            _quarantine_failed_source(Path(filepath), Path(error).parent)
            return CompactResult(
                name,
                name,
                study_uid,
                "compression_error",
                "DcmcjpegError",
                **identity,
                rule_matches=rule_result.matches,
            )
        os.replace(temp_dest, dest_path)
        promoted = True
        os.remove(filepath)
    except Exception as exc:
        temp_dest.unlink(missing_ok=True)
        # If promotion succeeded but source cleanup failed, discard the output;
        # otherwise the sender could upload an image lacking its DB association.
        if promoted:
            dest_path.unlink(missing_ok=True)
        _quarantine_failed_source(Path(filepath), Path(error).parent)
        return CompactResult(
            name,
            name,
            study_uid,
            "compression_error",
            type(exc).__name__,
            **identity,
            rule_matches=rule_result.matches,
        )
    return CompactResult(
        name,
        dest_path.name,
        study_uid,
        "compressed",
        **identity,
        rule_matches=rule_result.matches,
    )


def _record_compact_result(db: Session, unit: Unit, result: CompactResult) -> None:
    # An exact active current-study match always wins. Completed/archived orders
    # are considered only after the historical matcher, otherwise a study that
    # is currently being retrieved as history could be attached to an old main
    # order instead of the active historical flow.
    order = None
    if result.study_uid:
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
            filename=_bounded_db_text(
                result.output_name or result.source_name, 500
            ),
            correlation_id=correlation_id,
            study_uid=_bounded_db_text(result.study_uid, 128),
        )
        db.add(transfer)
        db.flush()
    else:
        transfer.order_id = order.id if order else transfer.order_id
        transfer.correlation_id = correlation_id
        transfer.study_uid = (
            _bounded_db_text(result.study_uid, 128) or transfer.study_uid
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
            transfer.correlation_id = _ensure_order_correlation(order)
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
                transfer.correlation_id = _ensure_order_correlation(order)

    transfer.status = "metadata_error" if rejection_type else result.status
    transfer.last_error = _bounded_db_text(
        rejection_detail or result.error_type,
        500,
    )
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
                accession=_bounded_db_text(result.accession, 64),
                study_uid=_bounded_db_text(result.study_uid, 128),
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


def _missing_inbound_fields(result: CompactResult) -> list[str]:
    return [
        dicom_name
        for attribute, dicom_name in _REQUIRED_INBOUND_FIELDS.items()
        if not _bounded_db_text(getattr(result, attribute), 128)
    ]


def _enrich_order_from_received(order: Order, result: CompactResult) -> None:
    """Fill information missing from an order as later series arrive."""
    order.pat_id = order.pat_id or _bounded_db_text(result.patient_id, 64)
    order.birth_date = order.birth_date or _bounded_db_text(result.birth_date, 16)
    order.exam_date = order.exam_date or _bounded_db_text(result.study_date, 16)
    order.modality = order.modality or _bounded_db_text(result.modality, 32)
    order.body_part = order.body_part or _bounded_db_text(result.body_part, 64)


def _quarantine_compacted_output(unit: Unit, result: CompactResult) -> None:
    """Keep an invalid inbound object out of the cloud upload queue."""
    filename = result.output_name or result.source_name
    if not filename:
        return
    source = Path(unit.send_dir) / filename
    if not source.is_file():
        # Compression/rule failures are already moved to the error directory.
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

    study_uid = _bounded_db_text(result.study_uid, 128)
    accession = _bounded_db_text(result.accession, 64)
    bind = db.get_bind()
    if bind.dialect.name == "postgresql":
        # Results from the compression pool are persisted serially today, but
        # this database lock keeps the invariant if more workers are deployed.
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
    modality, _ignored_first_at, second_at = schedule_from_now(db, result.modality)
    if order is None:
        correlation_id = new_correlation_id()
        order = Order(
            unit_id=unit.id,
            source_id=_bounded_db_text(
                f"storescp:{unit.id}:{accession}",
                64,
            ),
            filename=accession,
            pat_id=_bounded_db_text(result.patient_id, 64),
            acc=accession,
            birth_date=_bounded_db_text(result.birth_date, 16),
            exam_date=_bounded_db_text(result.study_date, 16),
            correlation_id=correlation_id,
            api_read_status="confirmed",
            api_read_at=now,
            status="wait_second" if second_at else "done",
            study_uid=study_uid,
            modality=_bounded_db_text(modality, 32),
            body_part=_bounded_db_text(result.body_part, 64),
            prior_status="disabled",
            retrieve_at=None,
            second_retrieve_at=second_at,
            found_at=now,
            done_at=None if second_at else now,
        )
        db.add(order)
        db.flush()
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
        if second_at:
            message = (
                "Exame recebido diretamente pelo Store SCP; "
                f"2º retrieve agendado para {second_at:%d/%m/%Y às %H:%M}"
            )
        else:
            message = (
                "Exame recebido diretamente pelo Store SCP; modalidade sem "
                "2º retrieve configurado"
            )
        add_event(db, order, message)
        action = "created"
    else:
        was_archived = order.archived_at is not None
        was_watching = order.status == "watching"
        order.study_uid = order.study_uid or study_uid
        _enrich_order_from_received(order, result)
        order.modality = order.modality or _bounded_db_text(modality, 32)
        order.found_at = order.found_at or now
        _ensure_order_correlation(order)

        # A direct delivery can satisfy a queued order that had not yet found
        # the study. An archived order is restored as requested. A completed,
        # non-archived order is merely reused, so every image in the same batch
        # cannot repeatedly schedule a new second retrieve.
        if order.status == "watching" or was_archived:
            order.status = "wait_second" if second_at else "done"
            order.retrieve_at = None
            order.second_retrieve_at = second_at
            order.attempts = 0
            order.last_error = ""
            order.heartbeat_at = None
            order.done_at = None if second_at else now
        if was_watching and unit.retrieve_prior_enabled:
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
        elif order.status in ("wait_second", "done") and matched_by == "accession":
            add_event(
                db,
                order,
                "Exame recebido diretamente e associado ao pedido pelo accession",
            )
            if was_watching and unit.retrieve_prior_enabled:
                add_event(
                    db,
                    order,
                    "Retrieve histórico mantido e enfileirado para execução imediata",
                )
            action = "matched"
        else:
            action = "reused"

    with log_context(_ensure_order_correlation(order)):
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
            accession=order.acc,
            study_uid=order.study_uid,
            modality=order.modality,
            second_retrieve_at=(
                order.second_retrieve_at.isoformat()
                if order.second_retrieve_at
                else None
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

    candidates = list(
        db.scalars(
            select(Order).where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                Order.prior_status != "disabled",
                Order.prior_started_at.is_not(None),
                Order.prior_started_at <= result.observed_at,
            )
        )
    )
    matched_order = None
    for order in candidates:
        if result.study_uid == order.study_uid:
            continue
        if order.prior_completed_at and result.observed_at > (
            order.prior_completed_at + timedelta(minutes=1)
        ):
            continue
        if not order.pat_id or not result.patient_id.startswith(order.pat_id):
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
                study_uid=_bounded_db_text(result.study_uid, 128),
                accession=_bounded_db_text(result.accession, 64),
                study_date=_bounded_db_text(result.study_date, 16),
                modality=_bounded_db_text(result.modality, 32),
                body_part=_bounded_db_text(result.body_part, 64),
                description=_bounded_db_text(result.description, 255),
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
    study.accession = study.accession or _bounded_db_text(result.accession, 64)
    study.study_date = study.study_date or _bounded_db_text(result.study_date, 16)
    study.modality = study.modality or _bounded_db_text(result.modality, 32)
    study.body_part = study.body_part or _bounded_db_text(result.body_part, 64)
    study.description = study.description or _bounded_db_text(
        result.description, 255
    )
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


def send_unit(db: Session, unit: Unit, cloud_url: str, settle: int) -> None:
    origin = Path(unit.send_dir)
    if not cloud_url:
        return
    # Uma indisponibilidade em uma unidade não deve abrir o circuito das demais,
    # mesmo quando elas usam o mesmo endpoint de nuvem.
    circuit = _cloud_circuits.setdefault(unit.id, CircuitState())
    current_time = datetime.now()
    if circuit.open_until and current_time < circuit.open_until:
        return

    settled, inspection_errors = _settled_files(
        origin, settle, timestamp=current_time.timestamp()
    )
    for path, exc in inspection_errors:
        log_event(
            log,
            logging.ERROR,
            "cloud.file.inspect",
            resource=f"unit:{unit.id}",
            status="failure",
            error=exc,
            error_detail=safe_error_detail(exc),
            unit_id=unit.id,
            path_name=path.name,
        )
    paths = {path.name: path for path in settled}
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
    # Confirmações remotas já persistidas vencem sobre sobras locais. Isso cobre
    # falha ao apagar o arquivo depois do commit sem provocar reenvio duplicado.
    for filename, transfer in list(existing.items()):
        if transfer.status != "uploaded":
            continue
        try:
            paths[filename].unlink(missing_ok=True)
            paths.pop(filename, None)
        except OSError as exc:
            log_event(
                log,
                logging.WARNING,
                "cloud.upload.cleanup",
                resource=f"transfer:{transfer.id}",
                status="retry",
                error=exc,
                error_detail=safe_error_detail(exc),
                transfer_id=transfer.id,
                unit_id=unit.id,
            )
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
    started_at = perf_counter()
    log_event(
        log,
        logging.INFO,
        "cloud.upload.batch",
        resource=f"unit:{unit.id}",
        status="started",
        unit_id=unit.id,
        file_count=len(jobs),
    )
    results = asyncio.run(_send_all(jobs, cloud_url, workers))
    _record_send_results(db, unit, results, circuit)
    failure_count = sum(not result.success for result in results)
    log_event(
        log,
        logging.WARNING if failure_count else logging.INFO,
        "cloud.upload.batch",
        resource=f"unit:{unit.id}",
        status="partial" if failure_count else "success",
        started_at=started_at,
        unit_id=unit.id,
        file_count=len(results),
        success_count=len(results) - failure_count,
        failure_count=failure_count,
    )


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
                logging.DEBUG,
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
    successful_filenames: list[tuple[int, str]] = []
    for result in results:
        persisted = False
        for persist_attempt in range(1, 4):
            try:
                transfer = db.get(ImageTransfer, result.transfer_id)
                if transfer is None:
                    persisted = True
                    break
                transfer.attempts += 1
                transfer.last_http_status = result.http_status
                transfer.last_error = _bounded_db_text(result.error_type, 500)
                if result.success:
                    transfer.status = "uploaded"
                    transfer.next_attempt_at = None
                    level = logging.DEBUG
                    status = "success"
                else:
                    transfer.status = "upload_error"
                    delay = min(
                        SEND_RETRY_MAX_SECONDS,
                        SEND_RETRY_BASE_SECONDS
                        * (2 ** min(transfer.attempts - 1, 10)),
                    )
                    transfer.next_attempt_at = now + timedelta(seconds=delay)
                    level = logging.WARNING
                    status = "retry"
                db.commit()
                persisted = True
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
                if result.success:
                    successful_filenames.append((transfer.id, transfer.filename))
                break
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
                    error_detail=safe_error_detail(exc),
                    transfer_id=result.transfer_id,
                    unit_id=unit.id,
                    attempt=persist_attempt,
                )
        if not persisted or not result.success:
            failures += 1

    if failures == len(results):
        # Conte falhas consecutivas de lote, não cada imagem do mesmo incidente.
        circuit.failures += 1
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
                error_detail=safe_error_detail(exc),
                transfer_id=transfer_id,
                unit_id=unit.id,
            )


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
        "receive": count(unit.receive_dir),
        "send": count(unit.send_dir),
        "error": count(unit.error_dir),
    }
