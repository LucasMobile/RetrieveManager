"""PLERES orders API: ingestion, acknowledgements, retention and locks."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from datetime import datetime, timedelta
from time import monotonic, perf_counter

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.audit import audit
from app.config import (
    ORDERS_API_ACK_BATCH_SIZE,
    ORDERS_API_ACK_CONCURRENCY,
    ORDERS_API_POLL_SECONDS,
)
from app.events import add_event
from app.models import (
    ManualMoveRequest,
    Order,
    Unit,
)
from app.observability import (
    log_context,
    log_event,
    new_correlation_id,
)
from app.order_state import ACTIVE_ORDER_STATUSES
from app.orders_api import (
    InvalidApiOrder,
    OrdersApiError,
    fetch_orders,
    iter_acknowledgements,
    parse_api_order,
)
from app.pipeline.common import ensure_order_correlation, log
from app.retention import archive_order
from app.validation import validate_orders_api_company_id

STALE_LOCK = timedelta(minutes=20)


UNMATCHED_ORDER_RETENTION = timedelta(days=1)


UNMATCHED_ORDER_CLEANUP_BATCH = 500


COMPLETED_ORDER_RETENTION = timedelta(weeks=2)


COMPLETED_ORDER_CLEANUP_BATCH = 500


_last_orders_api_poll: dict[int, float] = {}


def recover_stale_locks(db: Session) -> None:
    now = datetime.now()
    cutoff = now - STALE_LOCK
    candidates = list(
        db.execute(
            select(Order, Unit.move_timeout_first, Unit.move_timeout_update)
            .join(Unit, Unit.id == Order.unit_id)
            .where(
                Order.archived_at.is_(None),
                Order.status.in_(ACTIVE_ORDER_STATUSES),
                (Order.heartbeat_at.is_(None)) | (Order.heartbeat_at < cutoff),
            )
        )
    )
    rows = [
        order
        for order, first_timeout, update_timeout in candidates
        if order.heartbeat_at is None
        or order.heartbeat_at
        < now
        - max(
            STALE_LOCK,
            timedelta(
                seconds=max(
                    update_timeout
                    if order.status == "retrieving_update"
                    else first_timeout,
                    60,
                )
                + 120
            ),
        )
    ]
    for order in rows:
        correlation_id = ensure_order_correlation(order)
        if order.status == "retrieving_update":
            # Check again: the counts tell what the interrupted move missed.
            order.status = "monitoring"
            order.monitor_next_at = now
            order.monitor_pending_series = ""
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
        if not order.unit.retrieve_prior_enabled:
            # Turned off meanwhile: do not queue it again.
            order.prior_status = "disabled"
            order.prior_due_at = None
            order.prior_last_error = ""
            add_event(
                db,
                order,
                "Retrieve histórico interrompido pela queda do worker e não "
                "retomado: desativado na unidade",
                level="warn",
            )
            continue
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
        or request.started_at < now - timedelta(seconds=max(int(timeout), 60) + 120)
    ]
    for request in manual_rows:
        request.status = "queued"
        request.started_at = None
        request.last_error = "lock órfão do C-MOVE manual recuperado"
    if manual_rows:
        db.commit()


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
    get_started_at = perf_counter()
    # The GET snapshot predates any PUT that confirms during the request.
    get_requested_at = datetime.now()
    try:
        # db.get(Unit) abriu uma transação; não retenha a conexão durante
        # um GET que pode consumir todo o timeout e suas tentativas.
        db.commit()
        payload = asyncio.run(fetch_orders(unit.orders_api_url, unit.orders_api_token))
        log_event(
            log,
            logging.INFO,
            "orders.api.get",
            resource=f"unit:{unit.id}",
            status="success",
            unit_id=unit.id,
            record_count=len(payload),
            started_at=get_started_at,
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
            started_at=get_started_at,
        )
        return 0

    created = 0
    counts = dict.fromkeys(
        (
            "already_read",
            "other_station",
            "invalid",
            "duplicate_payload",
            "existing",
            "existing_archived",
            "confirmed_during_get",
            "persist_failed",
        ),
        0,
    )
    seen: set[str] = set()
    station_id = (unit.orders_api_station_id or "").strip()
    # One read for every order the payload names, instead of one per item: a
    # GET usually repeats many orders already recorded and not yet confirmed.
    existing = _existing_orders(db, unit.id, payload)
    for item in payload:
        if not isinstance(item, dict):
            counts["invalid"] += 1
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
            counts["already_read"] += 1
            continue
        if station_id and str(item.get("idPosto") or "").strip() != station_id:
            counts["other_station"] += 1
            continue
        try:
            parsed = parse_api_order(item)
        except InvalidApiOrder as exc:
            counts["invalid"] += 1
            log_event(
                log,
                logging.WARNING,
                "order.api.ingest",
                resource=f"unit:{unit.id}",
                status="rejected",
                error=exc,
                error_type="InvalidApiOrder",
                error_detail=str(exc),
                unit_id=unit.id,
            )
            continue
        if parsed.accession_number in seen:
            counts["duplicate_payload"] += 1
            continue
        seen.add(parsed.accession_number)

        exists = existing.get(parsed.accession_number)
        if exists is not None:
            if (
                exists.api_read_status == "confirmed"
                and exists.api_read_at is not None
                and exists.api_read_at >= get_requested_at
            ):
                # Stale snapshot: the PUT landed while this GET was in flight.
                # A second PUT would make the PLERES upsert create a stray
                # document without the order's attachments.
                counts["confirmed_during_get"] += 1
                continue
            counts["existing_archived" if exists.archived_at else "existing"] += 1
            # A API ainda informou mirthReaded != true; ACK idempotente.
            exists.api_read_status = "pending"
            exists.api_read_last_error = ""
            continue
        correlation_id = new_correlation_id()
        try:
            with db.begin_nested():
                order = Order(
                    unit_id=unit.id,
                    source_id=parsed.source_id,
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
                audit(
                    db,
                    None,
                    None,
                    action="create",
                    resource_type="order",
                    resource_id=order.id,
                    resource_name=order.acc,
                    summary=(
                        f"Pedido recebido automaticamente da unidade {unit.name}."
                    ),
                )
                add_event(
                    db,
                    order,
                    f"Pedido recebido da API PLERES: acc={parsed.accession_number}",
                )
        except SQLAlchemyError as exc:
            counts["persist_failed"] += 1
            log_event(
                log,
                logging.ERROR,
                "order.api.ingest.persist",
                resource=f"unit:{unit.id}",
                status="rejected",
                error=exc,
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
    log_event(
        log,
        logging.INFO,
        "orders.api.ingest.summary",
        resource=f"unit:{unit.id}",
        status="success",
        unit_id=unit.id,
        station_id=station_id,
        record_count=len(payload),
        created_count=created,
        **{f"{key}_count": value for key, value in counts.items()},
    )
    return created


def _existing_orders(db: Session, unit_id: int, payload: list) -> dict[str, Order]:
    """The unit's orders named by the payload's accession numbers."""
    accessions: set[str] = set()
    for item in payload:
        if isinstance(item, dict) and item.get("mirthReaded") is not True:
            # Parsed as the loop does, so the keys match its accession numbers.
            with suppress(InvalidApiOrder):
                accessions.add(parse_api_order(item).accession_number)
    if not accessions:
        return {}
    return {
        order.acc: order
        for order in db.scalars(
            select(Order).where(
                Order.unit_id == unit_id, Order.acc.in_(sorted(accessions))
            )
        )
    }


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
    try:
        company_id = validate_orders_api_company_id(unit.orders_api_company_id)
    except ValueError as exc:
        message = str(exc)
        for order in pending:
            order.api_read_last_error = message
        db.commit()
        log_event(
            log,
            logging.ERROR,
            "orders.api.configuration",
            resource=f"unit:{unit.id}",
            status="blocked",
            error_type="InvalidCompanyId",
            error_detail=message,
            unit_id=unit.id,
        )
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
    # SessionLocal uses expire_on_commit=False: all transport inputs above stay
    # materialized, and the pool connection is returned before the first PUT.
    db.commit()

    async def acknowledge_batch() -> None:
        nonlocal success_count, failure_count
        async for result in iter_acknowledgements(
            unit.orders_api_url,
            unit.orders_api_token,
            list(by_accession),
            ORDERS_API_ACK_CONCURRENCY,
            company_id=company_id,
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
            with log_context(ensure_order_correlation(order)):
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
                Order.heartbeat_at.is_(None),
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
        audit(
            db,
            None,
            None,
            action="archive",
            resource_type="order",
            resource_id=order.id,
            resource_name=order.acc,
            summary=audit_summary,
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
