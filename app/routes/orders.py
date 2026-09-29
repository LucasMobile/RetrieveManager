"""Order queue, history, detail and order actions."""

from __future__ import annotations

import logging
from datetime import datetime
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import case, delete, func, or_, select
from sqlalchemy.orm import Session, selectinload
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.db import get_db
from app.models import (
    STATUSES,
    HistoricalImageLink,
    HistoricalSeries,
    HistoricalStudy,
    ImageTransfer,
    ManualMoveRequest,
    Order,
    OrderEvent,
    Unit,
    User,
)
from app.observability import log_event, new_correlation_id
from app.order_state import (
    ACTIVE_ORDER_STATUSES,
    ACTIVE_PRIOR_STATUSES,
    can_archive,
    can_cancel,
    can_reprocess,
)
from app.pager import cursor_page_links, paginate, query_keep
from app.pipeline import resend_failed_transfers
from app.retention import archive_order
from app.rules import schedule_from_now
from app.web import (
    DEFAULT_PAGE_SIZE,
    EVENTS_PAGE,
    PAGE_SIZE_OPTIONS,
    PRIOR_STATUS_LABELS,
    audit,
    badge_for,
    ctx,
    cursor_page_cursors,
    flash,
    log,
    require_admin,
    require_user,
    templates,
)

router = APIRouter()


@router.post("/orders/{order_id}/resend-failed")
def order_resend_failed(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    order = db.get(Order, order_id)
    if order is None:
        flash(request, "Pedido não encontrado.", "err")
        return RedirectResponse("/orders", status_code=303)
    if order.archived_at is not None:
        flash(request, "Pedidos arquivados são somente para consulta.", "err")
        return RedirectResponse(f"/orders/{order_id}", status_code=303)
    count = resend_failed_transfers(db, ImageTransfer.order_id == order.id)
    audit(
        db,
        request,
        user,
        action="resend",
        resource_type="order",
        resource_id=order.id,
        resource_name=order.acc,
        summary=f"{count} imagem(ns) com falha de envio devolvidas à fila.",
    )
    db.commit()
    flash(request, f"{count} imagem(ns) devolvidas à fila de envio.")
    return RedirectResponse(f"/orders/{order_id}", status_code=303)


def _order_send_failures(db: Session, order_id: int) -> int:
    return int(
        db.scalar(
            select(func.count()).where(
                ImageTransfer.order_id == order_id,
                ImageTransfer.status == "send_error",
            )
        )
        or 0
    )


@router.get("/orders", response_class=HTMLResponse)
def orders_list(
    request: Request,
    unit_id: str = Query("", max_length=20),
    status: str = Query("", max_length=32),
    q: str = Query("", max_length=200),
    before: int | None = Query(None, ge=1),
    after: int | None = Query(None, ge=1),
    last: bool = False,
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE),
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    parsed_unit_id = _parse_optional_unit_id(unit_id)
    return _orders_response(
        request,
        db,
        unit_id=parsed_unit_id,
        status=status,
        q=q,
        before=before,
        after=after,
        last=last,
        page=page,
        page_size=page_size,
        history=False,
    )


@router.get("/orders/history", response_class=HTMLResponse)
def orders_history(
    request: Request,
    unit_id: str = Query("", max_length=20),
    status: str = Query("", max_length=32),
    q: str = Query("", max_length=200),
    before: int | None = Query(None, ge=1),
    after: int | None = Query(None, ge=1),
    last: bool = False,
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    parsed_unit_id = _parse_optional_unit_id(unit_id)
    return _orders_response(
        request,
        db,
        unit_id=parsed_unit_id,
        status=status,
        q=q,
        before=before,
        after=after,
        last=last,
        page=page,
        page_size=page_size,
        history=True,
    )


def _parse_optional_unit_id(value: str) -> int | None:
    """Convert the unit filter while treating the form's empty option as unset."""
    if value == "":
        return None
    if not value.isdecimal():
        raise StarletteHTTPException(422, "Unidade inválida")
    unit_id = int(value)
    if unit_id < 1:
        raise StarletteHTTPException(422, "Unidade inválida")
    return unit_id


def _orders_response(
    request: Request,
    db: Session,
    *,
    unit_id: int | None,
    status: str,
    q: str,
    before: int | None,
    after: int | None,
    last: bool,
    page: int,
    page_size: int,
    history: bool,
):
    if status and status not in STATUSES:
        raise StarletteHTTPException(422, "Status inválido")
    if page_size not in PAGE_SIZE_OPTIONS:
        raise StarletteHTTPException(422, "Quantidade de itens por página inválida")
    if sum((before is not None, after is not None, last)) > 1:
        raise StarletteHTTPException(422, "Use apenas um cursor de paginação")
    if page > 1 and before is None and after is None and not last:
        raise StarletteHTTPException(422, "Cursor de paginação ausente")
    archived_filter = (
        Order.archived_at.is_not(None) if history else Order.archived_at.is_(None)
    )
    filt = select(Order).where(archived_filter)
    if unit_id:
        filt = filt.where(Order.unit_id == unit_id)
    if status:
        filt = filt.where(Order.status == status)
    if q:
        like = f"%{q.strip()}%"
        filt = filt.where(
            or_(
                Order.acc.ilike(like),
                Order.pat_id.ilike(like),
                Order.source_id.ilike(like),
            )
        )
    total = db.scalar(select(func.count()).select_from(filt.subquery())) or 0
    filtered_orders = filt.subquery()
    order_status_counts = db.execute(
        select(
            func.sum(
                case(
                    (
                        filtered_orders.c.status.in_(
                            ("watching", "wait_retrieve", "wait_second")
                        )
                        | filtered_orders.c.prior_status.in_(("queued", "retry_wait")),
                        1,
                    ),
                    else_=0,
                )
            ),
            func.sum(
                case(
                    (
                        filtered_orders.c.status.in_(ACTIVE_ORDER_STATUSES)
                        | (filtered_orders.c.prior_status == "retrieving"),
                        1,
                    ),
                    else_=0,
                )
            ),
            func.sum(
                case(
                    (
                        (filtered_orders.c.status == "done")
                        & ~filtered_orders.c.prior_status.in_(ACTIVE_PRIOR_STATUSES),
                        1,
                    ),
                    else_=0,
                )
            ),
        ).select_from(filtered_orders)
    ).one()
    order_summary = {
        "total": total,
        "waiting": int(order_status_counts[0] or 0),
        "running": int(order_status_counts[1] or 0),
        "done": int(order_status_counts[2] or 0),
    }
    pages = max(1, (total + page_size - 1) // page_size)
    if last:
        page = pages
    elif before is None and after is None:
        page = 1
    pager = paginate(total, page, page_size)
    pager["size_options"] = PAGE_SIZE_OPTIONS
    pager["keep"] = {"unit_id": unit_id, "status": status, "q": q}
    stmt = filt.options(selectinload(Order.unit))
    if last:
        stmt = stmt.order_by(Order.id.asc())
    elif before is not None:
        stmt = stmt.where(Order.id < before).order_by(Order.id.desc())
    elif after is not None:
        stmt = stmt.where(Order.id > after).order_by(Order.id.asc())
    else:
        stmt = stmt.order_by(Order.id.desc())
    result_limit = (total - pager["offset"]) if last else pager["size"]
    stmt = stmt.limit(result_limit)
    rows = list(db.scalars(stmt))
    if after is not None or last:
        rows.reverse()
    for o in rows:
        o.status_label = STATUSES.get(o.status, o.status)  # type: ignore[attr-defined]
        o.badge = badge_for(o.status)  # type: ignore[attr-defined]
        o.can_reprocess = can_reprocess(o)  # type: ignore[attr-defined]
        o.can_archive = can_archive(o)  # type: ignore[attr-defined]
        o.prior_status_label = PRIOR_STATUS_LABELS.get(  # type: ignore[attr-defined]
            o.prior_status, o.prior_status
        )
    has_prev = False
    has_next = False
    if rows:
        has_prev = (
            db.scalar(
                filt.where(Order.id > rows[0].id).with_only_columns(Order.id).limit(1)
            )
            is not None
        )
        has_next = (
            db.scalar(
                filt.where(Order.id < rows[-1].id).with_only_columns(Order.id).limit(1)
            )
            is not None
        )
    pager.update(
        cursor=True,
        has_prev=has_prev,
        has_next=has_next,
        prev_cursor=rows[0].id if rows else None,
        next_cursor=rows[-1].id if rows else None,
    )

    pager["page_links"] = cursor_page_links(
        pager,
        page_cursors=cursor_page_cursors(db, filt, Order.id, pager),
    )
    units = list(db.scalars(select(Unit).order_by(Unit.name)))
    qs = query_keep(unit_id=unit_id, status=status, q=q, page_size=page_size)
    return_to = request.url.path
    if request.url.query:
        return_to = f"{return_to}?{request.url.query}"
    return templates.TemplateResponse(
        request=request,
        name="orders.html",
        context=ctx(
            request,
            db,
            "order_history" if history else "orders",
            orders=rows,
            units=units,
            statuses=STATUSES,
            unit_id=unit_id,
            status=status,
            q=q,
            pager=pager,
            order_summary=order_summary,
            qs=qs,
            history=history,
            orders_path="/orders/history" if history else "/orders",
            detail_return_qs=query_keep(return_to=return_to),
        ),
    )


def _image_status_summary(counts: dict[str, int]) -> dict[str, int]:
    return {
        "total": sum(counts.values()),
        "uploaded": counts.get("uploaded", 0),
        "compressed": counts.get("compressed", 0),
        "errors": sum(
            counts.get(status, 0)
            for status in (
                "compression_error",
                "upload_error",
                "send_error",
                "rule_error",
                "metadata_error",
                "file_missing",
            )
        ),
        "discarded": sum(
            counts.get(status, 0)
            for status in (
                "discarded_modality",
                "discarded_study",
                "discarded_rule",
            )
        ),
    }


@router.get("/orders/{order_id}", response_class=HTMLResponse)
def order_detail(
    order_id: int,
    request: Request,
    page: int = 1,
    return_to: str = Query("", max_length=2048),
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    order = db.scalar(
        select(Order).options(selectinload(Order.unit)).where(Order.id == order_id)
    )
    if order is None:
        return RedirectResponse("/orders", status_code=303)
    default_return_path = "/orders/history" if order.archived_at else "/orders"
    parsed_return = urlsplit(return_to)
    if (
        not return_to
        or parsed_return.scheme
        or parsed_return.netloc
        or parsed_return.path != default_return_path
    ):
        return_to = default_return_path
    order.can_reprocess = can_reprocess(order)  # type: ignore[attr-defined]
    order.can_archive = can_archive(order)  # type: ignore[attr-defined]
    order.can_cancel = can_cancel(order)  # type: ignore[attr-defined]
    manual_move_active = bool(
        db.scalar(
            select(func.count()).where(
                ManualMoveRequest.order_id == order_id,
                ManualMoveRequest.status.in_(("queued", "running")),
            )
        )
    )
    can_manual_move = bool(
        order.study_uid
        and order.archived_at is None
        and order.status not in ACTIVE_ORDER_STATUSES
        and order.status != "cancelled"
        and not manual_move_active
    )
    total = db.scalar(select(func.count()).where(OrderEvent.order_id == order_id)) or 0
    pager = paginate(total, page, EVENTS_PAGE)
    events = list(
        db.scalars(
            select(OrderEvent)
            .where(OrderEvent.order_id == order_id)
            .order_by(OrderEvent.id.desc())
            .offset(pager["offset"])
            .limit(pager["size"])
        )
    )
    transfer_stats = list(
        db.execute(
            select(ImageTransfer.status, func.count())
            .where(
                ImageTransfer.order_id == order_id,
                ~ImageTransfer.id.in_(
                    select(HistoricalImageLink.transfer_id)
                    .join(
                        HistoricalStudy,
                        HistoricalStudy.id == HistoricalImageLink.historical_study_id,
                    )
                    .where(HistoricalStudy.order_id == order_id)
                ),
            )
            .group_by(ImageTransfer.status)
            .order_by(ImageTransfer.status)
        )
    )
    transfer_counts = dict(transfer_stats)
    image_summary = _image_status_summary(transfer_counts)
    historical_studies = list(
        db.scalars(
            select(HistoricalStudy)
            .where(HistoricalStudy.order_id == order_id)
            .order_by(HistoricalStudy.study_date.desc(), HistoricalStudy.id)
        )
    )
    historical_counts: dict[int, dict[str, int]] = {
        study.id: {} for study in historical_studies
    }
    if historical_counts:
        count_rows = db.execute(
            select(
                HistoricalImageLink.historical_study_id,
                ImageTransfer.status,
                func.count(),
            )
            .join(
                ImageTransfer,
                ImageTransfer.id == HistoricalImageLink.transfer_id,
            )
            .where(HistoricalImageLink.historical_study_id.in_(historical_counts))
            .group_by(
                HistoricalImageLink.historical_study_id,
                ImageTransfer.status,
            )
        )
        for study_id, transfer_status, count in count_rows:
            historical_counts[int(study_id)][str(transfer_status)] = int(count)
    historical_total_images = 0
    for study in historical_studies:
        counts = historical_counts[study.id]
        study.image_counts = counts  # type: ignore[attr-defined]
        study.image_summary = _image_status_summary(counts)  # type: ignore[attr-defined]
        historical_total_images += study.image_summary["total"]  # type: ignore[attr-defined]
    return templates.TemplateResponse(
        request=request,
        name="order_detail.html",
        context=ctx(
            request,
            db,
            "order_history" if order.archived_at else "orders",
            order=order,
            events=events,
            status_label=STATUSES.get(order.status, order.status),
            badge=badge_for(order.status),
            transfer_counts=transfer_counts,
            image_summary=image_summary,
            send_failures=_order_send_failures(db, order_id),
            can_manual_move=can_manual_move,
            manual_move_active=manual_move_active,
            historical_studies=historical_studies,
            historical_total_images=historical_total_images,
            prior_status_label=PRIOR_STATUS_LABELS.get(
                order.prior_status, order.prior_status
            ),
            pager=pager,
            qs=query_keep(return_to=return_to),
            return_to=return_to,
            detail_return_qs=query_keep(return_to=return_to),
        ),
    )


@router.post("/orders/{order_id}/retrieve-now")
def order_retrieve_now(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    order = db.get(Order, order_id)
    if order is None:
        flash(request, "Pedido não encontrado.", "err")
        return RedirectResponse("/orders", status_code=303)
    if order.archived_at is not None:
        flash(request, "Pedidos arquivados são somente para consulta.", "err")
    elif not order.study_uid:
        flash(request, "Aguarde o C-FIND localizar o exame atual.", "err")
    elif order.status in ACTIVE_ORDER_STATUSES:
        flash(request, "Já existe um C-MOVE do exame atual em andamento.", "err")
    elif order.status == "cancelled":
        flash(request, "O pedido está cancelado.", "err")
    elif db.scalar(
        select(ManualMoveRequest.id).where(
            ManualMoveRequest.order_id == order.id,
            ManualMoveRequest.status.in_(("queued", "running")),
        )
    ):
        flash(request, "O C-MOVE manual já está na fila ou em andamento.", "err")
    else:
        correlation_id = new_correlation_id()
        db.add(
            ManualMoveRequest(
                order_id=order.id,
                unit_id=order.unit_id,
                requested_by_user_id=user.id,
                requested_by_username=user.username,
                correlation_id=correlation_id,
                status="queued",
            )
        )
        db.add(
            OrderEvent(
                order_id=order.id,
                level="info",
                message="C-MOVE manual do exame atual solicitado",
            )
        )
        audit(
            db,
            request,
            user,
            action="retry",
            resource_type="order",
            resource_id=order.id,
            resource_name=order.acc,
            summary="C-MOVE manual do exame atual colocado na fila.",
        )
        db.commit()
        log_event(
            log,
            logging.INFO,
            "order.current_move.request",
            resource=f"order:{order.id}",
            status="success",
            order_id=order.id,
            unit_id=order.unit_id,
            user_id=user.id,
            correlation_id=correlation_id,
        )
        flash(request, "C-MOVE do exame atual colocado na fila imediata.")
    return RedirectResponse(f"/orders/{order_id}", status_code=303)


@router.post("/orders/{order_id}/retry")
def order_retry(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    order = db.get(Order, order_id)
    if order and can_reprocess(order):
        _reset_order_for_reprocess(db, order)
        audit(
            db,
            request,
            user,
            action="retry",
            resource_type="order",
            resource_id=order.id,
            resource_name=order.acc,
            summary="Pedido reiniciado para novo retrieve e envio.",
        )
        db.commit()
        log_event(
            log,
            logging.INFO,
            "order.reprocess",
            resource=f"order:{order.id}",
            status="success",
            order_id=order.id,
            unit_id=order.unit_id,
            user_id=user.id,
        )
        flash(request, "Pedido reiniciado para novo retrieve e envio.")
    elif order:
        flash(
            request, "Aguarde o processamento atual terminar para reprocessar.", "err"
        )
    return RedirectResponse(
        request.headers.get("referer") or "/orders", status_code=303
    )


@router.post("/orders/{order_id}/retry-prior")
def order_retry_prior(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    order = db.get(Order, order_id)
    if order is None:
        flash(request, "Pedido não encontrado.", "err")
        return RedirectResponse("/orders", status_code=303)
    if order.archived_at is not None:
        flash(request, "Pedidos arquivados são somente para consulta.", "err")
        return RedirectResponse(f"/orders/{order_id}", status_code=303)
    if order.prior_status == "disabled":
        flash(request, "O retrieve histórico não está habilitado neste pedido.", "err")
    elif order.prior_status in ACTIVE_PRIOR_STATUSES:
        flash(request, "O retrieve histórico já está ativo.", "err")
    elif order.status in ACTIVE_ORDER_STATUSES:
        flash(request, "Aguarde o processamento atual terminar.", "err")
    else:
        for study in list(order.historical_studies):
            db.delete(study)
        db.execute(
            delete(HistoricalSeries).where(HistoricalSeries.order_id == order.id)
        )
        order.prior_status = "queued"
        order.prior_due_at = datetime.now()
        order.prior_started_at = None
        order.prior_completed_at = None
        order.prior_heartbeat_at = None
        order.prior_attempts = 0
        order.prior_last_error = ""
        db.add(
            OrderEvent(
                order_id=order.id,
                level="info",
                message="Reprocessamento manual do histórico solicitado",
            )
        )
        audit(
            db,
            request,
            user,
            action="retry",
            resource_type="order",
            resource_id=order.id,
            resource_name=order.acc,
            summary="Retrieve histórico colocado novamente na fila.",
        )
        db.commit()
        log_event(
            log,
            logging.INFO,
            "order.prior.reprocess",
            resource=f"order:{order.id}",
            status="success",
            order_id=order.id,
            unit_id=order.unit_id,
            user_id=user.id,
        )
        flash(request, "Retrieve histórico colocado novamente na fila.")
    return RedirectResponse(f"/orders/{order_id}", status_code=303)


def _reset_order_for_reprocess(db: Session, order: Order) -> None:
    order.correlation_id = new_correlation_id()
    order.attempts = 0
    order.done_at = None
    order.heartbeat_at = None
    order.last_error = ""
    if order.prior_status == "cancelled":
        if order.unit.retrieve_prior_enabled:
            order.prior_status = "queued"
            order.prior_due_at = datetime.now()
            order.prior_started_at = None
            order.prior_completed_at = None
            order.prior_heartbeat_at = None
            order.prior_attempts = 0
            order.prior_last_error = ""
        else:
            order.prior_status = "disabled"
    if order.study_uid:
        order.status = "wait_retrieve"
        order.retrieve_at = datetime.now()
        _, _, order.second_retrieve_at = schedule_from_now(db, order.modality)
    else:
        order.status = "watching"
        order.last_find_at = None
        order.retrieve_at = None
        order.second_retrieve_at = None
        order.found_at = None
    db.add(
        OrderEvent(
            order_id=order.id,
            level="info",
            message="Reprocessamento manual solicitado",
        )
    )


@router.post("/orders/{order_id}/cancel")
def order_cancel(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    order = db.get(Order, order_id)
    running_manual = bool(
        order
        and db.scalar(
            select(ManualMoveRequest.id).where(
                ManualMoveRequest.order_id == order.id,
                ManualMoveRequest.status == "running",
            )
        )
    )
    if order and can_cancel(order) and not running_manual:
        queued_manual = list(
            db.scalars(
                select(ManualMoveRequest).where(
                    ManualMoveRequest.order_id == order.id,
                    ManualMoveRequest.status == "queued",
                )
            )
        )
        for move_request in queued_manual:
            move_request.status = "cancelled"
            move_request.completed_at = datetime.now()
            move_request.last_error = "Cancelado junto com o pedido"
        if order.prior_status in {"queued", "retry_wait"}:
            order.prior_status = "cancelled"
            order.prior_completed_at = datetime.now()
            order.prior_last_error = "Cancelado junto com o pedido"
        order.status = "cancelled"
        audit(
            db,
            request,
            user,
            action="cancel",
            resource_type="order",
            resource_id=order.id,
            resource_name=order.acc,
            summary="Processamento do pedido cancelado.",
        )
        db.commit()
        flash(request, "Pedido cancelado.")
    elif order:
        flash(
            request,
            "Aguarde o retrieve atual terminar antes de cancelar.",
            "err",
        )
    return RedirectResponse(
        request.headers.get("referer") or "/orders", status_code=303
    )


def _delete_order_record(
    db: Session,
    order: Order,
    *,
    actor_id: int | None = None,
    actor_username: str = "Sistema",
    reason: str = "Pedido arquivado manualmente.",
) -> None:
    archive_order(
        db,
        order,
        reason=reason,
        actor_id=actor_id,
        actor_username=actor_username,
    )


@router.post("/orders/{order_id}/delete")
def order_delete(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    order = db.get(Order, order_id)
    if order is None:
        flash(request, "Pedido não encontrado.", "err")
        return RedirectResponse("/orders", status_code=303)
    if order.archived_at is not None:
        flash(request, "O pedido já está arquivado.", "err")
        return RedirectResponse("/orders/history", status_code=303)
    if not can_archive(order):
        flash(request, "Aguarde o processamento atual terminar para excluir.", "err")
        return RedirectResponse("/orders", status_code=303)
    unit_id = order.unit_id
    accession = order.acc
    _delete_order_record(
        db,
        order,
        actor_id=user.id,
        actor_username=user.username,
    )
    audit(
        db,
        request,
        user,
        action="archive",
        resource_type="order",
        resource_id=order_id,
        resource_name=accession,
        summary="Pedido arquivado com eventos e arquivos clínicos preservados.",
    )
    db.commit()
    log_event(
        log,
        logging.INFO,
        "order.archive",
        resource=f"order:{order_id}",
        status="success",
        order_id=order_id,
        unit_id=unit_id,
        user_id=user.id,
    )
    flash(request, "Pedido arquivado. Todo o histórico foi preservado.")
    return RedirectResponse("/orders", status_code=303)
