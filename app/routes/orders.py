"""Order queue, history, detail and order actions."""

from __future__ import annotations

from datetime import datetime
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import case, delete, func, or_, select
from sqlalchemy.orm import Session, selectinload
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.db import count_rows, get_db
from app.events import add_event
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
from app.observability import new_correlation_id
from app.order_state import (
    ACTIVE_ORDER_STATUSES,
    ACTIVE_PRIOR_STATUSES,
    can_archive,
    can_cancel,
    can_reprocess,
    queue_prior_retrieve,
)
from app.pager import keyset_page, paginate, query_keep
from app.pipeline import resend_failed_transfers
from app.pipeline.monitor import finish_monitoring, reset_monitoring
from app.retention import archive_order
from app.rules import prior_skip_reason
from app.web import (
    DEFAULT_PAGE_SIZE,
    EVENTS_PAGE,
    PAGE_SIZE_OPTIONS,
    PRIOR_STATUS_LABELS,
    FlashRedirect,
    badge_for,
    commit_action,
    ctx,
    flash,
    redirect,
    require_admin,
    require_user,
    templates,
)
from app.wording import counted

router = APIRouter()

MONITOR_STOPPABLE = frozenset({"monitoring", "wait_update"})


def _found(order: Order | None) -> Order:
    if order is None:
        raise FlashRedirect("/orders", "Pedido não encontrado.")
    return order


def _viewable_order(
    order_id: int,
    db: Session = Depends(get_db),
    _user: User = Depends(require_user),
) -> Order:
    return _found(
        db.scalar(
            select(Order).options(selectinload(Order.unit)).where(Order.id == order_id)
        )
    )


def _admin_order(
    order_id: int,
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
) -> Order:
    """Load an order for an admin action; authorization is checked first."""
    return _found(db.get(Order, order_id))


def _locked_admin_order(
    order_id: int,
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
) -> Order:
    return _found(
        db.scalar(select(Order).where(Order.id == order_id).with_for_update())
    )


def _active_order(order: Order = Depends(_admin_order)) -> Order:
    """Admin order that still accepts actions (not archived)."""
    if order.archived_at is not None:
        raise FlashRedirect(
            f"/orders/{order.id}", "Pedidos arquivados são somente para consulta."
        )
    return order


@router.post("/orders/{order_id}/resend-failed")
def order_resend_failed(
    order_id: int,
    request: Request,
    order: Order = Depends(_active_order),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    count = resend_failed_transfers(db, ImageTransfer.order_id == order.id)
    return commit_action(
        db,
        request,
        user,
        action="resend",
        resource_type="order",
        resource_id=order.id,
        resource_name=order.acc,
        summary=counted(
            count,
            "imagem com falha de envio devolvida à fila.",
            "imagens com falha de envio devolvidas à fila.",
        ),
        notice=counted(
            count,
            "imagem devolvida à fila de envio.",
            "imagens devolvidas à fila de envio.",
        ),
        redirect_to=f"/orders/{order_id}",
    )


def _order_send_failures(db: Session, order_id: int) -> int:
    return count_rows(
        db,
        ImageTransfer,
        ImageTransfer.order_id == order_id,
        ImageTransfer.status == "send_error",
    )


# Intervalo do auto-refresh da fila; a consulta é mais pesada que a da visão geral.
ORDERS_POLL_SECONDS = 10
# O histórico só cresce: a contagem para aqui ("mais de 10.000") para que abrir
# a tela custe o mesmo com dez mil ou dez milhões de pedidos arquivados.
HISTORY_COUNT_LIMIT = 10_000


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
    page: int = Query(1, ge=0),  # 0: page beyond HISTORY_COUNT_LIMIT
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


def status_condition(status: str):
    """Queue filter for one status. "error" also matches a failed historical
    retrieve, as the dashboard's error count does."""
    if status == "error":
        return (Order.status == "error") | (Order.prior_status == "error")
    return Order.status == status


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
    archived_filter = (
        Order.archived_at.is_not(None) if history else Order.archived_at.is_(None)
    )
    filt = select(Order).where(archived_filter)
    if unit_id:
        filt = filt.where(Order.unit_id == unit_id)
    if status:
        filt = filt.where(status_condition(status))
    if q:
        like = f"%{q.strip()}%"
        filt = filt.where(
            or_(
                Order.acc.ilike(like),
                Order.pat_id.ilike(like),
                Order.source_id.ilike(like),
            )
        )
    # Archived orders are never waiting or running: the history shows only its
    # total (counted up to HISTORY_COUNT_LIMIT by the pager). The queue counts
    # its total and each situation in one scan of the filtered orders.
    status_counts = None if history else _order_status_counts(db, filt)
    rows, pager = keyset_page(
        db,
        filt,
        Order.id,
        page=page,
        page_size=page_size,
        size_options=PAGE_SIZE_OPTIONS,
        before=before,
        after=after,
        last=last,
        keep={"unit_id": unit_id, "status": status, "q": q},
        options=(selectinload(Order.unit),),
        count_limit=HISTORY_COUNT_LIMIT if history else None,
        total=status_counts["total"] if status_counts else None,
    )
    for o in rows:
        o.status_label = STATUSES.get(o.status, o.status)  # type: ignore[attr-defined]
        o.badge = badge_for(o.status)  # type: ignore[attr-defined]
        o.can_reprocess = can_reprocess(o)  # type: ignore[attr-defined]
        o.can_archive = can_archive(o)  # type: ignore[attr-defined]
        o.prior_status_label = PRIOR_STATUS_LABELS.get(  # type: ignore[attr-defined]
            o.prior_status, o.prior_status
        )
    order_summary = {
        "total": pager["total"],
        "total_label": pager["total_label"],
    }
    if status_counts:
        order_summary |= {
            key: status_counts[key] for key in ("waiting", "running", "done")
        }
    units = list(db.scalars(select(Unit).order_by(Unit.name)))
    qs = query_keep(unit_id=unit_id, status=status, q=q, page_size=page_size)
    return_to = request.url.path
    if request.url.query:
        return_to = f"{return_to}?{request.url.query}"
    # O polling da fila pede só resumo e tabela; o histórico não muda sozinho.
    partial = not history and request.headers.get("x-partial") == "1"
    return templates.TemplateResponse(
        request=request,
        name="orders_partial.html" if partial else "orders.html",
        context=ctx(
            request,
            db,
            "order_history" if history else "orders",
            poll_seconds=0 if history else ORDERS_POLL_SECONDS,
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


def _order_status_counts(db: Session, filt) -> dict[str, int]:
    """Total, waiting, running and done of the filtered orders, in one scan."""
    filtered_orders = filt.subquery()
    row = db.execute(
        select(
            func.count(),
            func.sum(
                case(
                    (
                        filtered_orders.c.status.in_(
                            ("watching", "wait_retrieve", "wait_update")
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
    return {
        "total": int(row[0] or 0),
        "waiting": int(row[1] or 0),
        "running": int(row[2] or 0),
        "done": int(row[3] or 0),
    }


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
    order: Order = Depends(_viewable_order),
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
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
        count_rows(
            db,
            ManualMoveRequest,
            ManualMoveRequest.order_id == order_id,
            ManualMoveRequest.status.in_(("queued", "running")),
        )
    )
    can_manual_move = bool(
        order.study_uid
        and order.archived_at is None
        and order.status not in ACTIVE_ORDER_STATUSES
        and order.status != "cancelled"
        and not manual_move_active
    )
    total = count_rows(db, OrderEvent, OrderEvent.order_id == order_id)
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
        status_rows = db.execute(
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
        for study_id, transfer_status, count in status_rows:
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
    order: Order = Depends(_active_order),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    if not order.study_uid:
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
        return commit_action(
            db,
            request,
            user,
            action="retry",
            resource_type="order",
            resource_id=order.id,
            resource_name=order.acc,
            summary="C-MOVE manual do exame atual colocado na fila.",
            notice="C-MOVE do exame atual colocado na fila imediata.",
            redirect_to=f"/orders/{order_id}",
            event="order.current_move.request",
            order_id=order.id,
            unit_id=order.unit_id,
            correlation_id=correlation_id,
        )
    return redirect(f"/orders/{order_id}")


@router.post("/orders/{order_id}/retry")
def order_retry(
    request: Request,
    order: Order = Depends(_admin_order),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    if not can_reprocess(order):
        flash(
            request, "Aguarde o processamento atual terminar para reprocessar.", "err"
        )
        return redirect(request.headers.get("referer") or "/orders")
    _reset_order_for_reprocess(db, order)
    return commit_action(
        db,
        request,
        user,
        action="retry",
        resource_type="order",
        resource_id=order.id,
        resource_name=order.acc,
        summary="Pedido reiniciado para novo retrieve e envio.",
        notice="Pedido reiniciado para novo retrieve e envio.",
        redirect_to=request.headers.get("referer") or "/orders",
        event="order.reprocess",
        order_id=order.id,
        unit_id=order.unit_id,
    )


@router.post("/orders/{order_id}/retry-prior")
def order_retry_prior(
    order_id: int,
    request: Request,
    order: Order = Depends(_active_order),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
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
        queue_prior_retrieve(order, datetime.now())
        db.add(
            OrderEvent(
                order_id=order.id,
                level="info",
                message="Reprocessamento manual do histórico solicitado",
            )
        )
        return commit_action(
            db,
            request,
            user,
            action="retry",
            resource_type="order",
            resource_id=order.id,
            resource_name=order.acc,
            summary="Retrieve histórico colocado novamente na fila.",
            notice="Retrieve histórico colocado novamente na fila.",
            redirect_to=f"/orders/{order_id}",
            event="order.prior.reprocess",
            order_id=order.id,
            unit_id=order.unit_id,
        )
    return redirect(f"/orders/{order_id}")


@router.post("/orders/{order_id}/monitor-now")
def order_monitor_now(
    order_id: int,
    request: Request,
    order: Order = Depends(_admin_order),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    if order.archived_at is not None or order.status != "monitoring":
        flash(request, "O pedido não está monitorando novas imagens.", "err")
        return redirect(f"/orders/{order_id}")
    order.monitor_next_at = datetime.now()
    add_event(db, order, f"Verificação imediata solicitada por {user.username}")
    return commit_action(
        db,
        request,
        user,
        action="retry",
        resource_type="order",
        resource_id=order.id,
        resource_name=order.acc,
        summary="Verificação imediata de novas imagens solicitada.",
        notice="Verificação de novas imagens colocada na fila imediata.",
        redirect_to=f"/orders/{order_id}",
    )


@router.post("/orders/{order_id}/monitor-stop")
def order_monitor_stop(
    order_id: int,
    request: Request,
    order: Order = Depends(_locked_admin_order),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    if order.archived_at is not None or order.status not in MONITOR_STOPPABLE:
        flash(
            request,
            "Só é possível encerrar um monitoramento sem C-MOVE em andamento.",
            "err",
        )
        return redirect(f"/orders/{order_id}")
    finish_monitoring(db, order, datetime.now(), user.username)
    return commit_action(
        db,
        request,
        user,
        action="update",
        resource_type="order",
        resource_id=order.id,
        resource_name=order.acc,
        summary="Monitoramento de novas imagens encerrado manualmente.",
        notice="Monitoramento de novas imagens encerrado.",
        redirect_to=f"/orders/{order_id}",
        event="order.monitor.stop",
        order_id=order.id,
        unit_id=order.unit_id,
    )


def _reset_order_for_reprocess(db: Session, order: Order) -> None:
    order.correlation_id = new_correlation_id()
    order.attempts = 0
    order.done_at = None
    order.heartbeat_at = None
    order.last_error = ""
    if order.prior_status == "cancelled":
        prior_skip = prior_skip_reason(db, order.unit, order.modality)
        if prior_skip is None:
            queue_prior_retrieve(order, datetime.now())
        else:
            order.prior_status = "disabled"
            if prior_skip:
                add_event(db, order, prior_skip)
    if order.study_uid:
        order.status = "wait_retrieve"
        order.retrieve_at = datetime.now()
    else:
        order.status = "watching"
        order.last_find_at = None
        order.retrieve_at = None
        order.found_at = None
    reset_monitoring(order)
    db.add(
        OrderEvent(
            order_id=order.id,
            level="info",
            message="Reprocessamento manual solicitado",
        )
    )


@router.post("/orders/{order_id}/cancel")
def order_cancel(
    request: Request,
    order: Order = Depends(_admin_order),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    running_manual = bool(
        db.scalar(
            select(ManualMoveRequest.id).where(
                ManualMoveRequest.order_id == order.id,
                ManualMoveRequest.status == "running",
            )
        )
    )
    if not can_cancel(order) or running_manual:
        flash(
            request,
            "Aguarde o retrieve atual terminar antes de cancelar.",
            "err",
        )
        return redirect(request.headers.get("referer") or "/orders")
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
    return commit_action(
        db,
        request,
        user,
        action="cancel",
        resource_type="order",
        resource_id=order.id,
        resource_name=order.acc,
        summary="Processamento do pedido cancelado.",
        notice="Pedido cancelado.",
        redirect_to=request.headers.get("referer") or "/orders",
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
    order: Order = Depends(_admin_order),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    if order.archived_at is not None:
        flash(request, "O pedido já está arquivado.", "err")
        return redirect("/orders/history")
    if not can_archive(order):
        flash(request, "Aguarde o processamento atual terminar para excluir.", "err")
        return redirect("/orders")
    unit_id = order.unit_id
    accession = order.acc
    _delete_order_record(
        db,
        order,
        actor_id=user.id,
        actor_username=user.username,
    )
    return commit_action(
        db,
        request,
        user,
        action="archive",
        resource_type="order",
        resource_id=order_id,
        resource_name=accession,
        summary="Pedido arquivado com eventos e arquivos clínicos preservados.",
        notice="Pedido arquivado. Todo o histórico foi preservado.",
        redirect_to="/orders",
        event="order.archive",
        order_id=order_id,
        unit_id=unit_id,
    )
