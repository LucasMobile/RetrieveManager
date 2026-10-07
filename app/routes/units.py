"""Unit registration, receiver/PACS settings and unit actions."""

from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import or_, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload
from starlette.concurrency import run_in_threadpool

from app.audit import audit
from app.compression import (
    compression_form_for_unit,
    compression_modalities_for_form,
    default_unit_compression_form,
    save_unit_compression_settings,
    validate_unit_compression_form,
)
from app.config import (
    DEFAULT_CLOUD_URL,
    SEND_MAX_ATTEMPTS,
)
from app.db import count_rows, get_db
from app.dicom_net import PacsNode, echo
from app.dicom_rules import (
    link_default_rules,
)
from app.models import (
    ImageTransfer,
    Order,
    Unit,
    User,
)
from app.observability import log_event
from app.order_state import (
    ACTIVE_ORDER_STATUSES,
    ACTIVE_PRIOR_STATUSES,
)
from app.pager import paginate
from app.pipeline import resend_failed_transfers
from app.retention import archive_unit
from app.rules import (
    DEFAULT_PRIOR_MODALITIES,
    cancel_unwanted_priors,
    prior_modalities_for,
    save_prior_modalities,
)
from app.store_routing import (
    STORE_ROUTING_LOCK_KEY,
    StoreEndpoint,
    build_routing_plan,
    endpoint_conflicts,
    units_sharing_listener,
)
from app.validation import (
    validate_pacs_connection,
    validate_unit_form,
)
from app.web import (
    UNITS_PAGE,
    FlashRedirect,
    commit_action,
    ctx,
    flash,
    form_strings,
    log,
    redirect,
    require_admin,
    templates,
)
from app.wording import counted

router = APIRouter()


def _active_unit(
    unit_id: int,
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
) -> Unit:
    """Load a unit that was not archived; authorization is checked first."""
    unit = db.get(Unit, unit_id)
    if unit is None or unit.deleted_at is not None:
        raise FlashRedirect("/units")
    return unit


@router.get("/units", response_class=HTMLResponse)
def units_list(
    request: Request,
    page: int = 1,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    total = count_rows(db, Unit, Unit.deleted_at.is_(None))
    enabled = count_rows(db, Unit, Unit.enabled.is_(True), Unit.deleted_at.is_(None))
    pager = paginate(total, page, UNITS_PAGE)
    units = list(
        db.scalars(
            select(Unit)
            .options(selectinload(Unit.prior_modalities))
            .where(Unit.deleted_at.is_(None))
            .order_by(Unit.name)
            .offset(pager["offset"])
            .limit(pager["size"])
        )
    )
    return templates.TemplateResponse(
        request=request,
        name="units_list.html",
        context=ctx(
            request,
            db,
            "units",
            units=units,
            pager=pager,
            qs="",
            summary={"total": total, "enabled": enabled, "paused": total - enabled},
        ),
    )


@router.get("/units/new", response_class=HTMLResponse)
def units_new(
    request: Request, db: Session = Depends(get_db), user: User = Depends(require_admin)
):
    return _unit_form_page(request, db, None)


def _unit_form_page(request: Request, db: Session, unit: Unit | None):
    """Render the create (``unit=None``) or edit form for a unit."""
    store_shared_with: list[str] = []
    store_route_problem = None
    if unit is None:
        compression_settings = default_unit_compression_form()
        prior_modalities = DEFAULT_PRIOR_MODALITIES
    else:
        compression_settings = compression_form_for_unit(db, unit.id)
        prior_modalities = prior_modalities_for(db, unit.id)
        endpoint = StoreEndpoint.from_unit(unit)
        others = _other_endpoints(db, unit.id)
        store_shared_with = [
            other.name for other in units_sharing_listener(endpoint, others)
        ]
        store_route_problem = build_routing_plan([endpoint, *others]).unit_problems.get(
            unit.id
        )
    return templates.TemplateResponse(
        request=request,
        name="units_form.html",
        context=ctx(
            request,
            db,
            "units",
            unit=unit,
            prior_modalities=prior_modalities,
            default_cloud_url=DEFAULT_CLOUD_URL,
            compression_settings=compression_settings,
            compression_modalities=compression_modalities_for_form(
                compression_settings
            ),
            send_max_attempts=SEND_MAX_ATTEMPTS,
            store_shared_with=store_shared_with,
            store_route_problem=store_route_problem,
        ),
    )


def _unit_from_form(form: dict[str, Any], unit: Unit | None) -> Unit:
    obj = unit or Unit()
    obj.name = str(form["name"])
    obj.enabled = bool(form["enabled"])
    obj.pacs_aet = str(form["pacs_aet"])
    obj.pacs_ip = str(form["pacs_ip"])
    obj.pacs_port = int(form["pacs_port"])
    obj.pacs_patient_id_wildcard = bool(form["pacs_patient_id_wildcard"])
    obj.calling_aet = str(form["calling_aet"])
    obj.store_port = int(form["store_port"])
    obj.store_allowed_aets = str(form["store_allowed_aets"])
    obj.store_allowed_ips = str(form["store_allowed_ips"])
    obj.orders_api_url = str(form["orders_api_url"])
    orders_api_token = str(form.get("orders_api_token") or "")
    if orders_api_token:
        obj.orders_api_token = orders_api_token
    elif unit is None:
        obj.orders_api_token = ""
    obj.orders_api_station_id = str(form.get("orders_api_station_id") or "")
    obj.orders_api_company_id = str(form["orders_api_company_id"])
    obj.retrieve_prior_enabled = bool(form["retrieve_prior_enabled"])
    obj.move_timeout_prior = int(form["move_timeout_prior"])
    obj.receive_dir = str(form["receive_dir"])
    obj.send_dir = str(form["send_dir"])
    obj.error_dir = str(form["error_dir"])
    token = str(form.get("token") or "")
    if token:
        obj.token = token
    elif unit is None:
        obj.token = ""
    obj.cloud_url = str(form["cloud_url"])
    obj.move_timeout_first = int(form["move_timeout_first"])
    obj.move_timeout_update = int(form["move_timeout_update"])
    obj.max_parallel_moves = int(form["max_parallel_moves"])
    obj.find_interval_seconds = int(form["find_interval_seconds"])
    obj.compact_workers = int(form["compact_workers"])
    obj.send_workers = int(form["send_workers"])
    return obj


def _other_endpoints(db: Session, unit_id: int | None) -> list[StoreEndpoint]:
    q = select(Unit).where(Unit.deleted_at.is_(None))
    if unit_id:
        q = q.where(Unit.id != unit_id)
    # The unit being edited may hold unsaved form values: never flush them.
    with db.no_autoflush:
        return [StoreEndpoint.from_unit(unit) for unit in db.scalars(q)]


def _store_route_problems(
    db: Session, form: dict[str, Any], unit_id: int | None
) -> list[str]:
    """Routing and folder conflicts of the submitted unit with the others.

    The advisory lock lasts until this request's transaction ends, so two
    saves cannot both pass the check and then commit conflicting units.
    """
    db.execute(
        text("SELECT pg_advisory_xact_lock(:key)"), {"key": STORE_ROUTING_LOCK_KEY}
    )
    candidate = StoreEndpoint.from_values(
        unit_id=unit_id,
        name=str(form["name"]),
        enabled=bool(form["enabled"]),
        store_port=int(form["store_port"]),
        calling_aet=str(form["calling_aet"]),
        store_allowed_aets=str(form["store_allowed_aets"]),
        receive_dir=str(form["receive_dir"]),
        send_dir=str(form["send_dir"]),
        error_dir=str(form["error_dir"]),
    )
    return endpoint_conflicts(candidate, _other_endpoints(db, unit_id))


@router.post("/units/new")
async def units_create(
    request: Request, db: Session = Depends(get_db), user: User = Depends(require_admin)
):
    raw_form = await form_strings(request)
    try:
        form = validate_unit_form(raw_form, creating=True)
        compression_settings = validate_unit_compression_form(raw_form)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return redirect("/units/new")
    problems = _store_route_problems(db, form, None)
    if problems:
        db.rollback()
        flash(request, " ".join(problems[:3]), "err")
        return redirect("/units/new")
    unit = _unit_from_form(form, None)
    db.add(unit)
    try:
        db.flush()
        save_unit_compression_settings(db, unit, compression_settings)
        save_prior_modalities(db, unit, form["prior_modalities"])
        link_default_rules(db, unit)
        audit(
            db,
            request,
            user,
            action="create",
            resource_type="unit",
            resource_id=unit.id,
            resource_name=unit.name,
            summary="Unidade adicionada ao sistema.",
        )
        db.commit()
    except IntegrityError:
        db.rollback()
        flash(request, "Não foi possível salvar (nome duplicado?).", "err")
        return redirect("/units/new")
    flash(request, "Unidade criada. Cadastre o AET no PACS se ainda não existir.")
    return redirect("/units")


@router.post("/units/test-echo")
async def units_test_echo(
    request: Request,
    user: User = Depends(require_admin),
):
    raw_form = await form_strings(request)
    try:
        connection = validate_pacs_connection(raw_form)
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)

    started_at = perf_counter()
    node = PacsNode(
        host=str(connection["pacs_ip"]),
        port=int(connection["pacs_port"]),
        called_aet=str(connection["pacs_aet"]),
        calling_aet=str(connection["calling_aet"]),
    )
    result = await run_in_threadpool(lambda: echo(node, timeout=10))

    if not result.ok:
        log_event(
            log,
            logging.WARNING,
            "dicom.echo",
            resource="pacs",
            status="failure",
            started_at=started_at,
            error_type=result.error,
            dicom_status=result.status,
            user_id=user.id,
        )
        return JSONResponse(
            {
                "ok": False,
                "message": (
                    "O PACS não respondeu ao C-ECHO. Revise AET, endereço e porta."
                ),
            },
            status_code=502,
        )

    log_event(
        log,
        logging.INFO,
        "dicom.echo",
        resource="pacs",
        status="success",
        started_at=started_at,
        dicom_status=result.status,
        user_id=user.id,
    )
    return JSONResponse({"ok": True, "message": "C-ECHO OK"})


@router.get("/units/{unit_id}", response_class=HTMLResponse)
def units_edit(
    request: Request,
    unit: Unit = Depends(_active_unit),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    return _unit_form_page(request, db, unit)


@router.post("/units/{unit_id}")
async def units_update(
    unit_id: int,
    request: Request,
    unit: Unit = Depends(_active_unit),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    raw_form = await form_strings(request)
    try:
        form = validate_unit_form(raw_form)
        compression_settings = validate_unit_compression_form(raw_form)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return redirect(f"/units/{unit_id}")
    problems = _store_route_problems(db, form, unit_id)
    if problems:
        db.rollback()
        flash(request, " ".join(problems[:3]), "err")
        return redirect(f"/units/{unit_id}")
    _unit_from_form(form, unit)
    try:
        save_unit_compression_settings(db, unit, compression_settings)
        save_prior_modalities(db, unit, form["prior_modalities"])
        db.flush()
        priors_cancelled = cancel_unwanted_priors(db, unit)
        audit(
            db,
            request,
            user,
            action="update",
            resource_type="unit",
            resource_id=unit.id,
            resource_name=unit.name,
            summary="Configuração da unidade atualizada."
            + (
                " "
                + counted(
                    priors_cancelled,
                    "retrieve histórico da fila cancelado.",
                    "retrieves históricos da fila cancelados.",
                )
                if priors_cancelled
                else ""
            ),
        )
        db.commit()
    except IntegrityError:
        db.rollback()
        flash(request, "Não foi possível salvar (nome duplicado?).", "err")
        return redirect(f"/units/{unit_id}")
    if priors_cancelled:
        flash(
            request,
            "Unidade atualizada. "
            + counted(
                priors_cancelled,
                "retrieve histórico que estava na fila foi cancelado.",
                "retrieves históricos que estavam na fila foram cancelados.",
            ),
        )
    else:
        flash(request, "Unidade atualizada.")
    return redirect(f"/units/{unit_id}")


@router.post("/units/{unit_id}/toggle")
def units_toggle(
    request: Request,
    unit: Unit = Depends(_active_unit),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    unit.enabled = not unit.enabled
    return commit_action(
        db,
        request,
        user,
        action="enable" if unit.enabled else "disable",
        resource_type="unit",
        resource_id=unit.id,
        resource_name=unit.name,
        summary="Unidade ativada." if unit.enabled else "Unidade pausada.",
        notice="Unidade " + ("ativada" if unit.enabled else "pausada") + ".",
        redirect_to=request.headers.get("referer") or "/units",
    )


@router.post("/units/{unit_id}/delete")
def units_delete(
    request: Request,
    unit: Unit = Depends(_active_unit),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    processing = count_rows(
        db,
        Order,
        Order.unit_id == unit.id,
        Order.archived_at.is_(None),
        or_(
            Order.status.in_(ACTIVE_ORDER_STATUSES),
            Order.prior_status.in_(ACTIVE_PRIOR_STATUSES),
        ),
    )
    if processing:
        flash(
            request,
            "Aguarde os pedidos em processamento terminarem antes de arquivar.",
            "err",
        )
        return redirect("/units")
    unit_name = unit.name
    archived_at = datetime.now()
    archive_unit(
        unit,
        actor_id=user.id,
        actor_username=user.username,
        archived_at=archived_at,
    )
    archived_orders = db.execute(
        update(Order)
        .where(Order.unit_id == unit.id, Order.archived_at.is_(None))
        .values(
            archived_at=archived_at,
            archive_reason="Unidade arquivada pelo administrador.",
            archived_by_user_id=user.id,
            archived_by_username=user.username,
        )
    ).rowcount
    return commit_action(
        db,
        request,
        user,
        action="archive",
        resource_type="unit",
        resource_id=unit.id,
        resource_name=unit_name,
        summary=(
            "Unidade arquivada; "
            + counted(
                archived_orders or 0,
                "pedido foi movido para o histórico.",
                "pedidos foram movidos para o histórico.",
            )
        ),
        notice="Unidade arquivada com seus pedidos preservados.",
        redirect_to="/units",
    )


@router.post("/units/{unit_id}/retry-errors")
def units_retry_errors(
    unit_id: int,
    request: Request,
    unit: Unit = Depends(_active_unit),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    src = Path(unit.error_dir)
    dest = Path(unit.receive_dir)
    dest.mkdir(parents=True, exist_ok=True)
    moved = 0
    if src.is_dir():
        for f in src.iterdir():
            if f.is_file():
                shutil.move(str(f), str(dest / f.name))
                moved += 1
    return commit_action(
        db,
        request,
        user,
        action="retry",
        resource_type="unit",
        resource_id=unit.id,
        resource_name=unit.name,
        summary=counted(
            moved,
            "arquivo devolvido à fila de recebimento.",
            "arquivos devolvidos à fila de recebimento.",
        ),
        notice=counted(
            moved,
            "arquivo devolvido ao recebimento.",
            "arquivos devolvidos ao recebimento.",
        ),
        redirect_to=f"/units/{unit_id}",
    )


@router.post("/units/{unit_id}/resend-failed")
def units_resend_failed(
    unit_id: int,
    request: Request,
    unit: Unit = Depends(_active_unit),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    count = resend_failed_transfers(db, ImageTransfer.unit_id == unit.id)
    return commit_action(
        db,
        request,
        user,
        action="resend",
        resource_type="unit",
        resource_id=unit.id,
        resource_name=unit.name,
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
        redirect_to=f"/units/{unit_id}",
    )
