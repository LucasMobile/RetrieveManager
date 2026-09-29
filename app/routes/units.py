"""Unit registration, receiver/PACS settings and unit actions."""

from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from app.compression import (
    compression_form_for_unit,
    compression_modalities_for_form,
    legacy_unit_compression_form,
    save_unit_compression_settings,
    validate_unit_compression_form,
)
from app.config import (
    DEFAULT_CLOUD_URL,
    SEND_MAX_ATTEMPTS,
)
from app.db import get_db
from app.dicom_net import PacsNode, echo
from app.dicom_rules import (
    migrate_legacy_study_rule,
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
from app.validation import (
    validate_pacs_connection,
    validate_unit_form,
)
from app.web import UNITS_PAGE, audit, ctx, flash, log, require_admin, templates

router = APIRouter()


@router.get("/units", response_class=HTMLResponse)
def units_list(
    request: Request,
    page: int = 1,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    total = (
        db.scalar(
            select(func.count()).select_from(Unit).where(Unit.deleted_at.is_(None))
        )
        or 0
    )
    enabled = (
        db.scalar(
            select(func.count())
            .select_from(Unit)
            .where(Unit.enabled.is_(True), Unit.deleted_at.is_(None))
        )
        or 0
    )
    pager = paginate(total, page, UNITS_PAGE)
    units = list(
        db.scalars(
            select(Unit)
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
    compression_settings = legacy_unit_compression_form(db)
    return templates.TemplateResponse(
        request=request,
        name="units_form.html",
        context=ctx(
            request,
            db,
            "units",
            unit=None,
            default_cloud_url=DEFAULT_CLOUD_URL,
            compression_settings=compression_settings,
            compression_modalities=compression_modalities_for_form(
                compression_settings
            ),
            send_max_attempts=SEND_MAX_ATTEMPTS,
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
    obj.move_timeout_second = int(form["move_timeout_second"])
    obj.max_parallel_moves = int(form["max_parallel_moves"])
    obj.find_interval_seconds = int(form["find_interval_seconds"])
    obj.compact_workers = int(form["compact_workers"])
    obj.send_workers = int(form["send_workers"])
    return obj


def _port_taken(db: Session, port: int, unit_id: int | None) -> bool:
    q = select(Unit).where(
        Unit.store_port == port,
        Unit.deleted_at.is_(None),
    )
    if unit_id:
        q = q.where(Unit.id != unit_id)
    return db.scalar(q) is not None


@router.post("/units/new")
async def units_create(
    request: Request, db: Session = Depends(get_db), user: User = Depends(require_admin)
):
    raw_form = {k: v for k, v in (await request.form()).items() if isinstance(v, str)}
    try:
        form = validate_unit_form(raw_form, creating=True)
        compression_settings = validate_unit_compression_form(raw_form)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return RedirectResponse("/units/new", status_code=303)
    if _port_taken(db, int(form["store_port"]), None):
        flash(request, "Essa porta de store já está em uso.", "err")
        return RedirectResponse("/units/new", status_code=303)
    unit = _unit_from_form(form, None)
    db.add(unit)
    try:
        db.flush()
        save_unit_compression_settings(db, unit, compression_settings)
        migrate_legacy_study_rule(db)
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
        return RedirectResponse("/units/new", status_code=303)
    flash(request, "Unidade criada. Cadastre o AET no PACS se ainda não existir.")
    return RedirectResponse("/units", status_code=303)


@router.post("/units/test-echo")
async def units_test_echo(
    request: Request,
    user: User = Depends(require_admin),
):
    raw_form = {k: v for k, v in (await request.form()).items() if isinstance(v, str)}
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
    unit_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    unit = db.get(Unit, unit_id)
    if unit is None or unit.deleted_at is not None:
        return RedirectResponse("/units", status_code=303)
    compression_settings = compression_form_for_unit(db, unit.id)
    return templates.TemplateResponse(
        request=request,
        name="units_form.html",
        context=ctx(
            request,
            db,
            "units",
            unit=unit,
            default_cloud_url=DEFAULT_CLOUD_URL,
            compression_settings=compression_settings,
            compression_modalities=compression_modalities_for_form(
                compression_settings
            ),
            send_max_attempts=SEND_MAX_ATTEMPTS,
        ),
    )


@router.post("/units/{unit_id}")
async def units_update(
    unit_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    unit = db.get(Unit, unit_id)
    if unit is None or unit.deleted_at is not None:
        return RedirectResponse("/units", status_code=303)
    raw_form = {k: v for k, v in (await request.form()).items() if isinstance(v, str)}
    try:
        form = validate_unit_form(raw_form)
        compression_settings = validate_unit_compression_form(raw_form)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return RedirectResponse(f"/units/{unit_id}", status_code=303)
    if _port_taken(db, int(form["store_port"]), unit_id):
        flash(request, "Essa porta de store já está em uso.", "err")
        return RedirectResponse(f"/units/{unit_id}", status_code=303)
    _unit_from_form(form, unit)
    try:
        save_unit_compression_settings(db, unit, compression_settings)
        audit(
            db,
            request,
            user,
            action="update",
            resource_type="unit",
            resource_id=unit.id,
            resource_name=unit.name,
            summary="Configuração da unidade atualizada.",
        )
        db.commit()
    except IntegrityError:
        db.rollback()
        flash(request, "Não foi possível salvar (nome duplicado?).", "err")
        return RedirectResponse(f"/units/{unit_id}", status_code=303)
    flash(request, "Unidade atualizada.")
    return RedirectResponse(f"/units/{unit_id}", status_code=303)


@router.post("/units/{unit_id}/toggle")
def units_toggle(
    unit_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    unit = db.get(Unit, unit_id)
    if unit and unit.deleted_at is None:
        unit.enabled = not unit.enabled
        audit(
            db,
            request,
            user,
            action="enable" if unit.enabled else "disable",
            resource_type="unit",
            resource_id=unit.id,
            resource_name=unit.name,
            summary="Unidade ativada." if unit.enabled else "Unidade pausada.",
        )
        db.commit()
        flash(request, "Unidade " + ("ativada" if unit.enabled else "pausada") + ".")
    return RedirectResponse(request.headers.get("referer") or "/units", status_code=303)


@router.post("/units/{unit_id}/delete")
def units_delete(
    unit_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    unit = db.get(Unit, unit_id)
    if unit and unit.deleted_at is None:
        processing = db.scalar(
            select(func.count()).where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                or_(
                    Order.status.in_(ACTIVE_ORDER_STATUSES),
                    Order.prior_status.in_(ACTIVE_PRIOR_STATUSES),
                ),
            )
        )
        if processing:
            flash(
                request,
                "Aguarde os pedidos em processamento terminarem antes de arquivar.",
                "err",
            )
            return RedirectResponse("/units", status_code=303)
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
        audit(
            db,
            request,
            user,
            action="archive",
            resource_type="unit",
            resource_id=unit_id,
            resource_name=unit_name,
            summary=(
                f"Unidade arquivada; {archived_orders or 0} pedido(s) "
                "foram movidos para o histórico."
            ),
        )
        db.commit()
        flash(request, "Unidade arquivada com seus pedidos preservados.")
    return RedirectResponse("/units", status_code=303)


@router.post("/units/{unit_id}/retry-errors")
def units_retry_errors(
    unit_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    unit = db.get(Unit, unit_id)
    if unit is None or unit.deleted_at is not None:
        return RedirectResponse("/units", status_code=303)
    src = Path(unit.error_dir)
    dest = Path(unit.receive_dir)
    dest.mkdir(parents=True, exist_ok=True)
    moved = 0
    if src.is_dir():
        for f in src.iterdir():
            if f.is_file():
                shutil.move(str(f), str(dest / f.name))
                moved += 1
    audit(
        db,
        request,
        user,
        action="retry",
        resource_type="unit",
        resource_id=unit.id,
        resource_name=unit.name,
        summary=f"{moved} arquivo(s) devolvidos à fila de recebimento.",
    )
    db.commit()
    flash(request, f"{moved} arquivo(s) devolvidos ao recebimento.")
    return RedirectResponse(f"/units/{unit_id}", status_code=303)


@router.post("/units/{unit_id}/resend-failed")
def units_resend_failed(
    unit_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    unit = db.get(Unit, unit_id)
    if unit is None or unit.deleted_at is not None:
        return RedirectResponse("/units", status_code=303)
    count = resend_failed_transfers(db, ImageTransfer.unit_id == unit.id)
    audit(
        db,
        request,
        user,
        action="resend",
        resource_type="unit",
        resource_id=unit.id,
        resource_name=unit.name,
        summary=f"{count} imagem(ns) com falha de envio devolvidas à fila.",
    )
    db.commit()
    flash(request, f"{count} imagem(ns) devolvidas à fila de envio.")
    return RedirectResponse(f"/units/{unit_id}", status_code=303)
