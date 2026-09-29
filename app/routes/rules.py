"""DICOM, retrieve-time and compression/drop rules."""

from __future__ import annotations

import logging
from typing import Any, Literal

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.db import get_db
from app.dicom_rules import (
    ACTIONS,
    COMBINATORS,
    OPERATORS,
    VALUELESS_OPERATORS,
    dicom_tag_name,
    modifies_protected_tag,
    normalize_dicom_tag,
    validate_rule_payload,
)
from app.models import (
    CompressRule,
    DicomRule,
    DicomRuleApplication,
    DicomRuleCondition,
    DicomRuleUnit,
    DropModality,
    ModalityRule,
    Unit,
    User,
)
from app.observability import log_event
from app.validation import (
    validate_jpeg_flag,
    validate_modality,
)
from app.web import audit, ctx, flash, log, require_admin, templates

router = APIRouter()


def _dicom_rule_views(db: Session) -> list[DicomRule]:
    rules = list(
        db.scalars(
            select(DicomRule)
            .options(
                selectinload(DicomRule.conditions),
                selectinload(DicomRule.unit_links).selectinload(DicomRuleUnit.unit),
            )
            .order_by(DicomRule.priority, DicomRule.id)
        )
    )
    application_counts = dict(
        db.execute(
            select(DicomRuleApplication.rule_id, func.count())
            .where(DicomRuleApplication.rule_id.is_not(None))
            .group_by(DicomRuleApplication.rule_id)
        ).all()
    )
    for rule in rules:
        rule.unit_names = [  # type: ignore[attr-defined]
            link.unit.name for link in rule.unit_links if link.unit is not None
        ]
        rule.action_label = ACTIONS.get(rule.action, rule.action)  # type: ignore[attr-defined]
        rule.combinator_label = COMBINATORS.get(  # type: ignore[attr-defined]
            rule.combinator, rule.combinator
        )
        rule.action_tag_name = (  # type: ignore[attr-defined]
            dicom_tag_name(rule.action_tag) if rule.action_tag else ""
        )
        rule.protected_action = modifies_protected_tag(  # type: ignore[attr-defined]
            rule.action, rule.action_tag
        )
        rule.application_count = int(  # type: ignore[attr-defined]
            application_counts.get(rule.id, 0)
        )
        for condition in rule.conditions:
            condition.tag_name = dicom_tag_name(  # type: ignore[attr-defined]
                condition.tag
            )
            condition.operator_label = OPERATORS.get(  # type: ignore[attr-defined]
                condition.operator, condition.operator
            )
    return rules


@router.get("/rules", response_class=HTMLResponse)
def rules_dicom(
    request: Request,
    edit: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    rules = _dicom_rule_views(db)
    units = list(
        db.scalars(select(Unit).where(Unit.deleted_at.is_(None)).order_by(Unit.name))
    )
    edit_rule = next((rule for rule in rules if rule.id == edit), None)
    return templates.TemplateResponse(
        request=request,
        name="rules_dicom.html",
        context=ctx(
            request,
            db,
            "rules",
            rules=rules,
            units=units,
            edit_rule=edit_rule,
            operators=OPERATORS,
            combinators=COMBINATORS,
            actions=ACTIONS,
            valueless_operators=VALUELESS_OPERATORS,
            summary={
                "total": len(rules),
                "active": sum(rule.enabled for rule in rules),
                "units": len(
                    {
                        link.unit_id
                        for rule in rules
                        if rule.enabled
                        for link in rule.unit_links
                    }
                ),
            },
        ),
    )


@router.get("/rules/tag-info")
def rules_dicom_tag_info(
    tag: str = Query(..., min_length=1, max_length=32),
    user: User = Depends(require_admin),
):
    try:
        normalized = normalize_dicom_tag(tag)
        return {"ok": True, "tag": normalized, "name": dicom_tag_name(normalized)}
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)


def _validated_dicom_rule_form(db: Session, form: Any) -> dict[str, Any]:
    valid_unit_ids = set(db.scalars(select(Unit.id).where(Unit.deleted_at.is_(None))))
    return validate_rule_payload(
        name=str(form.get("name") or ""),
        enabled=form.get("enabled") == "1",
        priority=str(form.get("priority") or ""),
        combinator=str(form.get("combinator") or ""),
        action=str(form.get("action") or ""),
        action_tag=str(form.get("action_tag") or ""),
        action_value=str(form.get("action_value") or ""),
        unit_ids=[str(value) for value in form.getlist("unit_ids")],
        condition_tags=[str(value) for value in form.getlist("condition_tag")],
        condition_operators=[
            str(value) for value in form.getlist("condition_operator")
        ],
        condition_values=[str(value) for value in form.getlist("condition_value")],
        valid_unit_ids=valid_unit_ids,
    )


def _store_dicom_rule(db: Session, rule: DicomRule, data: dict[str, Any]) -> None:
    rule.name = data["name"]
    rule.enabled = data["enabled"]
    rule.priority = data["priority"]
    rule.combinator = data["combinator"]
    rule.action = data["action"]
    rule.action_tag = data["action_tag"]
    rule.action_value = data["action_value"]
    db.add(rule)
    db.flush()
    db.execute(delete(DicomRuleCondition).where(DicomRuleCondition.rule_id == rule.id))
    db.execute(delete(DicomRuleUnit).where(DicomRuleUnit.rule_id == rule.id))
    db.flush()
    db.add_all(
        DicomRuleCondition(
            rule_id=rule.id,
            position=position,
            tag=condition["tag"],
            operator=condition["operator"],
            value=condition["value"],
        )
        for position, condition in enumerate(data["conditions"])
    )
    db.add_all(
        DicomRuleUnit(rule_id=rule.id, unit_id=unit_id) for unit_id in data["unit_ids"]
    )


@router.post("/rules")
async def rules_dicom_create(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    try:
        data = _validated_dicom_rule_form(db, await request.form())
        rule = DicomRule()
        _store_dicom_rule(db, rule, data)
        audit(
            db,
            request,
            user,
            action="create",
            resource_type="dicom_rule",
            resource_id=rule.id,
            resource_name=rule.name,
            summary=f"Regra criada com {len(data['conditions'])} condição(ões).",
        )
        db.commit()
    except ValueError as exc:
        db.rollback()
        flash(request, str(exc), "err")
        return RedirectResponse("/rules", status_code=303)
    log_event(
        log,
        logging.INFO,
        "dicom.rule.create",
        resource=f"dicom-rule:{rule.id}",
        status="success",
        rule_id=rule.id,
        user_id=user.id,
    )
    flash(request, "Regra DICOM criada.")
    return RedirectResponse("/rules", status_code=303)


@router.post("/rules/dicom/{rule_id}")
async def rules_dicom_update(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    rule = db.get(DicomRule, rule_id)
    if rule is None:
        flash(request, "Regra não encontrada.", "err")
        return RedirectResponse("/rules", status_code=303)
    try:
        data = _validated_dicom_rule_form(db, await request.form())
        _store_dicom_rule(db, rule, data)
        audit(
            db,
            request,
            user,
            action="update",
            resource_type="dicom_rule",
            resource_id=rule.id,
            resource_name=rule.name,
            summary=f"Regra atualizada com {len(data['conditions'])} condição(ões).",
        )
        db.commit()
    except ValueError as exc:
        db.rollback()
        flash(request, str(exc), "err")
        return RedirectResponse(f"/rules?edit={rule_id}", status_code=303)
    log_event(
        log,
        logging.INFO,
        "dicom.rule.update",
        resource=f"dicom-rule:{rule.id}",
        status="success",
        rule_id=rule.id,
        user_id=user.id,
    )
    flash(request, "Regra DICOM atualizada.")
    return RedirectResponse("/rules", status_code=303)


@router.post("/rules/dicom/{rule_id}/toggle")
def rules_dicom_toggle(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    rule = db.get(DicomRule, rule_id)
    if rule is not None:
        rule.enabled = not rule.enabled
        audit(
            db,
            request,
            user,
            action="enable" if rule.enabled else "disable",
            resource_type="dicom_rule",
            resource_id=rule.id,
            resource_name=rule.name,
            summary="Regra ativada." if rule.enabled else "Regra desativada.",
        )
        db.commit()
        flash(request, "Regra ativada." if rule.enabled else "Regra desativada.")
    return RedirectResponse("/rules", status_code=303)


@router.post("/rules/dicom/{rule_id}/delete")
def rules_dicom_delete(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    rule = db.get(DicomRule, rule_id)
    if rule is not None:
        rule_name = rule.name
        db.execute(
            update(DicomRuleApplication)
            .where(DicomRuleApplication.rule_id == rule.id)
            .values(rule_id=None)
        )
        db.delete(rule)
        audit(
            db,
            request,
            user,
            action="delete",
            resource_type="dicom_rule",
            resource_id=rule_id,
            resource_name=rule_name,
            summary="Regra DICOM removida.",
        )
        db.commit()
        flash(request, "Regra DICOM excluída.")
    return RedirectResponse("/rules", status_code=303)


@router.get("/rules/retrieve", response_class=HTMLResponse)
def rules_retrieve(
    request: Request, db: Session = Depends(get_db), user: User = Depends(require_admin)
):
    rules = list(db.scalars(select(ModalityRule).order_by(ModalityRule.modality)))
    default_rule = next((rule for rule in rules if rule.modality == "*"), None)
    return templates.TemplateResponse(
        request=request,
        name="rules_retrieve.html",
        context=ctx(
            request,
            db,
            "retrieve",
            rules=rules,
            summary={
                "total": len(rules),
                "second_enabled": sum(rule.second_retrieve for rule in rules),
                "default_wait": default_rule.wait_minutes if default_rule else None,
            },
        ),
    )


@router.post("/rules/retrieve")
def rules_retrieve_add(
    request: Request,
    modality: str = Form(...),
    wait_minutes: int = Form(..., ge=0, le=1440),
    second_retrieve: Literal["0", "1"] = Form("0"),
    second_wait_minutes: int = Form(90, ge=0, le=2880),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    try:
        rule = ModalityRule()
        _apply_retrieve_rule(
            rule,
            modality=modality,
            wait_minutes=wait_minutes,
            second_retrieve=second_retrieve,
            second_wait_minutes=second_wait_minutes,
        )
    except ValueError as exc:
        flash(request, str(exc), "err")
        return RedirectResponse("/rules/retrieve", status_code=303)
    db.add(rule)
    try:
        db.flush()
        audit(
            db,
            request,
            user,
            action="create",
            resource_type="retrieve_rule",
            resource_id=rule.id,
            resource_name=rule.modality,
            summary="Regra de tempo de retrieve adicionada.",
        )
        db.commit()
        flash(request, "Regra adicionada.")
    except IntegrityError:
        db.rollback()
        flash(request, "Modalidade já existe.", "err")
    return RedirectResponse("/rules/retrieve", status_code=303)


@router.post("/rules/retrieve/{rule_id}")
def rules_retrieve_update(
    rule_id: int,
    request: Request,
    modality: str = Form(...),
    wait_minutes: int = Form(..., ge=0, le=1440),
    second_retrieve: Literal["0", "1"] = Form("0"),
    second_wait_minutes: int = Form(90, ge=0, le=2880),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    rule = db.get(ModalityRule, rule_id)
    if rule:
        try:
            _apply_retrieve_rule(
                rule,
                modality=modality,
                wait_minutes=wait_minutes,
                second_retrieve=second_retrieve,
                second_wait_minutes=second_wait_minutes,
            )
        except ValueError as exc:
            flash(request, str(exc), "err")
            return RedirectResponse("/rules/retrieve", status_code=303)
        try:
            audit(
                db,
                request,
                user,
                action="update",
                resource_type="retrieve_rule",
                resource_id=rule.id,
                resource_name=rule.modality,
                summary="Tempos de retrieve atualizados.",
            )
            db.commit()
            flash(request, "Regra salva.")
        except IntegrityError:
            db.rollback()
            flash(request, "Modalidade já existe.", "err")
    return RedirectResponse("/rules/retrieve", status_code=303)


def _apply_retrieve_rule(
    rule: ModalityRule,
    *,
    modality: str,
    wait_minutes: int,
    second_retrieve: Literal["0", "1"],
    second_wait_minutes: int,
) -> None:
    safe_modality = validate_modality(modality)
    if rule.modality != "*":
        rule.modality = safe_modality
    rule.wait_minutes = wait_minutes
    rule.second_retrieve = second_retrieve == "1"
    if rule.second_retrieve or rule.id is None:
        rule.second_wait_minutes = second_wait_minutes


@router.post("/rules/retrieve/{rule_id}/delete")
def rules_retrieve_delete(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    rule = db.get(ModalityRule, rule_id)
    if rule is None:
        flash(request, "Regra não encontrada.", "err")
        return RedirectResponse("/rules/retrieve", status_code=303)
    if rule.modality == "*":
        flash(request, "A regra padrão não pode ser excluída.", "err")
        return RedirectResponse("/rules/retrieve", status_code=303)

    modality = rule.modality
    db.delete(rule)
    audit(
        db,
        request,
        user,
        action="delete",
        resource_type="retrieve_rule",
        resource_id=rule_id,
        resource_name=modality,
        summary="Regra de tempo de retrieve removida.",
    )
    db.commit()
    log_event(
        log,
        logging.INFO,
        "retrieve_rule.delete",
        resource=f"retrieve-rule:{rule_id}",
        status="success",
        user_id=user.id,
        modality=modality,
    )
    flash(request, f"Regra de {modality} excluída.")
    return RedirectResponse("/rules/retrieve", status_code=303)


@router.get("/rules/compress")
def rules_compress(user: User = Depends(require_admin)):
    return RedirectResponse("/units", status_code=303)


@router.post("/rules/compress")
def rules_compress_add(
    request: Request,
    modality: str = Form(...),
    jpeg_flag: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    try:
        rule = CompressRule()
        _apply_compress_rule(rule, modality=modality, jpeg_flag=jpeg_flag)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return RedirectResponse("/rules/compress", status_code=303)
    db.add(rule)
    try:
        db.flush()
        audit(
            db,
            request,
            user,
            action="create",
            resource_type="compress_rule",
            resource_id=rule.id,
            resource_name=rule.modality,
            summary=f"Perfil de compactação {rule.jpeg_flag} adicionado.",
        )
        db.commit()
        flash(request, "Perfil JPEG adicionado.")
    except IntegrityError:
        db.rollback()
        flash(request, "Modalidade já existe.", "err")
    return RedirectResponse("/rules/compress", status_code=303)


@router.post("/rules/compress/{rule_id}")
def rules_compress_update(
    rule_id: int,
    request: Request,
    modality: str = Form(...),
    jpeg_flag: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    rule = db.get(CompressRule, rule_id)
    if rule:
        try:
            _apply_compress_rule(rule, modality=modality, jpeg_flag=jpeg_flag)
        except ValueError as exc:
            flash(request, str(exc), "err")
            return RedirectResponse("/rules/compress", status_code=303)
        try:
            audit(
                db,
                request,
                user,
                action="update",
                resource_type="compress_rule",
                resource_id=rule.id,
                resource_name=rule.modality,
                summary=f"Perfil de compactação alterado para {rule.jpeg_flag}.",
            )
            db.commit()
            flash(request, "Perfil salvo.")
        except IntegrityError:
            db.rollback()
            flash(request, "Modalidade já existe.", "err")
    return RedirectResponse("/rules/compress", status_code=303)


def _apply_compress_rule(rule: CompressRule, *, modality: str, jpeg_flag: str) -> None:
    safe_modality = validate_modality(modality)
    if rule.modality != "*":
        rule.modality = safe_modality
    rule.jpeg_flag = validate_jpeg_flag(jpeg_flag)


@router.post("/rules/compress/{rule_id}/delete")
def rules_compress_delete(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    rule = db.get(CompressRule, rule_id)
    if rule is None:
        log_event(
            log,
            logging.WARNING,
            "compression_rule.delete",
            resource=f"compression-rule:{rule_id}",
            status="failure",
            user_id=user.id,
            error_type="NotFound",
        )
        flash(request, "Regra de compactação não encontrada.", "err")
        return RedirectResponse("/rules/compress", status_code=303)
    if rule.modality == "*":
        log_event(
            log,
            logging.WARNING,
            "compression_rule.delete",
            resource=f"compression-rule:{rule_id}",
            status="failure",
            user_id=user.id,
            modality=rule.modality,
            error_type="ProtectedDefaultRule",
        )
        flash(request, "A regra padrão de compactação não pode ser excluída.", "err")
        return RedirectResponse("/rules/compress", status_code=303)

    modality = rule.modality
    jpeg_flag = rule.jpeg_flag
    db.delete(rule)
    audit(
        db,
        request,
        user,
        action="delete",
        resource_type="compress_rule",
        resource_id=rule_id,
        resource_name=modality,
        summary=f"Perfil de compactação {jpeg_flag} removido.",
    )
    db.commit()
    log_event(
        log,
        logging.INFO,
        "compression_rule.delete",
        resource=f"compression-rule:{rule_id}",
        status="success",
        user_id=user.id,
        modality=modality,
        jpeg_flag=jpeg_flag,
    )
    flash(request, f"Regra de compactação de {modality} excluída.")
    return RedirectResponse("/rules/compress", status_code=303)


@router.post("/rules/drop")
def rules_drop_add(
    request: Request,
    code: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    try:
        safe_code = validate_modality(code)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return RedirectResponse("/rules/compress", status_code=303)
    if safe_code == "*":
        flash(request, "O descarte não aceita modalidade curinga.", "err")
        return RedirectResponse("/rules/compress", status_code=303)
    row = DropModality(code=safe_code)
    db.add(row)
    try:
        db.flush()
        audit(
            db,
            request,
            user,
            action="create",
            resource_type="drop_rule",
            resource_id=row.id,
            resource_name=row.code,
            summary="Modalidade adicionada à lista de descarte.",
        )
        db.commit()
        flash(request, "Modalidade adicionada ao descarte.")
    except IntegrityError:
        db.rollback()
        flash(request, "Modalidade já existe no descarte.", "err")
    return RedirectResponse("/rules/compress", status_code=303)


@router.post("/rules/drop/{drop_id}/delete")
def rules_drop_delete(
    drop_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    row = db.get(DropModality, drop_id)
    if row:
        code = row.code
        db.delete(row)
        audit(
            db,
            request,
            user,
            action="delete",
            resource_type="drop_rule",
            resource_id=drop_id,
            resource_name=code,
            summary="Modalidade removida da lista de descarte.",
        )
        db.commit()
        flash(request, "Modalidade removida da lista de descarte.")
    return RedirectResponse("/rules/compress", status_code=303)
