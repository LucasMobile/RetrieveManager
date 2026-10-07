"""DICOM, retrieve-time and compression/drop rules."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.audit import audit
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
    DicomRule,
    DicomRuleApplication,
    DicomRuleCondition,
    DicomRuleUnit,
    ModalityRule,
    Unit,
    User,
)
from app.validation import (
    validate_modality,
)
from app.web import (
    FlashRedirect,
    commit_action,
    ctx,
    flash,
    redirect,
    require_admin,
    templates,
)
from app.wording import counted

router = APIRouter()


def _dicom_rule(
    rule_id: int,
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
) -> DicomRule:
    rule = db.get(DicomRule, rule_id)
    if rule is None:
        raise FlashRedirect("/rules", "Regra não encontrada.")
    return rule


def _retrieve_rule(
    rule_id: int,
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
) -> ModalityRule:
    rule = db.get(ModalityRule, rule_id)
    if rule is None:
        raise FlashRedirect("/rules/retrieve", "Regra não encontrada.")
    return rule


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
    except ValueError as exc:
        db.rollback()
        flash(request, str(exc), "err")
        return redirect("/rules")
    return commit_action(
        db,
        request,
        user,
        action="create",
        resource_type="dicom_rule",
        resource_id=rule.id,
        resource_name=rule.name,
        summary="Regra criada com "
        + counted(len(data["conditions"]), "condição.", "condições."),
        notice="Regra DICOM criada.",
        redirect_to="/rules",
        event="dicom.rule.create",
        rule_id=rule.id,
    )


@router.post("/rules/dicom/{rule_id}")
async def rules_dicom_update(
    rule_id: int,
    request: Request,
    rule: DicomRule = Depends(_dicom_rule),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    try:
        data = _validated_dicom_rule_form(db, await request.form())
        _store_dicom_rule(db, rule, data)
    except ValueError as exc:
        db.rollback()
        flash(request, str(exc), "err")
        return redirect(f"/rules?edit={rule_id}")
    return commit_action(
        db,
        request,
        user,
        action="update",
        resource_type="dicom_rule",
        resource_id=rule.id,
        resource_name=rule.name,
        summary="Regra atualizada com "
        + counted(len(data["conditions"]), "condição.", "condições."),
        notice="Regra DICOM atualizada.",
        redirect_to="/rules",
        event="dicom.rule.update",
        rule_id=rule.id,
    )


@router.post("/rules/dicom/{rule_id}/toggle")
def rules_dicom_toggle(
    rule_id: int,
    request: Request,
    rule: DicomRule = Depends(_dicom_rule),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    rule.enabled = not rule.enabled
    return commit_action(
        db,
        request,
        user,
        action="enable" if rule.enabled else "disable",
        resource_type="dicom_rule",
        resource_id=rule.id,
        resource_name=rule.name,
        summary="Regra ativada." if rule.enabled else "Regra desativada.",
        notice="Regra ativada." if rule.enabled else "Regra desativada.",
        redirect_to="/rules",
    )


@router.post("/rules/dicom/{rule_id}/delete")
def rules_dicom_delete(
    rule_id: int,
    request: Request,
    rule: DicomRule = Depends(_dicom_rule),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    rule_name = rule.name
    db.execute(
        update(DicomRuleApplication)
        .where(DicomRuleApplication.rule_id == rule.id)
        .values(rule_id=None)
    )
    db.delete(rule)
    return commit_action(
        db,
        request,
        user,
        action="delete",
        resource_type="dicom_rule",
        resource_id=rule_id,
        resource_name=rule_name,
        summary="Regra DICOM removida.",
        notice="Regra DICOM excluída.",
        redirect_to="/rules",
    )


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
                "monitor_enabled": sum(rule.monitor_enabled for rule in rules),
                "default_wait": default_rule.wait_minutes if default_rule else None,
            },
        ),
    )


class RetrieveRuleForm(BaseModel):
    """Fields shared by the create and edit forms of a retrieve-time rule."""

    modality: str
    wait_minutes: int = Field(ge=0, le=1440)
    monitor_enabled: Literal["0", "1"] = "0"
    monitor_interval_minutes: int = Field(5, ge=1, le=240)
    monitor_max_hours: int = Field(6, ge=1, le=72)


@router.post("/rules/retrieve")
def rules_retrieve_add(
    request: Request,
    form: RetrieveRuleForm = Form(),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    try:
        rule = ModalityRule()
        _apply_retrieve_rule(rule, form)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return redirect("/rules/retrieve")
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
    return redirect("/rules/retrieve")


@router.post("/rules/retrieve/{rule_id}")
def rules_retrieve_update(
    request: Request,
    form: RetrieveRuleForm = Form(),
    rule: ModalityRule = Depends(_retrieve_rule),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    try:
        _apply_retrieve_rule(rule, form)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return redirect("/rules/retrieve")
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
    return redirect("/rules/retrieve")


def _apply_retrieve_rule(rule: ModalityRule, form: RetrieveRuleForm) -> None:
    safe_modality = validate_modality(form.modality)
    if rule.modality != "*":
        rule.modality = safe_modality
    if form.monitor_interval_minutes > form.monitor_max_hours * 60:
        raise ValueError(
            "O intervalo entre as consultas não pode ser maior que o tempo máximo."
        )
    rule.wait_minutes = form.wait_minutes
    rule.monitor_enabled = form.monitor_enabled == "1"
    # Disabled fields are not posted: keep the stored values.
    if rule.monitor_enabled or rule.id is None:
        rule.monitor_interval_minutes = form.monitor_interval_minutes
        rule.monitor_max_hours = form.monitor_max_hours


@router.post("/rules/retrieve/{rule_id}/delete")
def rules_retrieve_delete(
    rule_id: int,
    request: Request,
    rule: ModalityRule = Depends(_retrieve_rule),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    if rule.modality == "*":
        flash(request, "A regra padrão não pode ser excluída.", "err")
        return redirect("/rules/retrieve")

    modality = rule.modality
    db.delete(rule)
    return commit_action(
        db,
        request,
        user,
        action="delete",
        resource_type="retrieve_rule",
        resource_id=rule_id,
        resource_name=modality,
        summary="Regra de tempo de retrieve removida.",
        notice=f"Regra de {modality} excluída.",
        redirect_to="/rules/retrieve",
        event="retrieve_rule.delete",
        modality=modality,
    )
