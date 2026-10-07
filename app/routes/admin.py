"""Users, audit logs and received instances needing attention."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.audit import audit
from app.db import count_rows, get_db
from app.instances import clear_issue_instances
from app.models import (
    AuditLog,
    DicomInstance,
    ImageTransfer,
    Unit,
    User,
)
from app.observability import log_event
from app.pager import keyset_page, paginate, query_keep
from app.security import (
    hash_password,
    revoke_sessions,
)
from app.web import (
    AUDIT_ACTION_BADGES,
    AUDIT_ACTION_LABELS,
    AUDIT_RESOURCE_LABELS,
    DEFAULT_PAGE_SIZE,
    INSTANCE_ISSUES,
    PAGE_SIZE_OPTIONS,
    USER_ROLES,
    FlashRedirect,
    commit_action,
    ctx,
    flash,
    log,
    normalize_username,
    redirect,
    require_admin,
    templates,
    validate_password,
)
from app.wording import counted

router = APIRouter()


def _managed_user(
    managed_user_id: int,
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
) -> User:
    managed_user = db.get(User, managed_user_id)
    if managed_user is None:
        raise FlashRedirect("/users")
    return managed_user


def _admin_count(db: Session) -> int:
    return count_rows(db, User, User.role == "admin")


@router.post("/instances/clear")
def instances_clear(
    request: Request,
    unit_id: int | None = Form(None, ge=1),
    state: str = Form("", max_length=16),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    """Delete the pending instances shown by the current filter, with their files."""
    if state and state not in INSTANCE_ISSUES:
        raise StarletteHTTPException(422, "Estado de instância inválido")
    unit = db.get(Unit, unit_id) if unit_id else None
    if unit_id and unit is None:
        raise StarletteHTTPException(422, "Unidade inválida")
    result = clear_issue_instances(
        db, [state] if state else list(INSTANCE_ISSUES), unit_id
    )
    scope = INSTANCE_ISSUES[state][0].lower() if state else "todas as pendências"
    kept = (
        " "
        + counted(
            result.files_kept,
            "arquivo não pôde ser excluído.",
            "arquivos não puderam ser excluídos.",
        )
        if result.files_kept
        else ""
    )
    return commit_action(
        db,
        request,
        user,
        action="delete",
        resource_type="instance",
        resource_id=unit_id or "all",
        resource_name=unit.name if unit else "Todas as unidades",
        summary=(
            f"Fila de instâncias limpa ({scope}): "
            + counted(result.instances, "registro excluído", "registros excluídos")
            + " e "
            + counted(result.files_removed, "arquivo excluído", "arquivos excluídos")
            + f".{kept}"
        ),
        notice=(
            counted(result.instances, "pendência removida", "pendências removidas")
            + " e "
            + counted(result.files_removed, "arquivo excluído", "arquivos excluídos")
            + f".{kept} "
            "Os objetos podem ser reenviados pelo PACS."
        ),
        redirect_to=f"/instances?{query_keep(unit_id=unit_id, state=state)}".rstrip(
            "?"
        ),
        event="dicom.instances.clear",
        unit_id=unit_id,
        state=state or None,
        instance_count=result.instances,
        files_removed=result.files_removed,
        files_kept=result.files_kept,
    )


@router.get("/instances", response_class=HTMLResponse)
def instances_page(
    request: Request,
    unit_id: int | None = Query(None, ge=1),
    state: str = Query("", max_length=16),
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    if state and state not in INSTANCE_ISSUES:
        raise StarletteHTTPException(422, "Estado de instância inválido")
    if page_size not in PAGE_SIZE_OPTIONS:
        raise StarletteHTTPException(422, "Quantidade de itens por página inválida")
    states = [state] if state else list(INSTANCE_ISSUES)
    scope = [DicomInstance.state.in_(states)]
    if unit_id:
        scope.append(DicomInstance.unit_id == unit_id)
    total = count_rows(db, DicomInstance, *scope)
    pager = paginate(total, page, page_size)
    pager["size_options"] = PAGE_SIZE_OPTIONS
    pager["keep"] = {"unit_id": unit_id, "state": state}
    rows = db.execute(
        select(DicomInstance, Unit.name, ImageTransfer.order_id)
        .join(Unit, Unit.id == DicomInstance.unit_id)
        .outerjoin(ImageTransfer, ImageTransfer.id == DicomInstance.transfer_id)
        .where(*scope)
        .order_by(DicomInstance.id.desc())
        .offset(pager["offset"])
        .limit(pager["size"])
    ).all()
    count_scope = [DicomInstance.state.in_(list(INSTANCE_ISSUES))]
    if unit_id:
        count_scope.append(DicomInstance.unit_id == unit_id)
    counts = dict(
        db.execute(
            select(DicomInstance.state, func.count())
            .where(*count_scope)
            .group_by(DicomInstance.state)
        ).all()
    )
    units = list(
        db.execute(
            select(Unit.id, Unit.name)
            .where(Unit.deleted_at.is_(None))
            .order_by(Unit.name)
        ).all()
    )
    return templates.TemplateResponse(
        request=request,
        name="instances.html",
        context=ctx(
            request,
            db,
            "instances",
            rows=[
                {
                    "instance": instance,
                    "unit_name": unit_name,
                    "order_id": order_id,
                    "file_name": Path(instance.source_path).name,
                }
                for instance, unit_name, order_id in rows
            ],
            issues=INSTANCE_ISSUES,
            counts={key: int(counts.get(key, 0)) for key in INSTANCE_ISSUES},
            units=units,
            unit_id=unit_id,
            state=state,
            pager=pager,
            qs=query_keep(unit_id=unit_id, state=state, page_size=page_size),
        ),
    )


@router.get("/logs", response_class=HTMLResponse)
def audit_logs(
    request: Request,
    action: str = Query("", max_length=32),
    resource: str = Query("", max_length=32),
    q: str = Query("", max_length=120),
    before: int | None = Query(None, ge=1),
    after: int | None = Query(None, ge=1),
    last: bool = False,
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    if action and action not in AUDIT_ACTION_LABELS:
        raise StarletteHTTPException(422, "Ação de auditoria inválida")
    if resource and resource not in AUDIT_RESOURCE_LABELS:
        raise StarletteHTTPException(422, "Tipo de recurso inválido")

    filtered = select(AuditLog)
    if action:
        filtered = filtered.where(AuditLog.action == action)
    if resource:
        filtered = filtered.where(AuditLog.resource_type == resource)
    search = q.strip()
    if search:
        like = f"%{search}%"
        filtered = filtered.where(
            or_(
                AuditLog.actor_username.like(like),
                AuditLog.resource_name.like(like),
                AuditLog.resource_id.like(like),
                AuditLog.summary.like(like),
            )
        )

    entries, pager = keyset_page(
        db,
        filtered,
        AuditLog.id,
        page=page,
        page_size=page_size,
        size_options=PAGE_SIZE_OPTIONS,
        before=before,
        after=after,
        last=last,
        keep={"action": action, "resource": resource, "q": search},
    )
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    summary = {
        "total": count_rows(db, AuditLog),
        "today": count_rows(db, AuditLog, AuditLog.created_at >= today),
        "actors": int(
            db.scalar(select(func.count(func.distinct(AuditLog.actor_username)))) or 0
        ),
    }
    return templates.TemplateResponse(
        request=request,
        name="logs.html",
        context=ctx(
            request,
            db,
            "logs",
            entries=entries,
            summary=summary,
            action=action,
            resource=resource,
            q=search,
            action_labels=AUDIT_ACTION_LABELS,
            resource_labels=AUDIT_RESOURCE_LABELS,
            action_badges=AUDIT_ACTION_BADGES,
            pager=pager,
            qs=query_keep(
                action=action,
                resource=resource,
                q=search,
                page_size=page_size,
            ),
        ),
    )


@router.get("/users", response_class=HTMLResponse)
def users_list(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    users = list(db.scalars(select(User).order_by(User.username)))
    admin_count = sum(item.role == "admin" for item in users)
    return templates.TemplateResponse(
        request=request,
        name="users.html",
        context=ctx(
            request,
            db,
            "users",
            users=users,
            summary={
                "total": len(users),
                "admins": admin_count,
                "regular": len(users) - admin_count,
            },
        ),
    )


@router.get("/users/new", response_class=HTMLResponse)
def users_new(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    return templates.TemplateResponse(
        request=request,
        name="user_form.html",
        context=ctx(request, db, "users", managed_user=None),
    )


@router.post("/users/new")
def users_create(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    password_confirmation: str = Form(...),
    role: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    try:
        clean_username = normalize_username(username)
        validate_password(password, password_confirmation)
        if role not in USER_ROLES:
            raise ValueError("Perfil de acesso inválido.")
    except ValueError as exc:
        flash(request, str(exc), "err")
        return redirect("/users/new")
    if db.scalar(select(User).where(User.username == clean_username)) is not None:
        flash(request, "Já existe um usuário com esse nome.", "err")
        return redirect("/users/new")
    managed_user = User(
        username=clean_username,
        password_hash=hash_password(password),
        role=role,
    )
    db.add(managed_user)
    try:
        db.flush()
        audit(
            db,
            request,
            user,
            action="create",
            resource_type="user",
            resource_id=managed_user.id,
            resource_name=managed_user.username,
            summary=f"Usuário criado com o perfil {managed_user.role}.",
        )
        db.commit()
    except IntegrityError:
        db.rollback()
        flash(request, "Já existe um usuário com esse nome.", "err")
        return redirect("/users/new")
    log_event(
        log,
        logging.INFO,
        "user.create",
        resource=f"user:{managed_user.id}",
        status="success",
        user_id=user.id,
        managed_user_id=managed_user.id,
        managed_user_role=managed_user.role,
    )
    flash(request, "Usuário criado.")
    return redirect("/users")


@router.get("/users/{managed_user_id}", response_class=HTMLResponse)
def users_edit(
    managed_user_id: int,
    request: Request,
    managed_user: User = Depends(_managed_user),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    return templates.TemplateResponse(
        request=request,
        name="user_form.html",
        context=ctx(request, db, "users", managed_user=managed_user),
    )


@router.post("/users/{managed_user_id}")
def users_update(
    managed_user_id: int,
    request: Request,
    managed_user: User = Depends(_managed_user),
    role: str = Form(...),
    new_password: str = Form(""),
    password_confirmation: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    if role not in USER_ROLES:
        flash(request, "Perfil de acesso inválido.", "err")
        return redirect(f"/users/{managed_user_id}")
    if managed_user.id == user.id and role != managed_user.role:
        flash(request, "Você não pode alterar o perfil da própria conta.", "err")
        return redirect(f"/users/{managed_user_id}")
    if managed_user.id == user.id and (new_password or password_confirmation):
        flash(
            request,
            "Use a opção Alterar minha senha para modificar a própria senha.",
            "err",
        )
        return redirect(f"/users/{managed_user_id}")
    if managed_user.role == "admin" and role != "admin" and _admin_count(db) <= 1:
        flash(request, "O sistema precisa manter pelo menos um administrador.", "err")
        return redirect(f"/users/{managed_user_id}")
    if new_password or password_confirmation:
        try:
            validate_password(new_password, password_confirmation)
        except ValueError as exc:
            flash(request, str(exc), "err")
            return redirect(f"/users/{managed_user_id}")
        managed_user.password_hash = hash_password(new_password)
        revoke_sessions(managed_user)
    managed_user.role = role
    update_summary = f"Perfil definido como {role}."
    if new_password:
        update_summary += " Senha redefinida pelo administrador."
    return commit_action(
        db,
        request,
        user,
        action="update",
        resource_type="user",
        resource_id=managed_user.id,
        resource_name=managed_user.username,
        summary=update_summary,
        notice="Usuário atualizado.",
        redirect_to="/users",
        event="user.update",
        managed_user_id=managed_user.id,
        managed_user_role=managed_user.role,
        password_reset=bool(new_password),
    )


@router.post("/users/{managed_user_id}/delete")
def users_delete(
    managed_user_id: int,
    request: Request,
    managed_user: User = Depends(_managed_user),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    if managed_user.id == user.id:
        flash(request, "Você não pode excluir a própria conta.", "err")
        return redirect("/users")
    if managed_user.role == "admin" and _admin_count(db) <= 1:
        flash(request, "O sistema precisa manter pelo menos um administrador.", "err")
        return redirect("/users")
    deleted_user_id = managed_user.id
    deleted_username = managed_user.username
    db.delete(managed_user)
    return commit_action(
        db,
        request,
        user,
        action="delete",
        resource_type="user",
        resource_id=deleted_user_id,
        resource_name=deleted_username,
        summary="Conta de usuário removida.",
        notice="Usuário excluído.",
        redirect_to="/users",
        event="user.delete",
        managed_user_id=deleted_user_id,
    )
