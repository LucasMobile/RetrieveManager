"""Users, audit logs and received instances needing attention."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.db import get_db
from app.models import (
    AuditLog,
    DicomInstance,
    ImageTransfer,
    Unit,
    User,
)
from app.observability import log_event
from app.pager import cursor_page_links, paginate, query_keep
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
    audit,
    ctx,
    cursor_page_cursors,
    flash,
    log,
    normalize_username,
    require_admin,
    templates,
    validate_password,
)

router = APIRouter()


def _admin_count(db: Session) -> int:
    return (
        db.scalar(select(func.count()).select_from(User).where(User.role == "admin"))
        or 0
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
    total = int(db.scalar(select(func.count()).where(*scope)) or 0)
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
    if page_size not in PAGE_SIZE_OPTIONS:
        raise StarletteHTTPException(422, "Quantidade de itens por página inválida")
    if sum((before is not None, after is not None, last)) > 1:
        raise StarletteHTTPException(422, "Use apenas um cursor de paginação")
    if page > 1 and before is None and after is None and not last:
        raise StarletteHTTPException(422, "Cursor de paginação ausente")

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

    count_query = select(func.count()).select_from(filtered.subquery())
    total = int(db.scalar(count_query) or 0)
    pages = max(1, (total + page_size - 1) // page_size)
    if last:
        page = pages
    elif before is None and after is None:
        page = 1
    pager = paginate(total, page, page_size)
    pager["size_options"] = PAGE_SIZE_OPTIONS
    pager["keep"] = {"action": action, "resource": resource, "q": search}
    stmt = filtered
    if last:
        stmt = stmt.order_by(AuditLog.id.asc())
    elif before is not None:
        stmt = stmt.where(AuditLog.id < before).order_by(AuditLog.id.desc())
    elif after is not None:
        stmt = stmt.where(AuditLog.id > after).order_by(AuditLog.id.asc())
    else:
        stmt = stmt.order_by(AuditLog.id.desc())
    result_limit = (total - pager["offset"]) if last else pager["size"]
    entries = list(db.scalars(stmt.limit(result_limit)))
    if after is not None or last:
        entries.reverse()

    has_prev = False
    has_next = False
    if entries:
        has_prev = (
            db.scalar(
                filtered.where(AuditLog.id > entries[0].id)
                .with_only_columns(AuditLog.id)
                .limit(1)
            )
            is not None
        )
        has_next = (
            db.scalar(
                filtered.where(AuditLog.id < entries[-1].id)
                .with_only_columns(AuditLog.id)
                .limit(1)
            )
            is not None
        )
    pager.update(
        cursor=True,
        has_prev=has_prev,
        has_next=has_next,
        prev_cursor=entries[0].id if entries else None,
        next_cursor=entries[-1].id if entries else None,
    )

    pager["page_links"] = cursor_page_links(
        pager,
        page_cursors=cursor_page_cursors(db, filtered, AuditLog.id, pager),
    )
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    summary = {
        "total": int(db.scalar(select(func.count()).select_from(AuditLog)) or 0),
        "today": int(
            db.scalar(
                select(func.count())
                .select_from(AuditLog)
                .where(AuditLog.created_at >= today)
            )
            or 0
        ),
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
        return RedirectResponse("/users/new", status_code=303)
    if db.scalar(select(User).where(User.username == clean_username)) is not None:
        flash(request, "Já existe um usuário com esse nome.", "err")
        return RedirectResponse("/users/new", status_code=303)
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
        return RedirectResponse("/users/new", status_code=303)
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
    return RedirectResponse("/users", status_code=303)


@router.get("/users/{managed_user_id}", response_class=HTMLResponse)
def users_edit(
    managed_user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    managed_user = db.get(User, managed_user_id)
    if managed_user is None:
        return RedirectResponse("/users", status_code=303)
    return templates.TemplateResponse(
        request=request,
        name="user_form.html",
        context=ctx(request, db, "users", managed_user=managed_user),
    )


@router.post("/users/{managed_user_id}")
def users_update(
    managed_user_id: int,
    request: Request,
    role: str = Form(...),
    new_password: str = Form(""),
    password_confirmation: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    managed_user = db.get(User, managed_user_id)
    if managed_user is None:
        return RedirectResponse("/users", status_code=303)
    if role not in USER_ROLES:
        flash(request, "Perfil de acesso inválido.", "err")
        return RedirectResponse(f"/users/{managed_user_id}", status_code=303)
    if managed_user.id == user.id and role != managed_user.role:
        flash(request, "Você não pode alterar o perfil da própria conta.", "err")
        return RedirectResponse(f"/users/{managed_user_id}", status_code=303)
    if managed_user.id == user.id and (new_password or password_confirmation):
        flash(
            request,
            "Use a opção Alterar minha senha para modificar a própria senha.",
            "err",
        )
        return RedirectResponse(f"/users/{managed_user_id}", status_code=303)
    if managed_user.role == "admin" and role != "admin" and _admin_count(db) <= 1:
        flash(request, "O sistema precisa manter pelo menos um administrador.", "err")
        return RedirectResponse(f"/users/{managed_user_id}", status_code=303)
    if new_password or password_confirmation:
        try:
            validate_password(new_password, password_confirmation)
        except ValueError as exc:
            flash(request, str(exc), "err")
            return RedirectResponse(f"/users/{managed_user_id}", status_code=303)
        managed_user.password_hash = hash_password(new_password)
        revoke_sessions(managed_user)
    managed_user.role = role
    update_summary = f"Perfil definido como {role}."
    if new_password:
        update_summary += " Senha redefinida pelo administrador."
    audit(
        db,
        request,
        user,
        action="update",
        resource_type="user",
        resource_id=managed_user.id,
        resource_name=managed_user.username,
        summary=update_summary,
    )
    db.commit()
    log_event(
        log,
        logging.INFO,
        "user.update",
        resource=f"user:{managed_user.id}",
        status="success",
        user_id=user.id,
        managed_user_id=managed_user.id,
        managed_user_role=managed_user.role,
        password_reset=bool(new_password),
    )
    flash(request, "Usuário atualizado.")
    return RedirectResponse("/users", status_code=303)


@router.post("/users/{managed_user_id}/delete")
def users_delete(
    managed_user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    managed_user = db.get(User, managed_user_id)
    if managed_user is None:
        return RedirectResponse("/users", status_code=303)
    if managed_user.id == user.id:
        flash(request, "Você não pode excluir a própria conta.", "err")
        return RedirectResponse("/users", status_code=303)
    if managed_user.role == "admin" and _admin_count(db) <= 1:
        flash(request, "O sistema precisa manter pelo menos um administrador.", "err")
        return RedirectResponse("/users", status_code=303)
    deleted_user_id = managed_user.id
    deleted_username = managed_user.username
    db.delete(managed_user)
    audit(
        db,
        request,
        user,
        action="delete",
        resource_type="user",
        resource_id=deleted_user_id,
        resource_name=deleted_username,
        summary="Conta de usuário removida.",
    )
    db.commit()
    log_event(
        log,
        logging.INFO,
        "user.delete",
        resource=f"user:{deleted_user_id}",
        status="success",
        user_id=user.id,
        managed_user_id=deleted_user_id,
    )
    flash(request, "Usuário excluído.")
    return RedirectResponse("/users", status_code=303)
