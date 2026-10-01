"""Shared web layer: templates, session user, context, audit and labels."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import Depends, HTTPException, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.config import (
    BASE_DIR,
)
from app.db import get_db
from app.models import (
    AuditLog,
    User,
)
from app.security import (
    csrf_token,
)

EVENTS_PAGE = 10


UNITS_PAGE = 20


DASH_PAGE = 8


PAGE_SIZE_OPTIONS = (10, 20, 30, 40, 50)


DEFAULT_PAGE_SIZE = 30


USER_ROLES = frozenset({"admin", "user"})


PRIOR_STATUS_LABELS = {
    "disabled": "Desativado",
    "queued": "Na fila",
    "retrieving": "Em andamento",
    "retry_wait": "Aguardando nova tentativa",
    "done": "Concluído",
    "error": "Erro",
    "cancelled": "Cancelado",
}


AUDIT_ACTION_LABELS = {
    "create": "Adição",
    "update": "Alteração",
    "delete": "Remoção",
    "enable": "Ativação",
    "disable": "Desativação",
    "retry": "Reprocessamento",
    "cancel": "Cancelamento",
    "password": "Senha alterada",
    "archive": "Arquivamento",
    "resend": "Reenvio",
}


AUDIT_RESOURCE_LABELS = {
    "unit": "Unidade",
    "order": "Pedido",
    "dicom_rule": "Regra DICOM",
    "retrieve_rule": "Regra de retrieve",
    "user": "Usuário",
}


AUDIT_ACTION_BADGES = {
    "create": "active",
    "update": "info",
    "delete": "danger",
    "enable": "success",
    "disable": "neutral",
    "retry": "warning",
    "cancel": "danger",
    "password": "info",
    "archive": "neutral",
    "resend": "warning",
}


# Received objects that need attention: (label, badge, explanation).
INSTANCE_ISSUES = {
    "conflict": (
        "Conflito",
        "danger",
        "Mesmo SOP Instance UID recebido com conteúdo diferente. O primeiro objeto "
        "seguiu no fluxo; o novo ficou na pasta de erro para análise.",
    ),
    "missing": (
        "Arquivo ausente",
        "warning",
        "O arquivo recebido sumiu antes de ser compactado ou publicado.",
    ),
    "error": (
        "Erro",
        "error",
        "Falha na leitura, nas regras ou na compactação. A origem está na pasta de "
        "erro; use Reprocessar erros na unidade depois de corrigir a causa.",
    ),
}


log = logging.getLogger("web")


templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))


templates.env.globals["csrf_token"] = csrf_token


def _flash(request: Request) -> dict | None:
    data = request.session.pop("flash", None)
    return data


def flash(request: Request, text: str, kind: str = "ok") -> None:
    request.session["flash"] = {"text": text, "kind": kind}


def current_user(request: Request, db: Session) -> User | None:
    uid = request.session.get("user_id")
    if not uid:
        return None
    user = db.get(User, uid)
    if user is None or request.session.get("session_version", 0) != (
        user.session_version or 0
    ):
        request.session.clear()
        return None
    return user


def require_user(request: Request, db: Session = Depends(get_db)) -> User:
    user = current_user(request, db)
    if user is None:
        raise LoginRedirect()
    request.state.user = user
    return user


def require_admin(user: User = Depends(require_user)) -> User:
    if user.role != "admin":
        raise HTTPException(
            status_code=403, detail="Acesso exclusivo para administradores."
        )
    return user


class LoginRedirect(Exception):
    pass


def wants_html(request: Request) -> bool:
    return "text/html" in request.headers.get("accept", "")


def error_page(
    request: Request,
    *,
    status_code: int,
    title: str,
    message: str,
    action_href: str | None = None,
    action_label: str = "Voltar ao início",
):
    session = request.scope.get("session", {})
    home_href = (
        "/" if isinstance(session, dict) and session.get("user_id") else "/login"
    )
    return templates.TemplateResponse(
        request=request,
        name="error.html",
        context={
            "request": request,
            "status_code": status_code,
            "title": title,
            "message": message,
            "home_href": home_href,
            "action_href": action_href or home_href,
            "action_label": action_label,
            "request_id": getattr(request.state, "correlation_id", ""),
        },
        status_code=status_code,
    )


def ctx(request: Request, db: Session, nav: str, **extra: Any) -> dict:
    user = getattr(request.state, "user", None) or current_user(request, db)
    data = {"request": request, "user": user, "nav": nav, "flash": _flash(request)}
    data.update(extra)
    return data


def audit(
    db: Session,
    request: Request | None,
    user: User | None,
    *,
    action: str,
    resource_type: str,
    resource_id: int | str | None,
    resource_name: str,
    summary: str,
) -> None:
    actor_id = getattr(user, "id", None)
    actor_username = getattr(user, "username", None)
    actor_role = getattr(user, "role", None)
    client = getattr(request, "client", None) if request else None
    db.add(
        AuditLog(
            actor_id=actor_id,
            actor_username=actor_username
            or (f"Usuário #{actor_id}" if actor_id else "Sistema"),
            actor_role=actor_role or ("admin" if actor_id else "system"),
            action=action,
            resource_type=resource_type,
            resource_id=str(resource_id or ""),
            resource_name=resource_name[:255],
            summary=summary[:500],
            ip_address=(client.host if client else "")[:64],
        )
    )


def badge_for(status: str) -> str:
    return {
        "done": "ok",
        "error": "err",
        "cancelled": "off",
        "watching": "info",
        "wait_retrieve": "warn",
        "wait_second": "warn",
        "retrieving": "accent",
        "retrieving_second": "accent",
        "receiving": "accent",
    }.get(status, "off")


def cursor_page_cursors(
    db: Session,
    filtered,
    id_column,
    pager: dict,
) -> dict[int, int | None]:
    """Return the keyset boundary required to open every visible page."""
    cursors: dict[int, int | None] = {}
    for target_page in pager["page_items"]:
        if target_page in (None, 1, pager["page"], pager["pages"]):
            continue
        boundary_offset = ((target_page - 1) * pager["size"]) - 1
        cursors[target_page] = db.scalar(
            filtered.with_only_columns(id_column)
            .order_by(id_column.desc())
            .offset(boundary_offset)
            .limit(1)
        )
    return cursors


def normalize_username(value: str) -> str:
    username = value.strip()
    if not 3 <= len(username) <= 80:
        raise ValueError("O usuário deve ter entre 3 e 80 caracteres.")
    if not all(char.isalnum() or char in "._-@" for char in username):
        raise ValueError(
            "O usuário aceita apenas letras, números, ponto, hífen, sublinhado e @."
        )
    return username


def validate_password(password: str, confirmation: str) -> None:
    if password != confirmation:
        raise ValueError("A confirmação da senha não confere.")
    password_bytes = len(password.encode("utf-8"))
    if not 12 <= password_bytes <= 72:
        raise ValueError("A senha deve ter entre 12 e 72 bytes.")
