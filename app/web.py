"""Shared web layer: templates, session user, context, actions and labels."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.audit import audit
from app.config import (
    BASE_DIR,
    COMPACT_GLOBAL_WORKERS,
)
from app.db import get_db
from app.models import User
from app.observability import log_event
from app.security import (
    csrf_token,
)
from app.wording import plural

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
    "restore": "Restauração",
}


AUDIT_RESOURCE_LABELS = {
    "unit": "Unidade",
    "order": "Pedido",
    "dicom_rule": "Regra DICOM",
    "instance": "Instâncias",
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
    "restore": "success",
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
templates.env.globals["compact_global_workers"] = COMPACT_GLOBAL_WORKERS


# Jinja: ``{{ n | plural("regra ativa", "regras ativas") }}``.
templates.env.filters["plural"] = plural


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


class FlashRedirect(Exception):
    """Abort a request with a 303 to ``url``, optionally flashing ``message``."""

    def __init__(self, url: str, message: str = "", kind: str = "err") -> None:
        super().__init__(url)
        self.url = url
        self.message = message
        self.kind = kind


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


async def form_strings(request: Request) -> dict[str, str]:
    """Posted form fields, ignoring uploads."""
    return {k: v for k, v in (await request.form()).items() if isinstance(v, str)}


def redirect(url: str) -> RedirectResponse:
    """303 so the browser follows a POST with a GET."""
    return RedirectResponse(url, status_code=303)


def commit_action(
    db: Session,
    request: Request,
    user: User,
    *,
    action: str,
    resource_type: str,
    resource_id: int | str,
    resource_name: str,
    summary: str,
    notice: str,
    redirect_to: str,
    event: str | None = None,
    **event_fields: Any,
) -> RedirectResponse:
    """Audit and commit a user action, log ``event`` and redirect with a notice."""
    audit(
        db,
        request,
        user,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        resource_name=resource_name,
        summary=summary,
    )
    db.commit()
    if event:
        log_event(
            log,
            logging.INFO,
            event,
            resource=f"{resource_type.replace('_', '-')}:{resource_id}",
            status="success",
            user_id=user.id,
            **event_fields,
        )
    flash(request, notice)
    return redirect(redirect_to)


def badge_for(status: str) -> str:
    return {
        "done": "ok",
        "error": "err",
        "cancelled": "off",
        "watching": "info",
        "wait_retrieve": "warn",
        "wait_update": "warn",
        "monitoring": "info",
        "retrieving": "progress",
        "retrieving_update": "progress",
        "receiving": "progress",
    }.get(status, "off")


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
