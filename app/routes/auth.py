"""Health checks, login/logout and the user's own password."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import (
    User,
)
from app.observability import log_event
from app.rate_limit import (
    apply_rate_limit_headers,
    client_ip,
    login_failure_rate_limiter,
)
from app.security import (
    csrf_token,
    hash_password,
    revoke_sessions,
    start_session,
    verify_login,
    verify_password,
)
from app.web import (
    audit,
    ctx,
    flash,
    log,
    require_admin,
    require_user,
    templates,
    validate_password,
)

router = APIRouter()


@router.get("/health")
def health(db: Session = Depends(get_db)) -> dict[str, str]:
    db.execute(text("SELECT 1"))
    return {"status": "ready"}


@router.get("/health/live")
def liveness() -> dict[str, str]:
    return {"status": "alive"}


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={"request": request, "error": None},
    )


@router.post("/login")
def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    client_id = client_ip(request)
    login_limit = login_failure_rate_limiter.check(client_id)
    if not login_limit.allowed:
        log_event(
            log,
            logging.WARNING,
            "auth.login",
            resource="session",
            status="rate_limited",
            error_type="TooManyLoginAttempts",
        )
        return apply_rate_limit_headers(
            templates.TemplateResponse(
                request=request,
                name="login.html",
                context={
                    "request": request,
                    "error": "Muitas tentativas. Aguarde alguns minutos.",
                },
                status_code=429,
            ),
            login_limit,
            scope="failed-login",
        )
    clean_username = username.strip()
    invalid_size = len(clean_username) > 80 or len(password.encode("utf-8")) > 72
    user = None
    password_ok = False
    if not invalid_size:
        user = db.scalar(select(User).where(User.username == clean_username))
        password_ok = verify_login(password, user.password_hash if user else None)
    if user is None or not password_ok:
        login_limit = login_failure_rate_limiter.check(client_id, consume=True)
        log_event(
            log,
            logging.WARNING,
            "auth.login",
            resource="session",
            status="failure",
            error_type="InvalidCredentials",
        )
        return apply_rate_limit_headers(
            templates.TemplateResponse(
                request=request,
                name="login.html",
                context={"request": request, "error": "Usuário ou senha inválidos."},
                status_code=401,
            ),
            login_limit,
            scope="failed-login",
        )
    request.session.clear()
    start_session(request, user)
    csrf_token(request)
    log_event(
        log,
        logging.INFO,
        "auth.login",
        resource="session",
        status="success",
        user_id=user.id,
    )
    return apply_rate_limit_headers(
        RedirectResponse("/", status_code=303),
        login_failure_rate_limiter.check(client_id),
        scope="failed-login",
    )


@router.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


@router.get("/settings")
def settings_redirect(user: User = Depends(require_admin)):
    return RedirectResponse("/users", status_code=303)


@router.get("/account/password", response_class=HTMLResponse)
def account_password_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    return templates.TemplateResponse(
        request=request,
        name="account_password.html",
        context=ctx(request, db, "account"),
    )


@router.post("/account/password")
def account_password_update(
    request: Request,
    current: str = Form(...),
    new_password: str = Form(...),
    password_confirmation: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    if not verify_password(current, user.password_hash):
        flash(request, "Senha atual incorreta.", "err")
        return RedirectResponse("/account/password", status_code=303)
    try:
        validate_password(new_password, password_confirmation)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return RedirectResponse("/account/password", status_code=303)
    user.password_hash = hash_password(new_password)
    # Other browsers/devices lose access; this one keeps a fresh session.
    revoke_sessions(user)
    start_session(request, user)
    audit(
        db,
        request,
        user,
        action="password",
        resource_type="user",
        resource_id=user.id,
        resource_name=user.username,
        summary="Usuário alterou a própria senha.",
    )
    db.commit()
    log_event(
        log,
        logging.INFO,
        "user.password_change",
        resource=f"user:{user.id}",
        status="success",
        user_id=user.id,
    )
    flash(request, "Senha atualizada.")
    return RedirectResponse("/account/password", status_code=303)
