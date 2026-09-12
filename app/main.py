from __future__ import annotations

import logging
import re
import shutil
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any, Literal
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, Form, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import case, delete, func, or_, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.sessions import SessionMiddleware

from app.config import BASE_DIR, SECRET_KEY, SESSION_HTTPS_ONLY
from app.db import get_db, init_db
from app.dicom_tools import ToolMissing, c_echo
from app.middleware import apply_security_headers, request_middleware
from app.models import (
    STATUSES,
    CompressRule,
    DropModality,
    HistoricalImageLink,
    HistoricalStudy,
    ImageTransfer,
    ModalityRule,
    Order,
    OrderEvent,
    Unit,
    User,
)
from app.netutil import port_listening
from app.observability import configure_logging, log_event, new_correlation_id
from app.pager import paginate, query_keep
from app.pipeline import folder_counts
from app.rules import get_settings, schedule_from_now
from app.security import (
    clear_login_failures,
    csrf_token,
    hash_password,
    login_allowed,
    record_login_failure,
    verify_csrf,
    verify_password,
)
from app.validation import (
    validate_cloud_url,
    validate_jpeg_flag,
    validate_modality,
    validate_pacs_connection,
    validate_unit_form,
)

ORDERS_PAGE = 40
EVENTS_PAGE = 10
UNITS_PAGE = 20
DASH_PAGE = 8
ACTIVE_ORDER_STATUSES = frozenset({"retrieving", "retrieving_second", "receiving"})
ACTIVE_PRIOR_STATUSES = frozenset({"queued", "retry_wait", "retrieving"})
PRIOR_STATUS_LABELS = {
    "disabled": "Desativado",
    "queued": "Na fila",
    "retrieving": "Em andamento",
    "retry_wait": "Aguardando nova tentativa",
    "done": "Concluído",
    "error": "Erro",
}
log = logging.getLogger("web")

configure_logging()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Retrieve Manager", lifespan=lifespan, dependencies=[Depends(verify_csrf)]
)
app.add_middleware(
    SessionMiddleware,
    secret_key=SECRET_KEY,
    session_cookie="rm_session",
    same_site="strict",
    https_only=SESSION_HTTPS_ONLY,
    max_age=8 * 60 * 60,
)
app.middleware("http")(request_middleware)
app.mount(
    "/static", StaticFiles(directory=str(BASE_DIR / "app" / "static")), name="static"
)
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
    return db.get(User, uid)


def require_user(request: Request, db: Session = Depends(get_db)) -> User:
    user = current_user(request, db)
    if user is None:
        raise LoginRedirect()
    return user


class LoginRedirect(Exception):
    pass


@app.exception_handler(LoginRedirect)
async def _login_redirect(_request: Request, _exc: LoginRedirect):
    return RedirectResponse("/login", status_code=303)


def _wants_html(request: Request) -> bool:
    return "text/html" in request.headers.get("accept", "")


def _error_page(
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


@app.exception_handler(StarletteHTTPException)
async def _http_error(request: Request, exc: StarletteHTTPException):
    if not _wants_html(request):
        return JSONResponse(
            {"detail": exc.detail}, status_code=exc.status_code, headers=exc.headers
        )
    errors = {
        403: (
            "Acesso não permitido",
            "Você não tem permissão para acessar este conteúdo.",
        ),
        404: (
            "Página não encontrada",
            "O endereço informado não existe ou não está mais disponível.",
        ),
        405: (
            "Ação não permitida",
            "Esta página não aceita o tipo de operação solicitado.",
        ),
    }
    title, message = errors.get(
        exc.status_code,
        (
            "Não foi possível abrir esta página",
            "Revise a solicitação e tente novamente.",
        ),
    )
    return _error_page(
        request, status_code=exc.status_code, title=title, message=message
    )


@app.exception_handler(RequestValidationError)
async def _request_validation_error(request: Request, exc: RequestValidationError):
    if not _wants_html(request):
        return JSONResponse({"detail": jsonable_encoder(exc.errors())}, status_code=422)
    return _error_page(
        request,
        status_code=422,
        title="Dados inválidos",
        message=(
            "Um ou mais campos não puderam ser validados. "
            "Revise os valores e tente novamente."
        ),
        action_href={
            "/settings/password": "/settings",
            "/settings": "/settings",
            "/rules/retrieve": "/rules/retrieve",
            "/rules/compress": "/rules/compress",
            "/rules/drop": "/rules/compress",
            "/login": "/login",
        }.get(request.url.path, "/"),
        action_label="Voltar",
    )


@app.exception_handler(Exception)
async def _unexpected_error(request: Request, _exc: Exception):
    if not _wants_html(request):
        return apply_security_headers(
            request, JSONResponse({"detail": "erro interno"}, status_code=500)
        )
    response = _error_page(
        request,
        status_code=500,
        title="Algo não saiu como esperado",
        message=(
            "A solicitação não pôde ser concluída. Tente novamente; se o erro "
            "continuar, informe o ID abaixo ao suporte."
        ),
        action_href=request.url.path,
        action_label="Tentar novamente",
    )
    return apply_security_headers(request, response)


def ctx(request: Request, db: Session, nav: str, **extra: Any) -> dict:
    user = current_user(request, db)
    data = {"request": request, "user": user, "nav": nav, "flash": _flash(request)}
    data.update(extra)
    return data


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


@app.get("/health")
def health(db: Session = Depends(get_db)) -> dict[str, str]:
    db.execute(text("SELECT 1"))
    return {"status": "ready"}


@app.get("/health/live")
def liveness() -> dict[str, str]:
    return {"status": "alive"}


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={"request": request, "error": None},
    )


@app.post("/login")
def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    client_id = request.client.host if request.client else "unknown"
    if not login_allowed(client_id):
        log_event(
            log,
            logging.WARNING,
            "auth.login",
            resource="session",
            status="rate_limited",
            error_type="TooManyLoginAttempts",
        )
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={
                "request": request,
                "error": "Muitas tentativas. Aguarde alguns minutos.",
            },
            status_code=429,
        )
    clean_username = username.strip()
    invalid_size = len(clean_username) > 80 or len(password.encode("utf-8")) > 72
    user = None
    if not invalid_size:
        user = db.scalar(select(User).where(User.username == clean_username))
    if user is None or not verify_password(password, user.password_hash):
        record_login_failure(client_id)
        log_event(
            log,
            logging.WARNING,
            "auth.login",
            resource="session",
            status="failure",
            error_type="InvalidCredentials",
        )
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={"request": request, "error": "Usuário ou senha inválidos."},
            status_code=401,
        )
    clear_login_failures(client_id)
    request.session.clear()
    request.session["user_id"] = user.id
    csrf_token(request)
    log_event(
        log,
        logging.INFO,
        "auth.login",
        resource="session",
        status="success",
        user_id=user.id,
    )
    return RedirectResponse("/", status_code=303)


@app.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


def _unit_view(unit: Unit, counts: tuple[int, int, int, int]) -> Unit:
    watching, queue, running, errors = counts
    unit.counts = {  # type: ignore[attr-defined]
        "watching": watching,
        "queue": queue,
        "running": running,
        "error": errors,
    }
    unit.folders = folder_counts(unit)  # type: ignore[attr-defined]
    unit.store_up = port_listening(unit.store_port) if unit.enabled else False  # type: ignore[attr-defined]
    return unit


def _dashboard_units(
    db: Session, page: int
) -> tuple[list[Unit], dict[str, int | bool], dict[str, int]]:
    total = db.scalar(select(func.count()).select_from(Unit)) or 0
    pager = paginate(total, page, DASH_PAGE)
    units = list(
        db.scalars(
            select(Unit)
            .order_by(Unit.name)
            .offset(pager["offset"])
            .limit(pager["size"])
        )
    )
    unit_ids = [unit.id for unit in units]
    stats: dict[int, tuple[int, int, int, int]] = {}
    if unit_ids:
        day = datetime.now() - timedelta(hours=24)
        rows = db.execute(
            select(
                Order.unit_id,
                func.sum(case((Order.status == "watching", 1), else_=0)),
                func.sum(
                    case(
                        (
                            Order.status.in_(("wait_retrieve", "wait_second"))
                            | Order.prior_status.in_(("queued", "retry_wait")),
                            1,
                        ),
                        else_=0,
                    )
                ),
                func.sum(
                    case(
                        (
                            Order.status.in_(("retrieving", "retrieving_second"))
                            | (Order.prior_status == "retrieving"),
                            1,
                        ),
                        else_=0,
                    )
                ),
                func.sum(
                    case(
                        (
                            (
                                (Order.status == "error")
                                | (Order.prior_status == "error")
                            )
                            & (Order.updated_at >= day),
                            1,
                        ),
                        else_=0,
                    )
                ),
            )
            .where(Order.unit_id.in_(unit_ids))
            .group_by(Order.unit_id)
        )
        for row in rows:
            stats[int(row[0])] = (
                int(row[1] or 0),
                int(row[2] or 0),
                int(row[3] or 0),
                int(row[4] or 0),
            )
    day = datetime.now() - timedelta(hours=24)
    summary_counts = db.execute(
        select(
            func.sum(case((Order.status == "watching", 1), else_=0)),
            func.sum(
                case(
                    (
                        Order.status.in_(("wait_retrieve", "wait_second"))
                        | Order.prior_status.in_(("queued", "retry_wait")),
                        1,
                    ),
                    else_=0,
                )
            ),
            func.sum(
                case(
                    (
                        Order.status.in_(("retrieving", "retrieving_second"))
                        | (Order.prior_status == "retrieving"),
                        1,
                    ),
                    else_=0,
                )
            ),
            func.sum(
                case(
                    (
                        (
                            (Order.status == "error")
                            | (Order.prior_status == "error")
                        )
                        & (Order.updated_at >= day),
                        1,
                    ),
                    else_=0,
                )
            ),
        )
    ).one()
    summary = {
        "enabled": int(
            db.scalar(select(func.count()).select_from(Unit).where(Unit.enabled)) or 0
        ),
        "watching": int(summary_counts[0] or 0),
        "queue": int(summary_counts[1] or 0),
        "running": int(summary_counts[2] or 0),
        "error": int(summary_counts[3] or 0),
    }
    views = [_unit_view(unit, stats.get(unit.id, (0, 0, 0, 0))) for unit in units]
    return views, pager, summary


@app.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    page: int = 1,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    units, pager, summary = _dashboard_units(db, page)
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context=ctx(
            request, db, "dash", units=units, pager=pager, summary=summary, qs=""
        ),
    )


@app.get("/dashboard/partial", response_class=HTMLResponse)
def dashboard_partial(
    request: Request,
    page: int = 1,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    units, pager, summary = _dashboard_units(db, page)
    return templates.TemplateResponse(
        request=request,
        name="dashboard_partial.html",
        context=ctx(
            request, db, "dash", units=units, pager=pager, summary=summary, qs=""
        ),
    )


@app.get("/units", response_class=HTMLResponse)
def units_list(
    request: Request,
    page: int = 1,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    total = db.scalar(select(func.count()).select_from(Unit)) or 0
    enabled = (
        db.scalar(select(func.count()).select_from(Unit).where(Unit.enabled.is_(True)))
        or 0
    )
    pager = paginate(total, page, UNITS_PAGE)
    units = list(
        db.scalars(
            select(Unit)
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


@app.get("/units/new", response_class=HTMLResponse)
def units_new(
    request: Request, db: Session = Depends(get_db), user: User = Depends(require_user)
):
    return templates.TemplateResponse(
        request=request,
        name="units_form.html",
        context=ctx(request, db, "units", unit=None),
    )


def _unit_from_form(form: dict[str, Any], unit: Unit | None) -> Unit:
    obj = unit or Unit()
    obj.name = str(form["name"])
    obj.enabled = bool(form["enabled"])
    obj.pacs_aet = str(form["pacs_aet"])
    obj.pacs_ip = str(form["pacs_ip"])
    obj.pacs_port = int(form["pacs_port"])
    obj.calling_aet = str(form["calling_aet"])
    obj.store_port = int(form["store_port"])
    obj.orders_api_url = str(form["orders_api_url"])
    orders_api_token = str(form.get("orders_api_token") or "")
    if orders_api_token:
        obj.orders_api_token = orders_api_token
    elif unit is None:
        obj.orders_api_token = ""
    obj.orders_api_station_id = str(form.get("orders_api_station_id") or "")
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
    obj.move_timeout_first = int(form["move_timeout_first"])
    obj.move_timeout_second = int(form["move_timeout_second"])
    obj.max_parallel_moves = int(form["max_parallel_moves"])
    obj.find_interval_seconds = int(form["find_interval_seconds"])
    obj.compact_workers = int(form["compact_workers"])
    obj.send_workers = int(form["send_workers"])
    return obj


def _port_taken(db: Session, port: int, unit_id: int | None) -> bool:
    q = select(Unit).where(Unit.store_port == port)
    if unit_id:
        q = q.where(Unit.id != unit_id)
    return db.scalar(q) is not None


@app.post("/units/new")
async def units_create(
    request: Request, db: Session = Depends(get_db), user: User = Depends(require_user)
):
    raw_form = {k: v for k, v in (await request.form()).items() if isinstance(v, str)}
    try:
        form = validate_unit_form(raw_form)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return RedirectResponse("/units/new", status_code=303)
    if _port_taken(db, int(form["store_port"]), None):
        flash(request, "Essa porta de store já está em uso.", "err")
        return RedirectResponse("/units/new", status_code=303)
    if not form.get("token"):
        flash(request, "Token da unidade é obrigatório.", "err")
        return RedirectResponse("/units/new", status_code=303)
    if len(str(form["token"])) > 64:
        flash(request, "Token da unidade excede 64 caracteres.", "err")
        return RedirectResponse("/units/new", status_code=303)
    if not form.get("orders_api_token"):
        flash(request, "Token de Integração é obrigatório.", "err")
        return RedirectResponse("/units/new", status_code=303)
    if len(str(form["orders_api_token"])) > 2048:
        flash(request, "Token de Integração excede 2048 caracteres.", "err")
        return RedirectResponse("/units/new", status_code=303)
    if len(str(form.get("orders_api_station_id") or "")) > 64:
        flash(request, "ID Posto excede 64 caracteres.", "err")
        return RedirectResponse("/units/new", status_code=303)
    unit = _unit_from_form(form, None)
    db.add(unit)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        flash(request, "Não foi possível salvar (nome duplicado?).", "err")
        return RedirectResponse("/units/new", status_code=303)
    flash(request, "Unidade criada. Cadastre o AET no PACS se ainda não existir.")
    return RedirectResponse("/units", status_code=303)


@app.post("/units/test-echo")
async def units_test_echo(
    request: Request,
    user: User = Depends(require_user),
):
    raw_form = {k: v for k, v in (await request.form()).items() if isinstance(v, str)}
    try:
        connection = validate_pacs_connection(raw_form)
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)

    started_at = perf_counter()
    try:
        code, _output = await run_in_threadpool(
            c_echo,
            str(connection["calling_aet"]),
            str(connection["pacs_aet"]),
            str(connection["pacs_ip"]),
            int(connection["pacs_port"]),
            10,
        )
    except ToolMissing as exc:
        log_event(
            log,
            logging.ERROR,
            "dicom.echo",
            resource="pacs",
            status="failure",
            started_at=started_at,
            error=exc,
            user_id=user.id,
        )
        return JSONResponse(
            {"ok": False, "message": "Ferramenta C-ECHO indisponível no container."},
            status_code=503,
        )

    if code != 0:
        log_event(
            log,
            logging.WARNING,
            "dicom.echo",
            resource="pacs",
            status="failure",
            started_at=started_at,
            return_code=code,
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
        return_code=code,
        user_id=user.id,
    )
    return JSONResponse({"ok": True, "message": "C-ECHO OK"})


@app.get("/units/{unit_id}", response_class=HTMLResponse)
def units_edit(
    unit_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    unit = db.get(Unit, unit_id)
    if unit is None:
        return RedirectResponse("/units", status_code=303)
    return templates.TemplateResponse(
        request=request,
        name="units_form.html",
        context=ctx(request, db, "units", unit=unit),
    )


@app.post("/units/{unit_id}")
async def units_update(
    unit_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    unit = db.get(Unit, unit_id)
    if unit is None:
        return RedirectResponse("/units", status_code=303)
    raw_form = {k: v for k, v in (await request.form()).items() if isinstance(v, str)}
    try:
        form = validate_unit_form(raw_form)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return RedirectResponse(f"/units/{unit_id}", status_code=303)
    if len(str(form.get("token") or "")) > 64:
        flash(request, "Token da unidade excede 64 caracteres.", "err")
        return RedirectResponse(f"/units/{unit_id}", status_code=303)
    if len(str(form.get("orders_api_token") or "")) > 2048:
        flash(request, "Token de Integração excede 2048 caracteres.", "err")
        return RedirectResponse(f"/units/{unit_id}", status_code=303)
    if len(str(form.get("orders_api_station_id") or "")) > 64:
        flash(request, "ID Posto excede 64 caracteres.", "err")
        return RedirectResponse(f"/units/{unit_id}", status_code=303)
    if _port_taken(db, int(form["store_port"]), unit_id):
        flash(request, "Essa porta de store já está em uso.", "err")
        return RedirectResponse(f"/units/{unit_id}", status_code=303)
    _unit_from_form(form, unit)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        flash(request, "Não foi possível salvar (nome duplicado?).", "err")
        return RedirectResponse(f"/units/{unit_id}", status_code=303)
    flash(request, "Unidade atualizada.")
    return RedirectResponse(f"/units/{unit_id}", status_code=303)


@app.post("/units/{unit_id}/toggle")
def units_toggle(
    unit_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    unit = db.get(Unit, unit_id)
    if unit:
        unit.enabled = not unit.enabled
        db.commit()
        flash(request, "Unidade " + ("ativada" if unit.enabled else "pausada") + ".")
    return RedirectResponse(request.headers.get("referer") or "/units", status_code=303)


@app.post("/units/{unit_id}/delete")
def units_delete(
    unit_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    unit = db.get(Unit, unit_id)
    if unit:
        db.execute(delete(ImageTransfer).where(ImageTransfer.unit_id == unit.id))
        ids = list(db.scalars(select(Order.id).where(Order.unit_id == unit.id)))
        if ids:
            db.execute(delete(OrderEvent).where(OrderEvent.order_id.in_(ids)))
            db.execute(delete(Order).where(Order.unit_id == unit.id))
        db.delete(unit)
        db.commit()
        flash(request, "Unidade apagada.")
    return RedirectResponse("/units", status_code=303)


@app.post("/units/{unit_id}/retry-errors")
def units_retry_errors(
    unit_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    unit = db.get(Unit, unit_id)
    if unit is None:
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
    flash(request, f"{moved} arquivo(s) devolvidos ao recebimento.")
    return RedirectResponse(f"/units/{unit_id}", status_code=303)


@app.get("/rules/retrieve", response_class=HTMLResponse)
def rules_retrieve(
    request: Request, db: Session = Depends(get_db), user: User = Depends(require_user)
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


@app.post("/rules/retrieve")
def rules_retrieve_add(
    request: Request,
    modality: str = Form(...),
    wait_minutes: int = Form(..., ge=0, le=1440),
    second_retrieve: Literal["0", "1"] = Form("0"),
    second_wait_minutes: int = Form(90, ge=0, le=2880),
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    try:
        safe_modality = validate_modality(modality)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return RedirectResponse("/rules/retrieve", status_code=303)
    db.add(
        ModalityRule(
            modality=safe_modality,
            wait_minutes=wait_minutes,
            second_retrieve=second_retrieve == "1",
            second_wait_minutes=second_wait_minutes,
        )
    )
    try:
        db.commit()
        flash(request, "Regra adicionada.")
    except IntegrityError:
        db.rollback()
        flash(request, "Modalidade já existe.", "err")
    return RedirectResponse("/rules/retrieve", status_code=303)


@app.post("/rules/retrieve/{rule_id}")
def rules_retrieve_update(
    rule_id: int,
    request: Request,
    modality: str = Form(...),
    wait_minutes: int = Form(..., ge=0, le=1440),
    second_retrieve: Literal["0", "1"] = Form("0"),
    second_wait_minutes: int = Form(90, ge=0, le=2880),
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    rule = db.get(ModalityRule, rule_id)
    if rule:
        try:
            safe_modality = validate_modality(modality)
        except ValueError as exc:
            flash(request, str(exc), "err")
            return RedirectResponse("/rules/retrieve", status_code=303)
        if rule.modality != "*":
            rule.modality = safe_modality
        rule.wait_minutes = wait_minutes
        rule.second_retrieve = second_retrieve == "1"
        if rule.second_retrieve:
            rule.second_wait_minutes = second_wait_minutes
        try:
            db.commit()
            flash(request, "Regra salva.")
        except IntegrityError:
            db.rollback()
            flash(request, "Modalidade já existe.", "err")
    return RedirectResponse("/rules/retrieve", status_code=303)


@app.post("/rules/retrieve/{rule_id}/delete")
def rules_retrieve_delete(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
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


@app.get("/rules/compress", response_class=HTMLResponse)
def rules_compress(
    request: Request, db: Session = Depends(get_db), user: User = Depends(require_user)
):
    compress = list(db.scalars(select(CompressRule).order_by(CompressRule.modality)))
    drops = list(db.scalars(select(DropModality).order_by(DropModality.code)))
    default_rule = next((rule for rule in compress if rule.modality == "*"), None)
    compression_summary = {
        "profiles": len(compress),
        "default_flag": default_rule.jpeg_flag if default_rule else "—",
        "drops": len(drops),
    }
    return templates.TemplateResponse(
        request=request,
        name="rules_compress.html",
        context=ctx(
            request,
            db,
            "compress",
            compress=compress,
            drops=drops,
            compression_summary=compression_summary,
        ),
    )


@app.post("/rules/compress")
def rules_compress_add(
    request: Request,
    modality: str = Form(...),
    jpeg_flag: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    try:
        safe_modality = validate_modality(modality)
        safe_flag = validate_jpeg_flag(jpeg_flag)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return RedirectResponse("/rules/compress", status_code=303)
    db.add(CompressRule(modality=safe_modality, jpeg_flag=safe_flag))
    try:
        db.commit()
        flash(request, "Perfil JPEG adicionado.")
    except IntegrityError:
        db.rollback()
        flash(request, "Modalidade já existe.", "err")
    return RedirectResponse("/rules/compress", status_code=303)


@app.post("/rules/compress/{rule_id}")
def rules_compress_update(
    rule_id: int,
    request: Request,
    modality: str = Form(...),
    jpeg_flag: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    rule = db.get(CompressRule, rule_id)
    if rule:
        try:
            safe_modality = validate_modality(modality)
            safe_flag = validate_jpeg_flag(jpeg_flag)
        except ValueError as exc:
            flash(request, str(exc), "err")
            return RedirectResponse("/rules/compress", status_code=303)
        if rule.modality != "*":
            rule.modality = safe_modality
        rule.jpeg_flag = safe_flag
        try:
            db.commit()
            flash(request, "Perfil salvo.")
        except IntegrityError:
            db.rollback()
            flash(request, "Modalidade já existe.", "err")
    return RedirectResponse("/rules/compress", status_code=303)


@app.post("/rules/compress/{rule_id}/delete")
def rules_compress_delete(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
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


@app.post("/rules/drop")
def rules_drop_add(
    request: Request,
    code: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    try:
        safe_code = validate_modality(code)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return RedirectResponse("/rules/compress", status_code=303)
    if safe_code == "*":
        flash(request, "O descarte não aceita modalidade curinga.", "err")
        return RedirectResponse("/rules/compress", status_code=303)
    db.add(DropModality(code=safe_code))
    try:
        db.commit()
        flash(request, "Modalidade adicionada ao descarte.")
    except IntegrityError:
        db.rollback()
        flash(request, "Modalidade já existe no descarte.", "err")
    return RedirectResponse("/rules/compress", status_code=303)


@app.post("/rules/drop/{drop_id}/delete")
def rules_drop_delete(
    drop_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    row = db.get(DropModality, drop_id)
    if row:
        db.delete(row)
        db.commit()
        flash(request, "Modalidade removida da lista de descarte.")
    return RedirectResponse("/rules/compress", status_code=303)


@app.get("/orders", response_class=HTMLResponse)
def orders_list(
    request: Request,
    unit_id: int | None = None,
    status: str = Query("", max_length=32),
    q: str = Query("", max_length=200),
    page: int = 1,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    if status and status not in STATUSES:
        raise StarletteHTTPException(422, "Status inválido")
    filt = select(Order)
    if unit_id:
        filt = filt.where(Order.unit_id == unit_id)
    if status:
        filt = filt.where(Order.status == status)
    if q:
        like = f"%{q.strip()}%"
        filt = filt.where(
            or_(
                Order.acc.like(like),
                Order.pat_id.like(like),
                Order.source_id.like(like),
            )
        )
    total = db.scalar(select(func.count()).select_from(filt.subquery())) or 0
    filtered_orders = filt.subquery()
    order_status_counts = db.execute(
        select(
            func.sum(
                case(
                    (
                        filtered_orders.c.status.in_(
                            ("watching", "wait_retrieve", "wait_second")
                        )
                        | filtered_orders.c.prior_status.in_(
                            ("queued", "retry_wait")
                        ),
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
    order_summary = {
        "total": total,
        "waiting": int(order_status_counts[0] or 0),
        "running": int(order_status_counts[1] or 0),
        "done": int(order_status_counts[2] or 0),
    }
    pager = paginate(total, page, ORDERS_PAGE)
    stmt = (
        filt.options(selectinload(Order.unit))
        .order_by(Order.id.desc())
        .offset(pager["offset"])
        .limit(pager["size"])
    )
    rows = list(db.scalars(stmt))
    for o in rows:
        o.status_label = STATUSES.get(o.status, o.status)  # type: ignore[attr-defined]
        o.badge = badge_for(o.status)  # type: ignore[attr-defined]
        o.prior_status_label = PRIOR_STATUS_LABELS.get(  # type: ignore[attr-defined]
            o.prior_status, o.prior_status
        )
    units = list(db.scalars(select(Unit).order_by(Unit.name)))
    qs = query_keep(unit_id=unit_id, status=status, q=q)
    return templates.TemplateResponse(
        request=request,
        name="orders.html",
        context=ctx(
            request,
            db,
            "orders",
            orders=rows,
            units=units,
            statuses=STATUSES,
            unit_id=unit_id,
            status=status,
            q=q,
            pager=pager,
            order_summary=order_summary,
            qs=qs,
        ),
    )


@app.get("/orders/{order_id}", response_class=HTMLResponse)
def order_detail(
    order_id: int,
    request: Request,
    page: int = 1,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    order = db.scalar(
        select(Order).options(selectinload(Order.unit)).where(Order.id == order_id)
    )
    if order is None:
        return RedirectResponse("/orders", status_code=303)
    total = db.scalar(select(func.count()).where(OrderEvent.order_id == order_id)) or 0
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
            .where(ImageTransfer.order_id == order_id)
            .group_by(ImageTransfer.status)
            .order_by(ImageTransfer.status)
        )
    )
    transfer_counts = dict(transfer_stats)
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
        count_rows = db.execute(
            select(
                HistoricalImageLink.historical_study_id,
                ImageTransfer.status,
                func.count(),
            )
            .join(
                ImageTransfer,
                ImageTransfer.id == HistoricalImageLink.transfer_id,
            )
            .where(
                HistoricalImageLink.historical_study_id.in_(historical_counts)
            )
            .group_by(
                HistoricalImageLink.historical_study_id,
                ImageTransfer.status,
            )
        )
        for study_id, transfer_status, count in count_rows:
            historical_counts[int(study_id)][str(transfer_status)] = int(count)
    historical_total_images = 0
    for study in historical_studies:
        counts = historical_counts[study.id]
        study.image_counts = counts  # type: ignore[attr-defined]
        study.image_total = sum(counts.values())  # type: ignore[attr-defined]
        historical_total_images += study.image_total  # type: ignore[attr-defined]
    return templates.TemplateResponse(
        request=request,
        name="order_detail.html",
        context=ctx(
            request,
            db,
            "orders",
            order=order,
            events=events,
            status_label=STATUSES.get(order.status, order.status),
            badge=badge_for(order.status),
            transfer_stats=transfer_stats,
            transfer_counts=transfer_counts,
            historical_studies=historical_studies,
            historical_total_images=historical_total_images,
            prior_status_label=PRIOR_STATUS_LABELS.get(
                order.prior_status, order.prior_status
            ),
            pager=pager,
            qs="",
        ),
    )


@app.post("/orders/{order_id}/retry")
def order_retry(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    order = db.get(Order, order_id)
    if (
        order
        and order.status not in ACTIVE_ORDER_STATUSES
        and order.prior_status not in ACTIVE_PRIOR_STATUSES
    ):
        _reset_order_for_reprocess(db, order)
        db.commit()
        log_event(
            log,
            logging.INFO,
            "order.reprocess",
            resource=f"order:{order.id}",
            status="success",
            order_id=order.id,
            unit_id=order.unit_id,
            user_id=user.id,
        )
        flash(request, "Pedido reiniciado para novo retrieve e envio.")
    elif order:
        flash(
            request, "Aguarde o processamento atual terminar para reprocessar.", "err"
        )
    return RedirectResponse(
        request.headers.get("referer") or "/orders", status_code=303
    )


@app.post("/orders/{order_id}/retry-prior")
def order_retry_prior(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    order = db.get(Order, order_id)
    if order is None:
        flash(request, "Pedido não encontrado.", "err")
        return RedirectResponse("/orders", status_code=303)
    if order.prior_status == "disabled":
        flash(request, "O retrieve histórico não está habilitado neste pedido.", "err")
    elif order.prior_status in ACTIVE_PRIOR_STATUSES:
        flash(request, "O retrieve histórico já está ativo.", "err")
    elif order.status in ACTIVE_ORDER_STATUSES:
        flash(request, "Aguarde o processamento atual terminar.", "err")
    else:
        for study in list(order.historical_studies):
            db.delete(study)
        order.prior_status = "queued"
        order.prior_due_at = datetime.now()
        order.prior_started_at = None
        order.prior_completed_at = None
        order.prior_heartbeat_at = None
        order.prior_attempts = 0
        order.prior_last_error = ""
        db.add(
            OrderEvent(
                order_id=order.id,
                level="info",
                message="Reprocessamento manual do histórico solicitado",
            )
        )
        db.commit()
        log_event(
            log,
            logging.INFO,
            "order.prior.reprocess",
            resource=f"order:{order.id}",
            status="success",
            order_id=order.id,
            unit_id=order.unit_id,
            user_id=user.id,
        )
        flash(request, "Retrieve histórico colocado novamente na fila.")
    return RedirectResponse(f"/orders/{order_id}", status_code=303)


def _reset_order_for_reprocess(db: Session, order: Order) -> None:
    order.correlation_id = new_correlation_id()
    order.attempts = 0
    order.done_at = None
    order.heartbeat_at = None
    order.last_error = ""
    if order.study_uid:
        order.status = "wait_retrieve"
        order.retrieve_at = datetime.now()
        _, _, order.second_retrieve_at = schedule_from_now(db, order.modality)
    else:
        order.status = "watching"
        order.last_find_at = None
        order.retrieve_at = None
        order.second_retrieve_at = None
        order.found_at = None
    db.add(
        OrderEvent(
            order_id=order.id,
            level="info",
            message="Reprocessamento manual solicitado",
        )
    )


@app.post("/orders/{order_id}/cancel")
def order_cancel(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    order = db.get(Order, order_id)
    if order and order.status not in ("done", "cancelled"):
        order.status = "cancelled"
        db.commit()
        flash(request, "Pedido cancelado.")
    return RedirectResponse(
        request.headers.get("referer") or "/orders", status_code=303
    )


def _delete_order_record(db: Session, order: Order) -> None:
    db.execute(
        update(ImageTransfer)
        .where(ImageTransfer.order_id == order.id)
        .values(order_id=None)
    )
    db.execute(delete(OrderEvent).where(OrderEvent.order_id == order.id))
    db.delete(order)


@app.post("/orders/{order_id}/delete")
def order_delete(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    order = db.get(Order, order_id)
    if order is None:
        flash(request, "Pedido não encontrado.", "err")
        return RedirectResponse("/orders", status_code=303)
    if (
        order.status in ACTIVE_ORDER_STATUSES
        or order.prior_status in ACTIVE_PRIOR_STATUSES
    ):
        flash(request, "Aguarde o processamento atual terminar para excluir.", "err")
        return RedirectResponse("/orders", status_code=303)
    unit_id = order.unit_id
    _delete_order_record(db, order)
    db.commit()
    log_event(
        log,
        logging.INFO,
        "order.delete",
        resource=f"order:{order_id}",
        status="success",
        order_id=order_id,
        unit_id=unit_id,
        user_id=user.id,
    )
    flash(request, "Pedido excluído. Os arquivos clínicos foram preservados.")
    return RedirectResponse("/orders", status_code=303)


@app.get("/settings", response_class=HTMLResponse)
def settings_page(
    request: Request, db: Session = Depends(get_db), user: User = Depends(require_user)
):
    settings = get_settings(db)
    settings_summary = {
        "cloud_host": urlparse(settings.cloud_url).hostname or "Não configurado",
        "drop_prefix": settings.drop_study_prefix or "—",
        "settle_seconds": settings.file_settle_seconds,
    }
    return templates.TemplateResponse(
        request=request,
        name="settings.html",
        context=ctx(
            request,
            db,
            "settings",
            settings=settings,
            settings_summary=settings_summary,
        ),
    )


@app.post("/settings")
def settings_save(
    request: Request,
    cloud_url: str = Form(..., min_length=1, max_length=500),
    drop_study_prefix: str = Form("SLRX"),
    file_settle_seconds: int = Form(3, ge=0, le=3600),
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    settings = get_settings(db)
    try:
        safe_cloud_url = validate_cloud_url(cloud_url)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return RedirectResponse("/settings", status_code=303)
    prefix = drop_study_prefix.strip().upper()
    if prefix and not re.fullmatch(r"[A-Z0-9_-]{1,32}", prefix):
        flash(request, "Prefixo de StudyID inválido.", "err")
        return RedirectResponse("/settings", status_code=303)
    settings.cloud_url = safe_cloud_url
    settings.drop_study_prefix = prefix
    settings.file_settle_seconds = file_settle_seconds
    db.commit()
    flash(request, "Configuração da nuvem salva.")
    return RedirectResponse("/settings", status_code=303)


@app.post("/settings/password")
def settings_password(
    request: Request,
    current: str = Form(...),
    new_password: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    if not verify_password(current, user.password_hash):
        flash(request, "Senha atual incorreta.", "err")
        return RedirectResponse("/settings", status_code=303)
    password_bytes = len(new_password.encode("utf-8"))
    if password_bytes < 12 or password_bytes > 72:
        flash(request, "A nova senha deve ter entre 12 e 72 bytes.", "err")
        return RedirectResponse("/settings", status_code=303)
    user.password_hash = hash_password(new_password)
    db.commit()
    flash(request, "Senha atualizada.")
    return RedirectResponse("/settings", status_code=303)
