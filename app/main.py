from __future__ import annotations

import logging
import shutil
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from time import monotonic, perf_counter
from typing import Any, Literal
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
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

from app.compression import (
    compression_form_for_unit,
    compression_modalities_for_form,
    legacy_unit_compression_form,
    save_unit_compression_settings,
    validate_unit_compression_form,
)
from app.config import (
    BASE_DIR,
    DASHBOARD_FILE_COUNT_CACHE_SECONDS,
    DEFAULT_CLOUD_URL,
    SECRET_KEY,
    SESSION_HTTPS_ONLY,
)
from app.db import get_db, init_db
from app.dicom_rules import (
    ACTIONS,
    COMBINATORS,
    OPERATORS,
    VALUELESS_OPERATORS,
    dicom_tag_name,
    migrate_legacy_study_rule,
    normalize_dicom_tag,
    validate_rule_payload,
)
from app.dicom_tools import ToolMissing, c_echo
from app.middleware import apply_security_headers, request_middleware
from app.models import (
    STATUSES,
    AuditLog,
    CompressRule,
    DicomRule,
    DicomRuleApplication,
    DicomRuleCondition,
    DicomRuleUnit,
    DropModality,
    HistoricalImageLink,
    HistoricalSeries,
    HistoricalStudy,
    ImageTransfer,
    ManualMoveRequest,
    ModalityRule,
    Order,
    OrderEvent,
    Unit,
    User,
)
from app.netutil import port_listening
from app.observability import configure_logging, log_event, new_correlation_id
from app.order_state import (
    ACTIVE_ORDER_STATUSES,
    ACTIVE_PRIOR_STATUSES,
    can_archive,
    can_cancel,
    can_reprocess,
)
from app.pager import cursor_page_links, paginate, query_keep
from app.pipeline import folder_counts
from app.rate_limit import (
    apply_rate_limit_headers,
    client_ip,
    login_failure_rate_limiter,
)
from app.retention import archive_order, archive_unit
from app.rules import schedule_from_now
from app.security import (
    csrf_token,
    hash_password,
    verify_csrf,
    verify_password,
)
from app.validation import (
    validate_jpeg_flag,
    validate_modality,
    validate_pacs_connection,
    validate_unit_form,
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
}
AUDIT_RESOURCE_LABELS = {
    "unit": "Unidade",
    "order": "Pedido",
    "dicom_rule": "Regra DICOM",
    "retrieve_rule": "Regra de retrieve",
    "compress_rule": "Regra de compactação",
    "drop_rule": "Regra de descarte",
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
}
log = logging.getLogger("web")

_dashboard_stats_cache: dict[str, Any] = {"expires_at": 0.0}
_dashboard_stats_lock = Lock()
_dashboard_folder_cache: dict[
    tuple[int, str, str, str], tuple[float, dict[str, int]]
] = {}
_dashboard_folder_lock = Lock()

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
            "/account/password": "/account/password",
            "/users/new": "/users/new",
            "/rules/retrieve": "/rules/retrieve",
            "/rules/compress": "/rules/compress",
            "/rules/drop": "/rules/compress",
            "/rules": "/rules",
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
    user = getattr(request.state, "user", None) or current_user(request, db)
    data = {"request": request, "user": user, "nav": nav, "flash": _flash(request)}
    data.update(extra)
    return data


def _audit(
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
    if not invalid_size:
        user = db.scalar(select(User).where(User.username == clean_username))
    if user is None or not verify_password(password, user.password_hash):
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
    return apply_rate_limit_headers(
        RedirectResponse("/", status_code=303),
        login_failure_rate_limiter.check(client_id),
        scope="failed-login",
    )


@app.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


def _unit_runtime(unit: Unit) -> tuple[dict[str, int], bool]:
    cache_key = (
        unit.id,
        unit.receive_dir,
        unit.send_dir,
        unit.error_dir,
    )
    now = monotonic()
    with _dashboard_folder_lock:
        cached = _dashboard_folder_cache.get(cache_key)
        folders = dict(cached[1]) if cached and now < cached[0] else None
    if folders is None:
        folders = folder_counts(unit)
        with _dashboard_folder_lock:
            _dashboard_folder_cache[cache_key] = (
                now + max(1, DASHBOARD_FILE_COUNT_CACHE_SECONDS),
                dict(folders),
            )
    return (
        folders,
        port_listening(unit.store_port) if unit.enabled else False,
    )


def _dashboard_units(
    db: Session, page: int
) -> tuple[list[Unit], dict[str, int | bool], dict[str, int]]:
    total = (
        db.scalar(
            select(func.count()).select_from(Unit).where(Unit.deleted_at.is_(None))
        )
        or 0
    )
    pager = paginate(total, page, DASH_PAGE)
    units = list(
        db.scalars(
            select(Unit)
            .where(Unit.deleted_at.is_(None))
            .order_by(Unit.name)
            .offset(pager["offset"])
            .limit(pager["size"])
        )
    )
    with _dashboard_stats_lock:
        cache_valid = _dashboard_stats_cache.get("engine_id") == id(
            db.get_bind()
        ) and monotonic() < float(_dashboard_stats_cache.get("expires_at", 0.0))
        if cache_valid:
            stats = _dashboard_stats_cache["stats"]
            summary = _dashboard_stats_cache["summary"]
        else:
            stats: dict[int, tuple[int, int, int, int]] = {}
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
                .where(
                    Order.archived_at.is_(None),
                    or_(
                        Order.status.in_(
                            (
                                "watching",
                                "wait_retrieve",
                                "wait_second",
                                "retrieving",
                                "retrieving_second",
                                "error",
                            )
                        ),
                        Order.prior_status.in_(
                            ("queued", "retry_wait", "retrieving", "error")
                        ),
                    ),
                )
                .group_by(Order.unit_id)
            )
            for row in rows:
                stats[int(row[0])] = tuple(int(value or 0) for value in row[1:5])
            summary = {
                "enabled": int(
                    db.scalar(
                        select(func.count())
                        .select_from(Unit)
                        .where(
                            Unit.enabled.is_(True),
                            Unit.deleted_at.is_(None),
                        )
                    )
                    or 0
                ),
                "watching": sum(row[0] for row in stats.values()),
                "queue": sum(row[1] for row in stats.values()),
                "running": sum(row[2] for row in stats.values()),
                "error": sum(row[3] for row in stats.values()),
            }
            _dashboard_stats_cache.update(
                expires_at=monotonic() + 5,
                engine_id=id(db.get_bind()),
                stats=stats,
                summary=summary,
            )
    if units:
        with ThreadPoolExecutor(max_workers=len(units)) as executor:
            runtime = list(executor.map(_unit_runtime, units))
        for unit, (folders, store_up) in zip(units, runtime, strict=True):
            unit.folders = folders  # type: ignore[attr-defined]
            unit.store_up = store_up  # type: ignore[attr-defined]
    views = []
    for unit in units:
        watching, queue, running, errors = stats.get(unit.id, (0, 0, 0, 0))
        unit.counts = {  # type: ignore[attr-defined]
            "watching": watching,
            "queue": queue,
            "running": running,
            "error": errors,
        }
        views.append(unit)
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


@app.get("/units/new", response_class=HTMLResponse)
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
        ),
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
    obj.cloud_url = str(form["cloud_url"])
    obj.file_settle_seconds = int(form["file_settle_seconds"])
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


@app.post("/units/new")
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
        _audit(
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


@app.post("/units/test-echo")
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
        ),
    )


@app.post("/units/{unit_id}")
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
        _audit(
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


@app.post("/units/{unit_id}/toggle")
def units_toggle(
    unit_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    unit = db.get(Unit, unit_id)
    if unit and unit.deleted_at is None:
        unit.enabled = not unit.enabled
        _audit(
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


@app.post("/units/{unit_id}/delete")
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
        _audit(
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


@app.post("/units/{unit_id}/retry-errors")
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
    _audit(
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


@app.get("/rules", response_class=HTMLResponse)
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


@app.get("/rules/tag-info")
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


@app.post("/rules")
async def rules_dicom_create(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    try:
        data = _validated_dicom_rule_form(db, await request.form())
        rule = DicomRule()
        _store_dicom_rule(db, rule, data)
        _audit(
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


@app.post("/rules/dicom/{rule_id}")
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
        _audit(
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


@app.post("/rules/dicom/{rule_id}/toggle")
def rules_dicom_toggle(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    rule = db.get(DicomRule, rule_id)
    if rule is not None:
        rule.enabled = not rule.enabled
        _audit(
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


@app.post("/rules/dicom/{rule_id}/delete")
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
        _audit(
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


@app.get("/rules/retrieve", response_class=HTMLResponse)
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


@app.post("/rules/retrieve")
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
        _audit(
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


@app.post("/rules/retrieve/{rule_id}")
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
            _audit(
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


@app.post("/rules/retrieve/{rule_id}/delete")
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
    _audit(
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


@app.get("/rules/compress")
def rules_compress(user: User = Depends(require_admin)):
    return RedirectResponse("/units", status_code=303)


@app.post("/rules/compress")
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
        _audit(
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


@app.post("/rules/compress/{rule_id}")
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
            _audit(
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


@app.post("/rules/compress/{rule_id}/delete")
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
    _audit(
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


@app.post("/rules/drop")
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
        _audit(
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


@app.post("/rules/drop/{drop_id}/delete")
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
        _audit(
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


@app.get("/orders", response_class=HTMLResponse)
def orders_list(
    request: Request,
    unit_id: str = Query("", max_length=20),
    status: str = Query("", max_length=32),
    q: str = Query("", max_length=200),
    before: int | None = Query(None, ge=1),
    after: int | None = Query(None, ge=1),
    last: bool = False,
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE),
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    parsed_unit_id = _parse_optional_unit_id(unit_id)
    return _orders_response(
        request,
        db,
        unit_id=parsed_unit_id,
        status=status,
        q=q,
        before=before,
        after=after,
        last=last,
        page=page,
        page_size=page_size,
        history=False,
    )


@app.get("/orders/history", response_class=HTMLResponse)
def orders_history(
    request: Request,
    unit_id: str = Query("", max_length=20),
    status: str = Query("", max_length=32),
    q: str = Query("", max_length=200),
    before: int | None = Query(None, ge=1),
    after: int | None = Query(None, ge=1),
    last: bool = False,
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    parsed_unit_id = _parse_optional_unit_id(unit_id)
    return _orders_response(
        request,
        db,
        unit_id=parsed_unit_id,
        status=status,
        q=q,
        before=before,
        after=after,
        last=last,
        page=page,
        page_size=page_size,
        history=True,
    )


def _parse_optional_unit_id(value: str) -> int | None:
    """Convert the unit filter while treating the form's empty option as unset."""
    if value == "":
        return None
    if not value.isdecimal():
        raise StarletteHTTPException(422, "Unidade inválida")
    unit_id = int(value)
    if unit_id < 1:
        raise StarletteHTTPException(422, "Unidade inválida")
    return unit_id


def _cursor_page_cursors(
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


def _orders_response(
    request: Request,
    db: Session,
    *,
    unit_id: int | None,
    status: str,
    q: str,
    before: int | None,
    after: int | None,
    last: bool,
    page: int,
    page_size: int,
    history: bool,
):
    if status and status not in STATUSES:
        raise StarletteHTTPException(422, "Status inválido")
    if page_size not in PAGE_SIZE_OPTIONS:
        raise StarletteHTTPException(422, "Quantidade de itens por página inválida")
    if sum((before is not None, after is not None, last)) > 1:
        raise StarletteHTTPException(422, "Use apenas um cursor de paginação")
    if page > 1 and before is None and after is None and not last:
        raise StarletteHTTPException(422, "Cursor de paginação ausente")
    archived_filter = (
        Order.archived_at.is_not(None) if history else Order.archived_at.is_(None)
    )
    filt = select(Order).where(archived_filter)
    if unit_id:
        filt = filt.where(Order.unit_id == unit_id)
    if status:
        filt = filt.where(Order.status == status)
    if q:
        like = f"%{q.strip()}%"
        filt = filt.where(
            or_(
                Order.acc.ilike(like),
                Order.pat_id.ilike(like),
                Order.source_id.ilike(like),
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
                        | filtered_orders.c.prior_status.in_(("queued", "retry_wait")),
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
    pages = max(1, (total + page_size - 1) // page_size)
    if last:
        page = pages
    elif before is None and after is None:
        page = 1
    pager = paginate(total, page, page_size)
    pager["size_options"] = PAGE_SIZE_OPTIONS
    pager["keep"] = {"unit_id": unit_id, "status": status, "q": q}
    stmt = filt.options(selectinload(Order.unit))
    if last:
        stmt = stmt.order_by(Order.id.asc())
    elif before is not None:
        stmt = stmt.where(Order.id < before).order_by(Order.id.desc())
    elif after is not None:
        stmt = stmt.where(Order.id > after).order_by(Order.id.asc())
    else:
        stmt = stmt.order_by(Order.id.desc())
    result_limit = (total - pager["offset"]) if last else pager["size"]
    stmt = stmt.limit(result_limit)
    rows = list(db.scalars(stmt))
    if after is not None or last:
        rows.reverse()
    for o in rows:
        o.status_label = STATUSES.get(o.status, o.status)  # type: ignore[attr-defined]
        o.badge = badge_for(o.status)  # type: ignore[attr-defined]
        o.can_reprocess = can_reprocess(o)  # type: ignore[attr-defined]
        o.can_archive = can_archive(o)  # type: ignore[attr-defined]
        o.prior_status_label = PRIOR_STATUS_LABELS.get(  # type: ignore[attr-defined]
            o.prior_status, o.prior_status
        )
    has_prev = False
    has_next = False
    if rows:
        has_prev = (
            db.scalar(
                filt.where(Order.id > rows[0].id).with_only_columns(Order.id).limit(1)
            )
            is not None
        )
        has_next = (
            db.scalar(
                filt.where(Order.id < rows[-1].id).with_only_columns(Order.id).limit(1)
            )
            is not None
        )
    pager.update(
        cursor=True,
        has_prev=has_prev,
        has_next=has_next,
        prev_cursor=rows[0].id if rows else None,
        next_cursor=rows[-1].id if rows else None,
    )

    pager["page_links"] = cursor_page_links(
        pager,
        page_cursors=_cursor_page_cursors(db, filt, Order.id, pager),
    )
    units = list(db.scalars(select(Unit).order_by(Unit.name)))
    qs = query_keep(unit_id=unit_id, status=status, q=q, page_size=page_size)
    return_to = request.url.path
    if request.url.query:
        return_to = f"{return_to}?{request.url.query}"
    return templates.TemplateResponse(
        request=request,
        name="orders.html",
        context=ctx(
            request,
            db,
            "order_history" if history else "orders",
            orders=rows,
            units=units,
            statuses=STATUSES,
            unit_id=unit_id,
            status=status,
            q=q,
            pager=pager,
            order_summary=order_summary,
            qs=qs,
            history=history,
            orders_path="/orders/history" if history else "/orders",
            detail_return_qs=query_keep(return_to=return_to),
        ),
    )


def _image_status_summary(counts: dict[str, int]) -> dict[str, int]:
    return {
        "total": sum(counts.values()),
        "uploaded": counts.get("uploaded", 0),
        "compressed": counts.get("compressed", 0),
        "errors": sum(
            counts.get(status, 0)
            for status in (
                "compression_error",
                "upload_error",
                "rule_error",
                "metadata_error",
                "file_missing",
            )
        ),
        "discarded": sum(
            counts.get(status, 0)
            for status in (
                "discarded_modality",
                "discarded_study",
                "discarded_rule",
            )
        ),
    }


@app.get("/orders/{order_id}", response_class=HTMLResponse)
def order_detail(
    order_id: int,
    request: Request,
    page: int = 1,
    return_to: str = Query("", max_length=2048),
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    order = db.scalar(
        select(Order).options(selectinload(Order.unit)).where(Order.id == order_id)
    )
    if order is None:
        return RedirectResponse("/orders", status_code=303)
    default_return_path = "/orders/history" if order.archived_at else "/orders"
    parsed_return = urlsplit(return_to)
    if (
        not return_to
        or parsed_return.scheme
        or parsed_return.netloc
        or parsed_return.path != default_return_path
    ):
        return_to = default_return_path
    order.can_reprocess = can_reprocess(order)  # type: ignore[attr-defined]
    order.can_archive = can_archive(order)  # type: ignore[attr-defined]
    order.can_cancel = can_cancel(order)  # type: ignore[attr-defined]
    manual_move_active = bool(
        db.scalar(
            select(func.count()).where(
                ManualMoveRequest.order_id == order_id,
                ManualMoveRequest.status.in_(("queued", "running")),
            )
        )
    )
    can_manual_move = bool(
        order.study_uid
        and order.archived_at is None
        and order.status not in ACTIVE_ORDER_STATUSES
        and order.status != "cancelled"
        and not manual_move_active
    )
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
            .where(
                ImageTransfer.order_id == order_id,
                ~ImageTransfer.id.in_(
                    select(HistoricalImageLink.transfer_id)
                    .join(
                        HistoricalStudy,
                        HistoricalStudy.id
                        == HistoricalImageLink.historical_study_id,
                    )
                    .where(HistoricalStudy.order_id == order_id)
                ),
            )
            .group_by(ImageTransfer.status)
            .order_by(ImageTransfer.status)
        )
    )
    transfer_counts = dict(transfer_stats)
    image_summary = _image_status_summary(transfer_counts)
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
            .where(HistoricalImageLink.historical_study_id.in_(historical_counts))
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
        study.image_summary = _image_status_summary(counts)  # type: ignore[attr-defined]
        historical_total_images += study.image_summary["total"]  # type: ignore[attr-defined]
    return templates.TemplateResponse(
        request=request,
        name="order_detail.html",
        context=ctx(
            request,
            db,
            "order_history" if order.archived_at else "orders",
            order=order,
            events=events,
            status_label=STATUSES.get(order.status, order.status),
            badge=badge_for(order.status),
            transfer_counts=transfer_counts,
            image_summary=image_summary,
            can_manual_move=can_manual_move,
            manual_move_active=manual_move_active,
            historical_studies=historical_studies,
            historical_total_images=historical_total_images,
            prior_status_label=PRIOR_STATUS_LABELS.get(
                order.prior_status, order.prior_status
            ),
            pager=pager,
            qs=query_keep(return_to=return_to),
            return_to=return_to,
            detail_return_qs=query_keep(return_to=return_to),
        ),
    )


@app.post("/orders/{order_id}/retrieve-now")
def order_retrieve_now(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    order = db.get(Order, order_id)
    if order is None:
        flash(request, "Pedido não encontrado.", "err")
        return RedirectResponse("/orders", status_code=303)
    if order.archived_at is not None:
        flash(request, "Pedidos arquivados são somente para consulta.", "err")
    elif not order.study_uid:
        flash(request, "Aguarde o C-FIND localizar o exame atual.", "err")
    elif order.status in ACTIVE_ORDER_STATUSES:
        flash(request, "Já existe um C-MOVE do exame atual em andamento.", "err")
    elif order.status == "cancelled":
        flash(request, "O pedido está cancelado.", "err")
    elif db.scalar(
        select(ManualMoveRequest.id).where(
            ManualMoveRequest.order_id == order.id,
            ManualMoveRequest.status.in_(("queued", "running")),
        )
    ):
        flash(request, "O C-MOVE manual já está na fila ou em andamento.", "err")
    else:
        correlation_id = new_correlation_id()
        db.add(
            ManualMoveRequest(
                order_id=order.id,
                unit_id=order.unit_id,
                requested_by_user_id=user.id,
                requested_by_username=user.username,
                correlation_id=correlation_id,
                status="queued",
            )
        )
        db.add(
            OrderEvent(
                order_id=order.id,
                level="info",
                message="C-MOVE manual do exame atual solicitado",
            )
        )
        _audit(
            db,
            request,
            user,
            action="retry",
            resource_type="order",
            resource_id=order.id,
            resource_name=order.acc,
            summary="C-MOVE manual do exame atual colocado na fila.",
        )
        db.commit()
        log_event(
            log,
            logging.INFO,
            "order.current_move.request",
            resource=f"order:{order.id}",
            status="success",
            order_id=order.id,
            unit_id=order.unit_id,
            user_id=user.id,
            correlation_id=correlation_id,
        )
        flash(request, "C-MOVE do exame atual colocado na fila imediata.")
    return RedirectResponse(f"/orders/{order_id}", status_code=303)


@app.post("/orders/{order_id}/retry")
def order_retry(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    order = db.get(Order, order_id)
    if order and can_reprocess(order):
        _reset_order_for_reprocess(db, order)
        _audit(
            db,
            request,
            user,
            action="retry",
            resource_type="order",
            resource_id=order.id,
            resource_name=order.acc,
            summary="Pedido reiniciado para novo retrieve e envio.",
        )
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
    user: User = Depends(require_admin),
):
    order = db.get(Order, order_id)
    if order is None:
        flash(request, "Pedido não encontrado.", "err")
        return RedirectResponse("/orders", status_code=303)
    if order.archived_at is not None:
        flash(request, "Pedidos arquivados são somente para consulta.", "err")
        return RedirectResponse(f"/orders/{order_id}", status_code=303)
    if order.prior_status == "disabled":
        flash(request, "O retrieve histórico não está habilitado neste pedido.", "err")
    elif order.prior_status in ACTIVE_PRIOR_STATUSES:
        flash(request, "O retrieve histórico já está ativo.", "err")
    elif order.status in ACTIVE_ORDER_STATUSES:
        flash(request, "Aguarde o processamento atual terminar.", "err")
    else:
        for study in list(order.historical_studies):
            db.delete(study)
        db.execute(
            delete(HistoricalSeries).where(HistoricalSeries.order_id == order.id)
        )
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
        _audit(
            db,
            request,
            user,
            action="retry",
            resource_type="order",
            resource_id=order.id,
            resource_name=order.acc,
            summary="Retrieve histórico colocado novamente na fila.",
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
    if order.prior_status == "cancelled":
        if order.unit.retrieve_prior_enabled:
            order.prior_status = "queued"
            order.prior_due_at = datetime.now()
            order.prior_started_at = None
            order.prior_completed_at = None
            order.prior_heartbeat_at = None
            order.prior_attempts = 0
            order.prior_last_error = ""
        else:
            order.prior_status = "disabled"
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
    user: User = Depends(require_admin),
):
    order = db.get(Order, order_id)
    running_manual = bool(
        order
        and db.scalar(
            select(ManualMoveRequest.id).where(
                ManualMoveRequest.order_id == order.id,
                ManualMoveRequest.status == "running",
            )
        )
    )
    if order and can_cancel(order) and not running_manual:
        queued_manual = list(
            db.scalars(
                select(ManualMoveRequest).where(
                    ManualMoveRequest.order_id == order.id,
                    ManualMoveRequest.status == "queued",
                )
            )
        )
        for move_request in queued_manual:
            move_request.status = "cancelled"
            move_request.completed_at = datetime.now()
            move_request.last_error = "Cancelado junto com o pedido"
        if order.prior_status in {"queued", "retry_wait"}:
            order.prior_status = "cancelled"
            order.prior_completed_at = datetime.now()
            order.prior_last_error = "Cancelado junto com o pedido"
        order.status = "cancelled"
        _audit(
            db,
            request,
            user,
            action="cancel",
            resource_type="order",
            resource_id=order.id,
            resource_name=order.acc,
            summary="Processamento do pedido cancelado.",
        )
        db.commit()
        flash(request, "Pedido cancelado.")
    elif order:
        flash(
            request,
            "Aguarde o retrieve atual terminar antes de cancelar.",
            "err",
        )
    return RedirectResponse(
        request.headers.get("referer") or "/orders", status_code=303
    )


def _delete_order_record(
    db: Session,
    order: Order,
    *,
    actor_id: int | None = None,
    actor_username: str = "Sistema",
    reason: str = "Pedido arquivado manualmente.",
) -> None:
    archive_order(
        db,
        order,
        reason=reason,
        actor_id=actor_id,
        actor_username=actor_username,
    )


@app.post("/orders/{order_id}/delete")
def order_delete(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    order = db.get(Order, order_id)
    if order is None:
        flash(request, "Pedido não encontrado.", "err")
        return RedirectResponse("/orders", status_code=303)
    if order.archived_at is not None:
        flash(request, "O pedido já está arquivado.", "err")
        return RedirectResponse("/orders/history", status_code=303)
    if not can_archive(order):
        flash(request, "Aguarde o processamento atual terminar para excluir.", "err")
        return RedirectResponse("/orders", status_code=303)
    unit_id = order.unit_id
    accession = order.acc
    _delete_order_record(
        db,
        order,
        actor_id=user.id,
        actor_username=user.username,
    )
    _audit(
        db,
        request,
        user,
        action="archive",
        resource_type="order",
        resource_id=order_id,
        resource_name=accession,
        summary="Pedido arquivado com eventos e arquivos clínicos preservados.",
    )
    db.commit()
    log_event(
        log,
        logging.INFO,
        "order.archive",
        resource=f"order:{order_id}",
        status="success",
        order_id=order_id,
        unit_id=unit_id,
        user_id=user.id,
    )
    flash(request, "Pedido arquivado. Todo o histórico foi preservado.")
    return RedirectResponse("/orders", status_code=303)


def _normalize_username(value: str) -> str:
    username = value.strip()
    if not 3 <= len(username) <= 80:
        raise ValueError("O usuário deve ter entre 3 e 80 caracteres.")
    if not all(char.isalnum() or char in "._-@" for char in username):
        raise ValueError(
            "O usuário aceita apenas letras, números, ponto, hífen, sublinhado e @."
        )
    return username


def _validate_password(password: str, confirmation: str) -> None:
    if password != confirmation:
        raise ValueError("A confirmação da senha não confere.")
    password_bytes = len(password.encode("utf-8"))
    if not 12 <= password_bytes <= 72:
        raise ValueError("A senha deve ter entre 12 e 72 bytes.")


def _admin_count(db: Session) -> int:
    return (
        db.scalar(select(func.count()).select_from(User).where(User.role == "admin"))
        or 0
    )


@app.get("/logs", response_class=HTMLResponse)
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
        page_cursors=_cursor_page_cursors(db, filtered, AuditLog.id, pager),
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


@app.get("/settings")
def settings_redirect(user: User = Depends(require_admin)):
    return RedirectResponse("/users", status_code=303)


@app.get("/users", response_class=HTMLResponse)
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


@app.get("/users/new", response_class=HTMLResponse)
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


@app.post("/users/new")
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
        clean_username = _normalize_username(username)
        _validate_password(password, password_confirmation)
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
        _audit(
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


@app.get("/users/{managed_user_id}", response_class=HTMLResponse)
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


@app.post("/users/{managed_user_id}")
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
            _validate_password(new_password, password_confirmation)
        except ValueError as exc:
            flash(request, str(exc), "err")
            return RedirectResponse(f"/users/{managed_user_id}", status_code=303)
        managed_user.password_hash = hash_password(new_password)
    managed_user.role = role
    update_summary = f"Perfil definido como {role}."
    if new_password:
        update_summary += " Senha redefinida pelo administrador."
    _audit(
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


@app.post("/users/{managed_user_id}/delete")
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
    _audit(
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


@app.get("/account/password", response_class=HTMLResponse)
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


@app.post("/account/password")
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
        _validate_password(new_password, password_confirmation)
    except ValueError as exc:
        flash(request, str(exc), "err")
        return RedirectResponse("/account/password", status_code=303)
    user.password_hash = hash_password(new_password)
    _audit(
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
