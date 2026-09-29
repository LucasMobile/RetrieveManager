"""FastAPI application: middleware, error handlers and the route modules."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.sessions import SessionMiddleware

from app.config import (
    BASE_DIR,
    SECRET_KEY,
    SESSION_HTTPS_ONLY,
)
from app.db import init_db
from app.middleware import apply_security_headers, request_middleware
from app.observability import configure_logging
from app.routes import admin, auth, dashboard, orders, rules, units
from app.security import (
    verify_csrf,
)
from app.web import LoginRedirect, error_page, wants_html

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


@app.exception_handler(LoginRedirect)
async def _login_redirect(_request: Request, _exc: LoginRedirect):
    return RedirectResponse("/login", status_code=303)


@app.exception_handler(StarletteHTTPException)
async def _http_error(request: Request, exc: StarletteHTTPException):
    if not wants_html(request):
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
    return error_page(
        request, status_code=exc.status_code, title=title, message=message
    )


@app.exception_handler(RequestValidationError)
async def _request_validation_error(request: Request, exc: RequestValidationError):
    if not wants_html(request):
        return JSONResponse({"detail": jsonable_encoder(exc.errors())}, status_code=422)
    return error_page(
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
    if not wants_html(request):
        return apply_security_headers(
            request, JSONResponse({"detail": "erro interno"}, status_code=500)
        )
    response = error_page(
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


# Registration order is the matching order of the original single module.
app.include_router(auth.router)
app.include_router(dashboard.router)
app.include_router(units.router)
app.include_router(rules.router)
app.include_router(orders.router)
app.include_router(admin.router)
