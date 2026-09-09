from __future__ import annotations

import logging
import re
from time import perf_counter
from urllib.parse import urlparse

from fastapi import Request
from fastapi.responses import JSONResponse, Response
from starlette.middleware.base import RequestResponseEndpoint

from app.observability import log_context, log_event, new_correlation_id

log = logging.getLogger("web.request")
_SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
_REQUEST_ID = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")


def _request_id(request: Request) -> str:
    supplied = request.headers.get("x-request-id", "")
    return supplied if _REQUEST_ID.fullmatch(supplied) else new_correlation_id()


def _same_origin(request: Request) -> bool:
    if request.method in _SAFE_METHODS:
        return True
    fetch_site = request.headers.get("sec-fetch-site", "")
    if fetch_site and fetch_site not in {"same-origin", "none"}:
        return False
    source = request.headers.get("origin") or request.headers.get("referer")
    if not source:
        return True  # Non-browser clients do not always send either header.
    source_url = urlparse(source)
    return source_url.netloc.lower() == request.headers.get("host", "").lower()


async def request_middleware(
    request: Request, call_next: RequestResponseEndpoint
) -> Response:
    correlation_id = _request_id(request)
    request.state.correlation_id = correlation_id
    started_at = perf_counter()
    resource = request.url.path
    with log_context(correlation_id):
        log_event(
            log, logging.INFO, "http.request", resource=resource, status="started"
        )
        if not _same_origin(request):
            response: Response = JSONResponse(
                {"detail": "origem da requisição não permitida"}, status_code=403
            )
            status = "rejected"
        else:
            try:
                response = await call_next(request)
                if response.status_code < 400:
                    status = "success"
                elif response.status_code < 500:
                    status = "rejected"
                else:
                    status = "failure"
            except Exception as exc:
                log_event(
                    log,
                    logging.ERROR,
                    "http.request",
                    resource=resource,
                    status="failure",
                    started_at=started_at,
                    error=exc,
                )
                raise

        session = request.scope.get("session", {})
        user_id = session.get("user_id") if isinstance(session, dict) else None
        log_event(
            log,
            logging.INFO,
            "http.request",
            resource=resource,
            status=status,
            started_at=started_at,
            user_id=user_id,
            http_method=request.method,
            http_status=response.status_code,
        )
        response.headers["X-Request-ID"] = correlation_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=()"
        )
        if not resource.startswith("/static/"):
            response.headers["Cache-Control"] = "no-store"
        if (
            request.url.scheme == "https"
            or request.headers.get("x-forwarded-proto", "").lower() == "https"
        ):
            response.headers["Strict-Transport-Security"] = (
                "max-age=31536000; includeSubDomains"
            )
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; style-src 'self'; script-src 'self'; "
            "img-src 'self' data:; "
            "connect-src 'self'; frame-ancestors 'none'; form-action 'self'"
        )
        return response
