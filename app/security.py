import secrets
from collections import defaultdict, deque
from threading import Lock
from time import monotonic

import bcrypt
from fastapi import HTTPException, Request

_LOGIN_WINDOW_SECONDS = 300
_LOGIN_MAX_FAILURES = 10
_login_failures: dict[str, deque[float]] = defaultdict(deque)
_login_lock = Lock()


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except ValueError:
        return False


def login_allowed(client_id: str) -> bool:
    now = monotonic()
    with _login_lock:
        failures = _login_failures[client_id]
        while failures and now - failures[0] > _LOGIN_WINDOW_SECONDS:
            failures.popleft()
        return len(failures) < _LOGIN_MAX_FAILURES


def record_login_failure(client_id: str) -> None:
    with _login_lock:
        _login_failures[client_id].append(monotonic())


def clear_login_failures(client_id: str) -> None:
    with _login_lock:
        _login_failures.pop(client_id, None)


def csrf_token(request: Request) -> str:
    token = request.session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        request.session["csrf_token"] = token
    return token


async def verify_csrf(request: Request) -> None:
    if request.method in {"GET", "HEAD", "OPTIONS"}:
        return
    expected = request.session.get("csrf_token")
    supplied = request.headers.get("x-csrf-token")
    if supplied is None:
        supplied = (await request.form()).get("csrf_token")
    if (
        not isinstance(expected, str)
        or not isinstance(supplied, str)
        or not secrets.compare_digest(expected.encode(), supplied.encode())
    ):
        raise HTTPException(403, "Token CSRF inválido; recarregue a página.")
