from collections import defaultdict, deque
from threading import Lock
from time import monotonic

import bcrypt

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
