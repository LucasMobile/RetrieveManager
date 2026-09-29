import secrets

import bcrypt
from fastapi import HTTPException, Request


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except ValueError:
        return False


# Hash of a discarded random secret, same cost as gensalt(). Comparing against it
# when the username does not exist keeps both login failures equally slow.
_UNKNOWN_USER_HASH = "$2b$12$WelYZAmC0QeoVoe8RBI29.1wfUMBPwDJ4HITdvwXI78ItBEiE2XKG"


def verify_login(plain: str, hashed: str | None) -> bool:
    """Verify a login without revealing whether the account exists."""
    if hashed is None:
        verify_password(plain, _UNKNOWN_USER_HASH)
        return False
    return verify_password(plain, hashed)


def start_session(request: Request, user) -> None:
    """Bind the signed cookie to the user's current credential version."""
    request.session["user_id"] = user.id
    request.session["session_version"] = user.session_version


def revoke_sessions(user) -> None:
    """Invalidate every existing cookie of this user (e.g. after a new password)."""
    user.session_version = (user.session_version or 0) + 1


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
