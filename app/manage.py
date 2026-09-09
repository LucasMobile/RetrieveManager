from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from sqlalchemy import select

from app.config import ADMIN_PASSWORD, ADMIN_USER
from app.db import SessionLocal
from app.models import User
from app.observability import configure_logging, log_event
from app.security import hash_password

log = logging.getLogger("manage")


def reset_admin() -> None:
    """Synchronize the configured admin credentials with the database."""
    with SessionLocal.begin() as db:
        user = db.scalar(select(User).where(User.username == ADMIN_USER))
        created = user is None
        if user is None:
            user = User(username=ADMIN_USER, password_hash="")
            db.add(user)
        user.password_hash = hash_password(ADMIN_PASSWORD)
        db.flush()
        user_id = user.id

    log_event(
        log,
        logging.INFO,
        "admin.credentials_reset",
        resource="user",
        status="success",
        user_id=user_id,
        admin_created=created,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Administração do Retrieve Manager")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser(
        "reset-admin",
        help="sincroniza o usuário e a senha administrativos definidos no ambiente",
    )
    args = parser.parse_args(argv)

    configure_logging()
    if args.command == "reset-admin":
        reset_admin()
        print("Credencial administrativa sincronizada com o ambiente.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
