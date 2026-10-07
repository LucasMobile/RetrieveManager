"""Audit trail shared by the web routes and the background pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from app.models import AuditLog, User

if TYPE_CHECKING:
    from starlette.requests import Request


def audit(
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
    """Record an action; without ``user`` it is attributed to the system."""
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
