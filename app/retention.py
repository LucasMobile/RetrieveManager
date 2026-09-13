from datetime import datetime

from sqlalchemy.orm import Session

from app.models import Order, OrderEvent, Unit


def archive_order(
    db: Session,
    order: Order,
    *,
    reason: str,
    actor_id: int | None = None,
    actor_username: str = "Sistema",
    archived_at: datetime | None = None,
) -> bool:
    """Remove an order from operational views without destroying evidence."""
    if order.archived_at is not None:
        return False
    order.archived_at = archived_at or datetime.now()
    order.archive_reason = reason[:255]
    order.archived_by_user_id = actor_id
    order.archived_by_username = actor_username[:80]
    db.add(
        OrderEvent(
            order_id=order.id,
            level="info",
            message="Pedido arquivado",
            detail=reason[:500],
        )
    )
    return True


def archive_unit(
    unit: Unit,
    *,
    actor_id: int | None,
    actor_username: str,
    archived_at: datetime | None = None,
) -> bool:
    """Disable a unit while retaining its configuration and related records."""
    if unit.deleted_at is not None:
        return False
    unit.enabled = False
    unit.deleted_at = archived_at or datetime.now()
    unit.deleted_by_user_id = actor_id
    unit.deleted_by_username = actor_username[:80]
    return True
