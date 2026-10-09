from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Order, OrderEvent


def add_event(
    db: Session,
    order: Order,
    message: str,
    detail: str = "",
    level: str = "info",
    *,
    kind: str = "",
) -> None:
    """Record an order event.

    With ``kind``, an event that repeats the order's latest one (same kind and
    level) updates that row instead: its message and detail become the latest,
    ``repeat_count`` grows and ``last_seen_at`` moves. A routine poll then
    leaves one line per stretch instead of one per attempt.
    """
    message = message[:500]
    detail = detail[-20000:] if detail else ""
    if kind:
        latest = db.scalar(
            select(OrderEvent)
            .where(OrderEvent.order_id == order.id)
            .order_by(OrderEvent.id.desc())
            .limit(1)
        )
        if latest is not None and latest.kind == kind and latest.level == level:
            latest.message = message
            latest.detail = detail
            latest.repeat_count = (latest.repeat_count or 1) + 1
            latest.last_seen_at = datetime.now()
            db.flush()
            return
    db.add(
        OrderEvent(
            order_id=order.id,
            message=message,
            detail=detail,
            level=level,
            kind=kind,
        )
    )
    db.flush()
