from sqlalchemy.orm import Session

from app.models import Order, OrderEvent


def add_event(
    db: Session, order: Order, message: str, detail: str = "", level: str = "info"
) -> None:
    db.add(
        OrderEvent(
            order_id=order.id,
            message=message[:500],
            detail=detail[-20000:] if detail else "",
            level=level,
        )
    )
    db.flush()
