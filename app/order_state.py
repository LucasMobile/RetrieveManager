from __future__ import annotations

from app.models import Order

ACTIVE_ORDER_STATUSES = frozenset({"retrieving", "retrieving_second", "receiving"})
ACTIVE_PRIOR_STATUSES = frozenset({"queued", "retry_wait", "retrieving"})


def is_processing(order: Order) -> bool:
    """Return whether any current or historical retrieve is still in flight."""
    return (
        order.status in ACTIVE_ORDER_STATUSES
        or order.prior_status in ACTIVE_PRIOR_STATUSES
    )


def can_reprocess(order: Order) -> bool:
    return order.archived_at is None and not is_processing(order)


def can_archive(order: Order) -> bool:
    return order.archived_at is None and not is_processing(order)


def can_cancel(order: Order) -> bool:
    return (
        order.archived_at is None
        and order.status not in ACTIVE_ORDER_STATUSES | {"done", "cancelled"}
        and order.prior_status != "retrieving"
    )
