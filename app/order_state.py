from __future__ import annotations

from datetime import date, datetime, timedelta

from app.models import Order

ACTIVE_ORDER_STATUSES = frozenset({"retrieving", "retrieving_update", "receiving"})
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


def prior_date_range(today: date | None = None) -> tuple[str, str]:
    current = today or date.today()
    try:
        start = current.replace(year=current.year - 3)
    except ValueError:
        start = current.replace(year=current.year - 3, day=28)
    end = current - timedelta(days=1)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def queue_prior_retrieve(
    order: Order, now: datetime, *, refresh_window: bool = False
) -> None:
    """Queue the historical retrieve for immediate execution with fresh counters.

    ``refresh_window`` recomputes the study-date window from ``now``; otherwise
    the window chosen when the order was first queued is kept.
    """
    order.prior_status = "queued"
    if refresh_window:
        order.prior_date_from, order.prior_date_to = prior_date_range(now.date())
    order.prior_due_at = now
    order.prior_started_at = None
    order.prior_completed_at = None
    order.prior_heartbeat_at = None
    order.prior_attempts = 0
    order.prior_last_error = ""
