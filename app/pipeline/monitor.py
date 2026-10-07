"""Monitoring of the current study after the 1st retrieve.

Exams keep reaching the PACS long after they are first found. When the 1st
C-MOVE ends, an order whose modality has monitoring enabled stays in
``monitoring``: every interval a SERIES-level C-FIND compares each
NumberOfSeriesRelatedInstances with the images already received, and the
series that grew are fetched again (``wait_update`` → ``retrieving_update``).
The window closes ``max_hours`` after the 1st retrieve; then the order is done.

The comparison counts every image received, whatever happened to it later
(compacted, discarded or failed): a compression or upload failure is not
solved by asking the PACS again.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from time import perf_counter

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.config import FIND_TIMEOUT_SECONDS
from app.dicom_net import FindResult, PacsNode, find_study_series
from app.events import add_event
from app.models import DicomInstance, DicomStudy, Order, Unit
from app.observability import log_context, log_event, safe_error_detail
from app.pipeline.common import ensure_order_correlation, log
from app.rules import MonitorPlan
from app.wording import counted


@dataclass(frozen=True)
class UpdatePlan:
    """Outcome of one monitoring check.

    ``series`` lists the series to fetch; ``whole_study`` asks for everything
    because the PACS did not report the counts. Neither means nothing is new.
    """

    pacs_total: int = 0
    received_total: int = 0
    series_count: int = 0
    series: tuple[str, ...] = ()
    whole_study: bool = False
    detail: str = ""

    @property
    def has_news(self) -> bool:
        return self.whole_study or bool(self.series)


def received_series_counts(db: Session, unit_id: int, study_uid: str) -> dict[str, int]:
    """Distinct SOP instances received per series of a study, in any state."""
    rows = db.execute(
        select(
            DicomInstance.series_uid, func.count(func.distinct(DicomInstance.sop_uid))
        )
        .join(DicomStudy, DicomStudy.id == DicomInstance.study_id)
        .where(
            DicomInstance.unit_id == unit_id,
            DicomStudy.study_uid == study_uid,
        )
        .group_by(DicomInstance.series_uid)
    ).all()
    return {str(series_uid or ""): int(count) for series_uid, count in rows}


def plan_update(found: FindResult, received: dict[str, int]) -> UpdatePlan:
    """Compare the PACS series counts with what was received."""
    received_total = sum(received.values())
    lines = [found.summary]
    pacs_total = 0
    pending: list[str] = []
    counts_missing = False
    for response in found.responses:
        series_uid = str(response.get("SeriesInstanceUID", "") or "").strip()
        try:
            expected = int(response.get("NumberOfSeriesRelatedInstances"))
        except TypeError, ValueError:
            expected = -1
        held = received.get(series_uid, 0)
        if not series_uid or expected < 0:
            counts_missing = True
            lines.append(
                f"Série {series_uid or '?'}: PACS sem contagem; recebidas {held}"
            )
            continue
        pacs_total += expected
        mark = " (nova)" if expected > held else ""
        lines.append(f"Série {series_uid}: PACS {expected}; recebidas {held}{mark}")
        if expected > held:
            pending.append(series_uid)
    return UpdatePlan(
        pacs_total=pacs_total,
        received_total=received_total,
        series_count=len(found.responses),
        series=() if counts_missing else tuple(pending),
        whole_study=counts_missing,
        detail="\n".join(lines),
    )


def start_monitoring(order: Order, plan: MonitorPlan, now: datetime) -> str:
    """Open the monitoring window; return the timeline sentence for it."""
    order.status = "monitoring"
    order.monitor_interval_minutes = plan.interval_minutes
    order.monitor_until = now + timedelta(hours=plan.max_hours)
    order.monitor_next_at = min(
        now + timedelta(minutes=plan.interval_minutes), order.monitor_until
    )
    order.monitor_checks = 0
    order.monitor_new_images = 0
    order.monitor_pending_series = ""
    order.heartbeat_at = None
    order.done_at = None
    return (
        f"Monitoramento de novas imagens a cada {plan.interval_minutes} min "
        f"até {order.monitor_until:%d/%m às %H:%M}"
    )


def reset_monitoring(order: Order) -> None:
    order.monitor_interval_minutes = 0
    order.monitor_next_at = None
    order.monitor_until = None
    order.monitor_checks = 0
    order.monitor_new_images = 0
    order.monitor_pending_series = ""


def continue_monitoring(order: Order, now: datetime) -> str:
    """Schedule the next check, or close the window once it has passed.

    The last check runs at the window end itself. Returns the sentence that
    completes the event of the step that just finished.
    """
    order.monitor_pending_series = ""
    order.heartbeat_at = None
    if order.monitor_until is None or now >= order.monitor_until:
        _close(order, now)
        return f"Monitoramento de novas imagens encerrado: {_totals(order)}."
    order.status = "monitoring"
    interval = timedelta(minutes=max(1, order.monitor_interval_minutes or 5))
    order.monitor_next_at = min(now + interval, order.monitor_until)
    return f"Próxima verificação às {order.monitor_next_at:%H:%M}."


def finish_monitoring(
    db: Session, order: Order, now: datetime, stopped_by: str
) -> None:
    """Close the window on request, before its end."""
    _close(order, now)
    add_event(
        db,
        order,
        f"Monitoramento de novas imagens encerrado manualmente por {stopped_by}: "
        f"{_totals(order)}",
    )


def _close(order: Order, now: datetime) -> None:
    order.status = "done"
    order.done_at = now
    order.monitor_next_at = None
    order.monitor_pending_series = ""
    order.heartbeat_at = None


def _totals(order: Order) -> str:
    return (
        f"{counted(order.monitor_checks, 'verificação', 'verificações')}, "
        + counted(
            order.monitor_new_images,
            "imagem nova recebida",
            "imagens novas recebidas",
        )
    )


def check_monitoring(db: Session, unit: Unit, max_orders: int = 1) -> int:
    """Run the due monitoring checks of a unit; return how many were claimed."""
    now = datetime.now()
    # Same in-flight claim as the C-FIND of new orders: a check that died
    # with the worker is picked up again once its heartbeat is old.
    in_flight_cutoff = now - timedelta(seconds=max(180, 2 * FIND_TIMEOUT_SECONDS))
    orders = list(
        db.scalars(
            select(Order)
            .where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                Order.status == "monitoring",
                Order.monitor_next_at.is_not(None),
                Order.monitor_next_at <= now,
                or_(
                    Order.heartbeat_at.is_(None),
                    Order.heartbeat_at <= in_flight_cutoff,
                ),
            )
            .order_by(Order.monitor_next_at, Order.id)
            .limit(max_orders)
            .with_for_update(skip_locked=True)
        )
    )
    for order in orders:
        order.heartbeat_at = now
    if orders:
        db.commit()
    for order in orders:
        order_id = order.id
        try:
            _check_one(db, unit, order)
        except Exception as exc:
            db.rollback()
            failed = db.get(Order, order_id)
            if failed is not None and failed.status == "monitoring":
                next_step = continue_monitoring(failed, datetime.now())
                add_event(
                    db,
                    failed,
                    f"Verificação de novas imagens interrompida. {next_step}",
                    safe_error_detail(exc),
                    "warn",
                )
                db.commit()
            log_event(
                log,
                logging.ERROR,
                "dicom.monitor.check",
                resource=f"order:{order_id}",
                status="failure",
                error=exc,
                order_id=order_id,
                unit_id=unit.id,
            )
    return len(orders)


def _check_one(db: Session, unit: Unit, order: Order) -> None:
    correlation_id = ensure_order_correlation(order)
    started_at = perf_counter()
    with log_context(correlation_id):
        found = find_study_series(
            PacsNode.from_unit(unit), order.study_uid, timeout=FIND_TIMEOUT_SECONDS
        )
        received = received_series_counts(db, unit.id, order.study_uid)
        # The order may have been cancelled, stopped or archived meanwhile.
        db.refresh(order, with_for_update=True)
        if order.status != "monitoring" or order.archived_at is not None:
            order.heartbeat_at = None
            db.commit()
            return
        now = datetime.now()
        order.monitor_checks += 1
        number = order.monitor_checks
        if not found.ok:
            next_step = continue_monitoring(order, now)
            add_event(
                db,
                order,
                f"Verificação {number}: C-FIND falhou ({found.error}). {next_step}",
                found.summary,
                "warn",
            )
            result = "retry"
        else:
            plan = plan_update(found, received)
            counts = (
                f"PACS: {counted(plan.pacs_total, 'imagem', 'imagens')} em "
                f"{counted(plan.series_count, 'série', 'séries')}; "
                f"recebidas: {plan.received_total}."
            )
            if plan.has_news:
                order.status = "wait_update"
                order.monitor_next_at = now
                order.monitor_pending_series = "\n".join(plan.series)
                order.heartbeat_at = None
                if plan.whole_study:
                    request = "PACS sem contagem por série: estudo completo solicitado."
                else:
                    request = (
                        "Imagens novas em "
                        f"{counted(len(plan.series), 'série', 'séries')}; "
                        "C-MOVE solicitado."
                    )
                add_event(
                    db, order, f"Verificação {number}: {counts} {request}", plan.detail
                )
                result = "new_images"
            else:
                level = "info" if plan.series_count else "warn"
                if not plan.series_count:
                    counts = "PACS não retornou séries do estudo."
                next_step = continue_monitoring(order, now)
                add_event(
                    db,
                    order,
                    f"Verificação {number}: nenhuma imagem nova. {counts} {next_step}",
                    plan.detail,
                    level,
                )
                result = "no_change"
        db.commit()
        log_event(
            log,
            logging.INFO if found.ok else logging.WARNING,
            "dicom.monitor.check",
            resource=f"order:{order.id}",
            status=result,
            started_at=started_at,
            order_id=order.id,
            unit_id=unit.id,
            check_number=number,
        )
