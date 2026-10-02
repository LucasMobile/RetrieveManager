"""C-MOVE: current study (1st and monitoring updates), manual and prior studies."""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timedelta
from time import monotonic, perf_counter

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.dicom_net import (
    MoveResult,
    PacsNode,
    find_prior_series,
    move_series,
    move_study,
)
from app.events import add_event
from app.models import (
    HistoricalSeries,
    HistoricalStudy,
    ManualMoveRequest,
    Order,
    Unit,
)
from app.observability import (
    log_context,
    log_event,
    safe_error_detail,
)
from app.order_state import ACTIVE_ORDER_STATUSES
from app.parse import (
    PriorSeriesResult,
    prior_series_results,
)
from app.pipeline.common import (
    append_diagnostic,
    bounded_db_text,
    ensure_order_correlation,
    log,
)
from app.pipeline.find import prior_identity_matches
from app.pipeline.monitor import continue_monitoring, start_monitoring
from app.rules import monitor_plan_for

PRIOR_RETRY_DELAYS = (60, 300)


CURRENT_MOVE_RETRY_DELAYS = (60, 300)


def _move_log_fields(moved: MoveResult) -> dict[str, object]:
    return {
        "dicom_status": moved.status,
        "completed_count": moved.completed,
        "failed_count": moved.failed,
        "warning_count": moved.warning,
        "error_type": moved.error or None,
    }


def claim_due_moves(db: Session, unit: Unit) -> list[tuple[int, str]]:
    now = datetime.now()
    current_inflight = (
        db.scalar(
            select(func.count()).where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                Order.status.in_(ACTIVE_ORDER_STATUSES),
            )
        )
        or 0
    )
    prior_inflight = (
        db.scalar(
            select(func.count()).where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                Order.prior_status == "retrieving",
            )
        )
        or 0
    )
    manual_inflight = (
        db.scalar(
            select(func.count()).where(
                ManualMoveRequest.unit_id == unit.id,
                ManualMoveRequest.status == "running",
            )
        )
        or 0
    )
    slots = (
        max(1, unit.max_parallel_moves)
        - current_inflight
        - prior_inflight
        - manual_inflight
    )
    claimed: list[tuple[int, str]] = []
    if slots <= 0:
        return claimed

    active_manual_order_ids = select(ManualMoveRequest.order_id).where(
        ManualMoveRequest.status.in_(("queued", "running"))
    )
    manual = list(
        db.scalars(
            select(ManualMoveRequest)
            .join(Order, Order.id == ManualMoveRequest.order_id)
            .where(
                ManualMoveRequest.unit_id == unit.id,
                ManualMoveRequest.status == "queued",
                Order.archived_at.is_(None),
                Order.study_uid != "",
                Order.status.notin_(ACTIVE_ORDER_STATUSES),
            )
            .order_by(ManualMoveRequest.created_at, ManualMoveRequest.id)
            .limit(slots)
            .with_for_update(skip_locked=True)
        )
    )
    first = list(
        db.scalars(
            select(Order)
            .where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                Order.status == "wait_retrieve",
                Order.id.notin_(active_manual_order_ids),
                Order.retrieve_at.is_not(None),
                Order.retrieve_at <= now,
            )
            .order_by(Order.retrieve_at)
            .limit(slots)
            .with_for_update(skip_locked=True)
        )
    )
    update = list(
        db.scalars(
            select(Order)
            .where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                Order.status == "wait_update",
                Order.id.notin_(active_manual_order_ids),
                Order.monitor_next_at.is_not(None),
                Order.monitor_next_at <= now,
            )
            .order_by(Order.monitor_next_at)
            .limit(slots)
            .with_for_update(skip_locked=True)
        )
    )
    prior = list(
        db.scalars(
            select(Order)
            .where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                Order.prior_status.in_(("queued", "retry_wait")),
                Order.prior_due_at.is_not(None),
                Order.prior_due_at <= now,
            )
            .order_by(Order.prior_due_at, Order.id)
            .limit(slots)
            .with_for_update(skip_locked=True)
        )
    )
    candidates = (
        [(request.created_at, -1, request.id, "manual", request) for request in manual]
        + [(order.retrieve_at, 1, order.id, "first", order) for order in first]
        + [(order.monitor_next_at, 2, order.id, "update", order) for order in update]
        + [(order.prior_due_at, 0, order.id, "prior", order) for order in prior]
    )
    kind_priority = {"manual": 0, "first": 1, "update": 1, "prior": 2}
    candidates.sort(
        key=lambda item: (kind_priority[item[3]], item[0], item[1], item[2])
    )
    for _due_at, _priority, _resource_id, kind, resource in candidates[:slots]:
        if kind == "manual":
            resource.status = "running"
            resource.started_at = now
            claimed.append((resource.id, kind))
            continue
        if kind == "first":
            resource.status = "retrieving"
            resource.heartbeat_at = now
        elif kind == "update":
            resource.status = "retrieving_update"
            resource.heartbeat_at = now
        else:
            resource.prior_status = "retrieving"
            resource.prior_heartbeat_at = now
        claimed.append((resource.id, kind))
    if claimed:
        db.commit()
        log_event(
            log,
            logging.INFO,
            "dicom.move.claim",
            resource=f"unit:{unit.id}",
            status="success",
            unit_id=unit.id,
            claimed_count=len(claimed),
            move_kinds=[kind for _resource_id, kind in claimed],
        )
    return claimed


def run_claimed_move(db: Session, resource_id: int, kind: str) -> None:
    if kind == "manual":
        request = db.get(ManualMoveRequest, resource_id)
        if request is None or request.status != "running":
            return
        _run_manual_move(db, resource_id)
        return
    order = db.get(Order, resource_id)
    if order is None:
        return
    unit = db.get(Unit, order.unit_id)
    if unit is None:
        return
    if kind == "prior":
        if order.prior_status != "retrieving" or order.archived_at is not None:
            return
        _run_prior_move(db, unit, order)
    elif kind == "update":
        if order.status != "retrieving_update" or order.archived_at is not None:
            return
        _run_update_move(db, unit, order)
    else:
        if order.status != "retrieving" or order.archived_at is not None:
            return
        _run_move(db, unit, order)


def fail_claimed_move(
    db: Session,
    resource_id: int,
    kind: str,
    error: BaseException,
) -> None:
    """Persist a terminal/retry state after an unexpected executor failure."""
    now = datetime.now()
    detail = safe_error_detail(error)
    if kind == "manual":
        request = db.get(ManualMoveRequest, resource_id)
        if request is None or request.status != "running":
            return
        request.status = "error"
        request.completed_at = now
        request.last_error = f"Falha interna no C-MOVE: {detail}"[:500]
        order = db.get(Order, request.order_id)
        if order is not None:
            add_event(
                db,
                order,
                "C-MOVE manual interrompido por falha interna",
                detail,
                "error",
            )
        db.commit()
        return

    order = db.get(Order, resource_id)
    if order is None or order.archived_at is not None:
        return
    if kind == "prior" and order.prior_status == "retrieving":
        order.prior_heartbeat_at = now
        if order.prior_attempts < 3:
            delay_index = max(0, min(order.prior_attempts - 1, 1))
            delay = PRIOR_RETRY_DELAYS[delay_index]
            order.prior_status = "retry_wait"
            order.prior_due_at = now + timedelta(seconds=delay)
            order.prior_last_error = f"Falha interna: {detail}"
            add_event(
                db,
                order,
                "Retrieve histórico interrompido; nova tentativa agendada",
                detail,
                "warn",
            )
        else:
            order.prior_status = "error"
            order.prior_completed_at = now
            order.prior_last_error = f"Falha interna: {detail}"
            add_event(
                db,
                order,
                "Retrieve histórico interrompido após esgotar as tentativas",
                detail,
                "error",
            )
        db.commit()
        return

    if kind == "update":
        if order.status != "retrieving_update":
            return
        next_step = continue_monitoring(order, now)
        add_event(
            db,
            order,
            f"C-MOVE de novas imagens interrompido por falha interna. {next_step}",
            detail,
            "warn",
        )
        db.commit()
        return
    if order.status != "retrieving":
        return
    _schedule_current_move_retry(
        db,
        order,
        now=now,
        error_message=f"Falha interna: {detail}",
        event_detail=detail,
    )
    db.commit()


def _schedule_current_move_retry(
    db: Session,
    order: Order,
    *,
    now: datetime,
    error_message: str,
    event_detail: str,
) -> bool:
    """Schedule a bounded retry of the 1st C-MOVE; return False when exhausted."""
    order.heartbeat_at = now
    if order.attempts >= 3:
        order.status = "error"
        order.last_error = error_message
        add_event(
            db,
            order,
            "1º C-MOVE falhou após 3 tentativas",
            event_detail,
            "error",
        )
        return False
    delay = CURRENT_MOVE_RETRY_DELAYS[max(0, order.attempts - 1)]
    order.status = "wait_retrieve"
    order.retrieve_at = now + timedelta(seconds=delay)
    order.last_error = error_message
    add_event(
        db,
        order,
        f"1º C-MOVE falhou; nova tentativa em {delay} segundos",
        event_detail,
        "warn",
    )
    return True


def _run_manual_move(db: Session, request_id: int) -> None:
    move_request = db.get(ManualMoveRequest, request_id)
    if move_request is None:
        return
    order = db.get(Order, move_request.order_id)
    unit = db.get(Unit, move_request.unit_id)
    if order is None or unit is None or order.archived_at or not order.study_uid:
        move_request.status = "error"
        move_request.completed_at = datetime.now()
        move_request.last_error = "Pedido indisponível para C-MOVE manual"
        db.commit()
        return

    started_at = perf_counter()
    with log_context(move_request.correlation_id):
        log_event(
            log,
            logging.INFO,
            "dicom.move.manual",
            resource=f"order:{order.id}",
            status="started",
            order_id=order.id,
            unit_id=unit.id,
            manual_move_request_id=move_request.id,
        )
        db.commit()
        moved = move_study(
            PacsNode.from_unit(unit),
            order.study_uid,
            timeout=unit.move_timeout_first,
        )
        move_request.completed_at = datetime.now()
        if moved.ok:
            move_request.status = "done"
            move_request.last_error = ""
            add_event(
                db,
                order,
                "C-MOVE manual do exame atual concluído",
                moved.summary,
            )
            event_level = logging.INFO
            event_status = "success"
        else:
            move_request.status = "error"
            move_request.last_error = bounded_db_text(
                f"C-MOVE manual: {moved.error}", 500
            )
            add_event(
                db,
                order,
                f"C-MOVE manual do exame atual falhou ({moved.error})",
                moved.summary,
                "error",
            )
            event_level = logging.ERROR
            event_status = "failure"
        db.commit()
        log_event(
            log,
            event_level,
            "dicom.move.manual",
            resource=f"order:{order.id}",
            status=event_status,
            started_at=started_at,
            order_id=order.id,
            unit_id=unit.id,
            manual_move_request_id=move_request.id,
            **_move_log_fields(moved),
        )


def _register_prior_studies(
    db: Session,
    unit: Unit,
    order: Order,
    series_results: tuple[PriorSeriesResult, ...],
) -> tuple[PriorSeriesResult, ...]:
    """Persist valid historical studies before requesting their images."""
    valid_series = tuple(
        result
        for result in series_results
        if result.study_uid != order.study_uid
        and result.study_date
        and order.prior_date_from <= result.study_date <= order.prior_date_to
    )
    studies: dict[str, PriorSeriesResult] = {}
    for result in valid_series:
        current = studies.get(result.study_uid)
        if current is None or (not current.body_part and result.body_part):
            studies[result.study_uid] = result

    for study_uid, result in studies.items():
        study = db.scalar(
            select(HistoricalStudy).where(
                HistoricalStudy.order_id == order.id,
                HistoricalStudy.study_uid == study_uid,
            )
        )
        if study is None:
            db.add(
                HistoricalStudy(
                    order_id=order.id,
                    unit_id=unit.id,
                    study_uid=bounded_db_text(study_uid, 128),
                    accession=bounded_db_text(result.accession, 64),
                    study_date=bounded_db_text(result.study_date, 16),
                    modality=bounded_db_text(result.modality, 32),
                    body_part=bounded_db_text(result.body_part, 64),
                    description=bounded_db_text(result.description, 255),
                )
            )
            continue
        study.accession = bounded_db_text(result.accession, 64) or study.accession
        study.study_date = bounded_db_text(result.study_date, 16) or study.study_date
        study.modality = bounded_db_text(result.modality, 32) or study.modality
        study.body_part = bounded_db_text(result.body_part, 64) or study.body_part
        study.description = (
            bounded_db_text(result.description, 255) or study.description
        )
    db.flush()
    return valid_series


def _register_prior_series(
    db: Session,
    unit: Unit,
    order: Order,
    series_results: tuple[PriorSeriesResult, ...],
) -> list[HistoricalSeries]:
    """Create idempotent per-series checkpoints and return unfinished jobs."""
    unique_results = {result.series_uid: result for result in series_results}
    existing = {
        series.series_uid: series
        for series in db.scalars(
            select(HistoricalSeries).where(HistoricalSeries.order_id == order.id)
        )
    }
    for series_uid, result in unique_results.items():
        if series_uid in existing:
            continue
        series = HistoricalSeries(
            order_id=order.id,
            unit_id=unit.id,
            study_uid=bounded_db_text(result.study_uid, 128),
            series_uid=bounded_db_text(series_uid, 128),
        )
        db.add(series)
        existing[series_uid] = series
    db.flush()
    return [series for series in existing.values() if series.status != "done"]


def _run_prior_move(db: Session, unit: Unit, order: Order) -> None:
    correlation_id = ensure_order_correlation(order)
    started_at = perf_counter()
    deadline = monotonic() + max(unit.move_timeout_prior, 1)
    now = datetime.now()
    order.prior_status = "retrieving"
    if order.prior_started_at is None:
        order.prior_started_at = now
    order.prior_heartbeat_at = now
    order.prior_attempts += 1
    db.commit()
    with log_context(correlation_id):
        log_event(
            log,
            logging.INFO,
            "dicom.move.prior",
            resource=f"order:{order.id}",
            status="started",
            order_id=order.id,
            unit_id=unit.id,
            attempt=order.prior_attempts,
        )
        operation = "C-FIND histórico"
        outputs: list[str] = []
        node = PacsNode.from_unit(unit)
        found = find_prior_series(
            node,
            timeout=max(1, int(deadline - monotonic())),
            body_part=order.body_part,
            modality=order.modality,
            patient_id=order.pat_id,
            birth_date=order.birth_date,
            date_range=f"{order.prior_date_from}-{order.prior_date_to}",
            patient_id_wildcard=unit.pacs_patient_id_wildcard,
        )
        outputs.append(found.summary)
        failure = found.error
        discovered_studies = 0
        discovered_series = 0
        foreign_series = 0
        if found.ok:
            parsed_series = prior_series_results(found.responses)
            if found.responses and not parsed_series:
                failure = "respostas sem StudyInstanceUID/SeriesInstanceUID"
                outputs.append(
                    "O PACS respondeu ao C-FIND sem StudyInstanceUID/SeriesInstanceUID."
                )
            else:
                trusted_series = tuple(
                    result
                    for result in parsed_series
                    if prior_identity_matches(unit, order, result)
                )
                foreign_series = len(parsed_series) - len(trusted_series)
                if foreign_series:
                    outputs.append(
                        f"{foreign_series} série(s) ignorada(s): identificação do "
                        "paciente ou nascimento diferente do pedido."
                    )
                    log_event(
                        log,
                        logging.WARNING,
                        "dicom.find.prior",
                        resource=f"order:{order.id}",
                        status="rejected",
                        error_type="PriorIdentityMismatch",
                        order_id=order.id,
                        unit_id=unit.id,
                        rejected_series_count=foreign_series,
                    )
                valid_series = _register_prior_studies(db, unit, order, trusted_series)
                discovered_series = len(valid_series)
                discovered_studies = len({result.study_uid for result in valid_series})
                series_jobs = _register_prior_series(db, unit, order, valid_series)
                all_series = list(
                    db.scalars(
                        select(HistoricalSeries).where(
                            HistoricalSeries.order_id == order.id
                        )
                    )
                )
                discovered_series = len(all_series)
                discovered_studies = len({series.study_uid for series in all_series})
                # Torna os UIDs e checkpoints visíveis enquanto os C-MOVEs executam.
                db.commit()
                operation = "C-MOVE histórico"
                for series_job in series_jobs:
                    remaining = int(deadline - monotonic())
                    if remaining <= 0:
                        failure = failure or "tempo total do histórico esgotado"
                        append_diagnostic(outputs, "C-MOVE histórico", "TIMEOUT global")
                        break
                    series_job.status = "running"
                    series_job.attempts += 1
                    series_job.last_error = ""
                    db.commit()
                    moved = move_series(
                        node,
                        series_job.study_uid,
                        series_job.series_uid,
                        timeout=remaining,
                    )
                    append_diagnostic(
                        outputs,
                        "C-MOVE histórico "
                        f"StudyUID={series_job.study_uid} "
                        f"SeriesUID={series_job.series_uid}",
                        moved.summary,
                    )
                    if moved.ok:
                        series_job.status = "done"
                        series_job.completed_at = datetime.now()
                        series_job.last_error = ""
                    else:
                        series_job.status = "error"
                        series_job.last_error = bounded_db_text(moved.error, 500)
                    db.commit()
                    if not moved.ok and not failure:
                        failure = moved.error
        safe_output = "\n\n".join(outputs)
        finished_at = datetime.now()
        order.prior_heartbeat_at = finished_at
        if not failure:
            order.prior_status = "done"
            order.prior_completed_at = finished_at
            order.prior_last_error = ""
            if discovered_series:
                message = (
                    "Retrieve histórico concluído: "
                    f"{discovered_studies} exame(s), {discovered_series} série(s)"
                )
            else:
                message = (
                    "Retrieve histórico concluído: nenhum exame anterior localizado"
                )
            if foreign_series:
                message += (
                    f"; {foreign_series} série(s) com identificação divergente "
                    "ignorada(s)"
                )
            add_event(db, order, message, safe_output)
            event_level = logging.INFO
            event_status = "success"
        elif order.prior_attempts < 3:
            delay = PRIOR_RETRY_DELAYS[order.prior_attempts - 1]
            order.prior_status = "retry_wait"
            order.prior_due_at = finished_at + timedelta(seconds=delay)
            order.prior_last_error = bounded_db_text(f"{operation}: {failure}", 500)
            add_event(
                db,
                order,
                f"{operation} falhou ({failure}); nova tentativa agendada",
                safe_output,
                "warn",
            )
            event_level = logging.WARNING
            event_status = "retry"
        else:
            order.prior_status = "error"
            order.prior_completed_at = finished_at
            order.prior_last_error = bounded_db_text(f"{operation}: {failure}", 500)
            add_event(
                db,
                order,
                f"{operation} falhou após 3 tentativas ({failure})",
                safe_output,
                "error",
            )
            event_level = logging.ERROR
            event_status = "failure"
        db.commit()
        log_event(
            log,
            event_level,
            "dicom.move.prior",
            resource=f"order:{order.id}",
            status=event_status,
            started_at=started_at,
            order_id=order.id,
            unit_id=unit.id,
            attempt=order.prior_attempts,
            error_type=failure or None,
        )


def _run_move(db: Session, unit: Unit, order: Order) -> None:
    correlation_id = ensure_order_correlation(order)
    started_at = perf_counter()
    order.status = "retrieving"
    order.heartbeat_at = datetime.now()
    order.attempts += 1
    db.commit()
    with log_context(correlation_id):
        log_event(
            log,
            logging.INFO,
            "dicom.move",
            resource=f"order:{order.id}",
            status="started",
            order_id=order.id,
            unit_id=unit.id,
            retrieve_number=1,
            attempt=order.attempts,
        )
        moved = move_study(
            PacsNode.from_unit(unit), order.study_uid, timeout=unit.move_timeout_first
        )
        if moved.ok:
            now = datetime.now()
            order.last_error = ""
            order.heartbeat_at = now
            # As tentativas medem somente o 1º C-MOVE.
            order.attempts = 0
            plan = monitor_plan_for(db, order.modality)
            if plan is None:
                order.status = "done"
                order.done_at = now
                add_event(db, order, "1º C-MOVE concluído", moved.summary)
            else:
                window = start_monitoring(order, plan, now)
                add_event(db, order, f"1º C-MOVE concluído. {window}", moved.summary)
            event_level = logging.INFO
            event_status = "success"
        else:
            retrying = _schedule_current_move_retry(
                db,
                order,
                now=datetime.now(),
                error_message=f"C-MOVE: {moved.error}",
                event_detail=moved.summary,
            )
            event_level = logging.WARNING if retrying else logging.ERROR
            event_status = "retry" if retrying else "failure"
        db.commit()
        log_event(
            log,
            event_level,
            "dicom.move",
            resource=f"order:{order.id}",
            status=event_status,
            started_at=started_at,
            order_id=order.id,
            unit_id=unit.id,
            retrieve_number=1,
            **_move_log_fields(moved),
        )


def _run_update_move(db: Session, unit: Unit, order: Order) -> None:
    """Fetch what the last monitoring check found new, then keep monitoring.

    A failure is not an order error: the next check compares the counts again
    and asks for whatever is still missing.
    """
    correlation_id = ensure_order_correlation(order)
    started_at = perf_counter()
    order.status = "retrieving_update"
    order.heartbeat_at = datetime.now()
    db.commit()
    series = tuple(
        uid for uid in (order.monitor_pending_series or "").splitlines() if uid
    )
    with log_context(correlation_id):
        log_event(
            log,
            logging.INFO,
            "dicom.move",
            resource=f"order:{order.id}",
            status="started",
            order_id=order.id,
            unit_id=unit.id,
            retrieve_number="update",
            series_count=len(series),
        )
        node = PacsNode.from_unit(unit)
        timeout = unit.move_timeout_update
        if series:
            moved = _move_series_list(node, order.study_uid, series, timeout)
        else:
            moved = move_study(node, order.study_uid, timeout=timeout)
        order.monitor_new_images = (order.monitor_new_images or 0) + moved.completed
        target = f"{len(series)} série(s)" if series else "estudo completo"
        next_step = continue_monitoring(order, datetime.now())
        if moved.ok:
            add_event(
                db,
                order,
                f"C-MOVE de novas imagens concluído ({target}): "
                f"{moved.completed} imagem(ns) recebida(s). {next_step}",
                moved.summary,
            )
            event_level = logging.INFO
            event_status = "success"
        else:
            add_event(
                db,
                order,
                f"C-MOVE de novas imagens falhou ({target}; {moved.error}). "
                f"{next_step}",
                moved.summary,
                "warn",
            )
            event_level = logging.WARNING
            event_status = "retry"
        db.commit()
        log_event(
            log,
            event_level,
            "dicom.move",
            resource=f"order:{order.id}",
            status=event_status,
            started_at=started_at,
            order_id=order.id,
            unit_id=unit.id,
            retrieve_number="update",
            **_move_log_fields(moved),
        )


def _move_series_list(
    node: PacsNode, study_uid: str, series: tuple[str, ...], timeout: float
) -> MoveResult:
    """C-MOVE the given series in turn; stops at the first failure."""
    deadline = monotonic() + timeout
    totals = {"completed": 0, "failed": 0, "warning": 0}
    for series_uid in series:
        remaining = deadline - monotonic()
        if remaining <= 0:
            return MoveResult(False, error="tempo esgotado", remaining=0, **totals)
        moved = move_series(node, study_uid, series_uid, timeout=remaining)
        totals["completed"] += moved.completed
        totals["failed"] += moved.failed
        totals["warning"] += moved.warning
        if not moved.ok:
            return replace(moved, **totals)
    return MoveResult(True, 0x0000, **totals)
