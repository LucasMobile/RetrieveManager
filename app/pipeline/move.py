"""C-MOVE: current study (1st and monitoring updates), manual and prior studies."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from time import monotonic, perf_counter, sleep

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import MOVE_RECEIVE_CONFIRM_SECONDS, PRIOR_MOVE_IDLE_TIMEOUT_SECONDS
from app.dicom_net import (
    MoveResult,
    MoveSession,
    PacsNode,
    find_prior_series,
    move_series,
    move_study,
)
from app.events import add_event
from app.models import (
    DicomStudy,
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
from app.wording import counted

PRIOR_RETRY_DELAYS = (60, 300)


CURRENT_MOVE_RETRY_DELAYS = (60, 300)


@dataclass(frozen=True)
class Arrival:
    """What the receiver recorded of a study after a C-MOVE."""

    moved: MoveResult  # failed when the PACS reported images that never arrived
    instance_count: int  # distinct images of the study held by the receiver


def _study_receipt(
    db: Session, unit_id: int, study_uid: str
) -> tuple[datetime | None, int]:
    row = db.execute(
        select(DicomStudy.last_received_at, DicomStudy.instance_count).where(
            DicomStudy.unit_id == unit_id, DicomStudy.study_uid == study_uid
        )
    ).one_or_none()
    return (row[0], int(row[1])) if row else (None, 0)


def _confirm_arrival(
    db: Session, unit: Unit, study_uid: str, moved: MoveResult, started: datetime
) -> Arrival:
    """Check that a C-MOVE the PACS called successful reached this receiver.

    The PACS counts the images it delivered to the destination AE. When that
    AE resolves to another Store SCP (another service on the port, a wrong IP
    or port in the PACS), the C-MOVE succeeds and nothing arrives here, so the
    order would be marked retrieved with no images. The receiver commits each
    image right after acknowledging it: wait until it has recorded images of
    the study since ``started`` and its count stops growing.

    Called right after the C-MOVE, with nothing pending in the session: each
    read is committed so no connection stays idle in a transaction meanwhile.
    """
    last, count = _study_receipt(db, unit.id, study_uid)
    if not moved.ok or not (moved.completed or moved.warning):
        return Arrival(moved, count)
    deadline = monotonic() + MOVE_RECEIVE_CONFIRM_SECONDS
    previous: int | None = None
    while True:
        arrived = last is not None and last >= started
        if arrived and count == previous:
            return Arrival(moved, count)
        if monotonic() >= deadline:
            break
        previous = count if arrived else None
        db.commit()
        sleep(0.5)
        last, count = _study_receipt(db, unit.id, study_uid)
    if arrived:
        return Arrival(moved, count)
    error = (
        "o PACS informou "
        + counted(moved.completed + moved.warning, "imagem enviada", "imagens enviadas")
        + f", mas nenhuma chegou ao receptor; confira no PACS se o AE "
        f"{unit.calling_aet} aponta para este servidor na porta {unit.store_port}"
    )
    log_event(
        log,
        logging.ERROR,
        "dicom.move.arrival",
        resource=f"unit:{unit.id}",
        status="failure",
        error_type="ImagesNotReceived",
        unit_id=unit.id,
        completed_count=moved.completed,
        store_port=unit.store_port,
        calling_aet=unit.calling_aet,
    )
    return Arrival(replace(moved, ok=False, error=error), count)


def _move_log_fields(moved: MoveResult) -> dict[str, object]:
    return {
        "dicom_status": moved.status,
        "completed_count": moved.completed,
        "failed_count": moved.failed,
        "warning_count": moved.warning,
        "error_type": moved.error or None,
    }


@dataclass(frozen=True)
class MoveSlots:
    """How the unit's parallel C-MOVEs are shared between kinds of work.

    ``total`` is the unit's limit (what its PACS accepts at once). The 1st
    retrieve and manual moves may use any free slot. The background work,
    monitoring updates and prior studies, never takes the last slot, so a new
    exam starts within one claim cycle; and neither of the two takes more than
    half, so dozens of monitored CTs cannot hold every slot while the prior
    queue waits, nor the other way round. A unit with one slot shares it in
    priority order.
    """

    total: int
    background: int
    update: int
    prior: int


def move_slots(max_parallel_moves: int) -> MoveSlots:
    total = max(1, max_parallel_moves)
    if total == 1:
        return MoveSlots(1, 1, 1, 1)
    half = (total + 1) // 2
    return MoveSlots(total, total - 1, half, half)


def _moves_in_flight(db: Session, unit_id: int) -> dict[str, int]:
    """Running C-MOVEs of the unit by kind: current, update, prior, manual."""
    by_status = dict(
        db.execute(
            select(Order.status, func.count())
            .where(
                Order.unit_id == unit_id,
                Order.archived_at.is_(None),
                Order.status.in_(ACTIVE_ORDER_STATUSES),
            )
            .group_by(Order.status)
        ).all()
    )
    update = by_status.pop("retrieving_update", 0)
    prior = db.scalar(
        select(func.count()).where(
            Order.unit_id == unit_id,
            Order.archived_at.is_(None),
            Order.prior_status == "retrieving",
        )
    )
    manual = db.scalar(
        select(func.count()).where(
            ManualMoveRequest.unit_id == unit_id,
            ManualMoveRequest.status == "running",
        )
    )
    return {
        "current": sum(by_status.values()),
        "update": update,
        "prior": prior or 0,
        "manual": manual or 0,
    }


def claim_due_moves(db: Session, unit: Unit) -> list[tuple[int, str]]:
    now = datetime.now()
    limits = move_slots(unit.max_parallel_moves)
    running = _moves_in_flight(db, unit.id)
    slots = limits.total - sum(running.values())
    claimed: list[tuple[int, str]] = []
    if slots <= 0:
        return claimed
    background_free = limits.background - running["update"] - running["prior"]
    update_free = min(slots, background_free, limits.update - running["update"])
    # With the prior retrieve turned off nothing historical starts; saving
    # the unit withdraws the queue (rules.cancel_unwanted_priors).
    prior_free = (
        min(slots, background_free, limits.prior - running["prior"])
        if unit.retrieve_prior_enabled
        else 0
    )

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
    update = (
        list(
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
                .limit(update_free)
                .with_for_update(skip_locked=True)
            )
        )
        if update_free > 0
        else []
    )
    prior = (
        list(
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
                .limit(prior_free)
                .with_for_update(skip_locked=True)
            )
        )
        if prior_free > 0
        else []
    )
    # Manual requests, then the 1st retrieves by due time; then the background
    # work by due time, whichever kind waited longer, within its own share.
    foreground = [
        (request.created_at, request.id, "manual", request) for request in manual
    ]
    foreground += [(order.retrieve_at, order.id, "first", order) for order in first]
    background = sorted(
        [(order.monitor_next_at, order.id, "update", order) for order in update]
        + [(order.prior_due_at, order.id, "prior", order) for order in prior],
        key=lambda item: (item[0], item[1]),
    )
    background_room = {"update": update_free, "prior": prior_free}
    for _due_at, _resource_id, kind, resource in foreground + background:
        if slots <= 0:
            break
        if kind in background_room:
            if background_room[kind] <= 0 or background_free <= 0:
                continue
            background_room[kind] -= 1
            background_free -= 1
        slots -= 1
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
            running=running,
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
        move_started = datetime.now()
        moved = move_study(
            PacsNode.from_unit(unit),
            order.study_uid,
            timeout=unit.move_timeout_first,
        )
        moved = _confirm_arrival(db, unit, order.study_uid, moved, move_started).moved
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
        stopped = False
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
                        counted(foreign_series, "série ignorada", "séries ignoradas")
                        + ": identificação do paciente ou nascimento diferente "
                        "do pedido."
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
                # One association for every series; the arrival is confirmed
                # once per study, after its series.
                with MoveSession(node) as session:
                    for study_uid, study_jobs in _by_study(series_jobs):
                        moved_jobs: list[tuple[HistoricalSeries, MoveResult]] = []
                        study_started = datetime.now()
                        interrupted = False
                        for series_job in study_jobs:
                            if not _prior_enabled(db, unit.id):
                                # Turned off on the unit meanwhile: stop
                                # between series.
                                stopped = interrupted = True
                                break
                            remaining = int(deadline - monotonic())
                            if remaining <= 0:
                                failure = failure or "tempo total do histórico esgotado"
                                append_diagnostic(
                                    outputs, "C-MOVE histórico", "TIMEOUT global"
                                )
                                interrupted = True
                                break
                            series_job.status = "running"
                            series_job.attempts += 1
                            series_job.last_error = ""
                            db.commit()
                            moved = move_series(
                                node,
                                study_uid,
                                series_job.series_uid,
                                timeout=remaining,
                                session=session,
                                idle_timeout=PRIOR_MOVE_IDLE_TIMEOUT_SECONDS,
                            )
                            append_diagnostic(
                                outputs,
                                "C-MOVE histórico "
                                f"StudyUID={study_uid} "
                                f"SeriesUID={series_job.series_uid}",
                                moved.summary,
                            )
                            moved_jobs.append((series_job, moved))
                        settled_failure = _settle_prior_series(
                            db, unit, study_uid, moved_jobs, study_started, outputs
                        )
                        failure = failure or settled_failure
                        if interrupted:
                            break
        safe_output = "\n\n".join(outputs)
        finished_at = datetime.now()
        order.prior_heartbeat_at = finished_at
        if stopped:
            order.prior_status = "disabled"
            order.prior_due_at = None
            order.prior_last_error = ""
            done_series = sum(1 for job in series_jobs if job.status == "done")
            add_event(
                db,
                order,
                "Retrieve histórico interrompido: desativado na unidade ("
                + counted(done_series, "série concluída", "séries concluídas")
                + f" de {len(series_jobs)})",
                safe_output,
            )
            event_level = logging.INFO
            event_status = "cancelled"
        elif not failure:
            order.prior_status = "done"
            order.prior_completed_at = finished_at
            order.prior_last_error = ""
            if discovered_series:
                message = (
                    "Retrieve histórico concluído: "
                    f"{counted(discovered_studies, 'exame', 'exames')}, "
                    f"{counted(discovered_series, 'série', 'séries')}"
                )
            else:
                message = (
                    "Retrieve histórico concluído: nenhum exame anterior localizado"
                )
            if foreign_series:
                message += "; " + counted(
                    foreign_series,
                    "série com identificação divergente ignorada",
                    "séries com identificação divergente ignoradas",
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


def _by_study(
    jobs: list[HistoricalSeries],
) -> list[tuple[str, list[HistoricalSeries]]]:
    """The series grouped by study, in the order the studies first appear."""
    studies: dict[str, list[HistoricalSeries]] = {}
    for job in jobs:
        studies.setdefault(job.study_uid, []).append(job)
    return list(studies.items())


def _settle_prior_series(
    db: Session,
    unit: Unit,
    study_uid: str,
    moved_jobs: list[tuple[HistoricalSeries, MoveResult]],
    started: datetime,
    outputs: list[str],
) -> str:
    """Record the C-MOVEs of one study's series; return the first failure.

    The receiver's arrival check is per study: one check covers every series
    the PACS reported as sent since ``started``.
    """
    sent = [moved for _job, moved in moved_jobs if moved.ok]
    arrival_error = ""
    if sent:
        combined = MoveResult(
            True,
            0x0000,
            completed=sum(moved.completed for moved in sent),
            warning=sum(moved.warning for moved in sent),
        )
        confirmed = _confirm_arrival(db, unit, study_uid, combined, started).moved
        if not confirmed.ok:
            arrival_error = confirmed.error
            append_diagnostic(
                outputs, f"Receptor StudyUID={study_uid}", confirmed.summary
            )
    first_failure = ""
    for job, moved in moved_jobs:
        error = moved.error if not moved.ok else arrival_error
        if error:
            job.status = "error"
            job.last_error = bounded_db_text(error, 500)
            first_failure = first_failure or error
        else:
            job.status = "done"
            job.completed_at = datetime.now()
            job.last_error = ""
    db.commit()
    return first_failure


def _prior_enabled(db: Session, unit_id: int) -> bool:
    """Current setting, read again: the unit may be edited during a retrieve."""
    return bool(
        db.scalar(select(Unit.retrieve_prior_enabled).where(Unit.id == unit_id))
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
        move_started = datetime.now()
        moved = move_study(
            PacsNode.from_unit(unit), order.study_uid, timeout=unit.move_timeout_first
        )
        arrival = _confirm_arrival(db, unit, order.study_uid, moved, move_started)
        moved = arrival.moved
        if moved.ok:
            now = datetime.now()
            order.last_error = ""
            order.heartbeat_at = now
            # As tentativas medem somente o 1º C-MOVE.
            order.attempts = 0
            detail = f"{moved.summary}\nReceptor: " + counted(
                arrival.instance_count,
                "imagem do estudo registrada",
                "imagens do estudo registradas",
            )
            plan = monitor_plan_for(db, order.modality)
            if plan is None:
                order.status = "done"
                order.done_at = now
                add_event(db, order, "1º C-MOVE concluído", detail)
            else:
                window = start_monitoring(order, plan, now)
                add_event(db, order, f"1º C-MOVE concluído. {window}", detail)
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
    _last, held_before = _study_receipt(db, unit.id, order.study_uid)
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
        move_started = datetime.now()
        if series:
            moved = _move_series_list(node, order.study_uid, series, timeout)
        else:
            moved = move_study(node, order.study_uid, timeout=timeout)
        arrival = _confirm_arrival(db, unit, order.study_uid, moved, move_started)
        moved = arrival.moved
        # The PACS sends each series whole, images already held included: count
        # what the receiver gained, not what the PACS sent.
        received = max(0, arrival.instance_count - held_before)
        order.monitor_new_images = (order.monitor_new_images or 0) + received
        target = (
            counted(len(series), "série", "séries") if series else "estudo completo"
        )
        next_step = continue_monitoring(order, datetime.now())
        if moved.ok:
            add_event(
                db,
                order,
                f"C-MOVE de novas imagens concluído ({target}): "
                + counted(received, "imagem nova recebida", "imagens novas recebidas")
                + f". {next_step}",
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
    """C-MOVE the given series in turn, on one association; stops at the first
    failure."""
    deadline = monotonic() + timeout
    totals = {"completed": 0, "failed": 0, "warning": 0}
    with MoveSession(node) as session:
        for series_uid in series:
            remaining = deadline - monotonic()
            if remaining <= 0:
                return MoveResult(False, error="tempo esgotado", remaining=0, **totals)
            moved = move_series(
                node, study_uid, series_uid, timeout=remaining, session=session
            )
            totals["completed"] += moved.completed
            totals["failed"] += moved.failed
            totals["warning"] += moved.warning
            if not moved.ok:
                return replace(moved, **totals)
    return MoveResult(True, 0x0000, **totals)
