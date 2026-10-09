"""C-FIND of the current study and its scheduling."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from time import perf_counter

from sqlalchemy import and_, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.compression import find_modalities_for_unit
from app.config import (
    FIND_BATCH_SIZE,
    FIND_TIMEOUT_SECONDS,
)
from app.dicom_net import (
    FindResult,
    PacsNode,
    find_study,
    find_study_series,
)
from app.events import add_event
from app.models import (
    Order,
    Unit,
)
from app.observability import (
    log_context,
    log_event,
    safe_error_detail,
)
from app.order_state import queue_prior_retrieve
from app.parse import (
    PriorSeriesResult,
    StudyFindResponse,
    first_allowed_modality,
    first_series_body_part,
    patient_id_matches,
    series_have_modality,
    series_metadata,
    study_response,
)
from app.pipeline.common import (
    bounded_db_text,
    ensure_order_correlation,
    log,
)
from app.pipeline.monitor import reset_monitoring
from app.rules import prior_skip_reason, schedule_from_now
from app.wording import counted

# Orders the PACS has not answered yet are searched less often as they age:
# the unit's interval for the first minutes, then twice and four times it, so
# hours of pending orders do not take the C-FIND slots from the fresh ones.
# The longer waits never exceed FIND_BACKOFF_MAX_WAIT (or the interval itself).
FIND_BACKOFF_STEPS = (
    (timedelta(minutes=15), 1),
    (timedelta(hours=1), 2),
)
FIND_BACKOFF_FACTOR = 4
FIND_BACKOFF_MAX_WAIT = timedelta(minutes=5)


def find_due(now: datetime, interval: timedelta):
    """SQL condition: a watching order is due for its next C-FIND."""
    cap = max(interval, FIND_BACKOFF_MAX_WAIT)
    conditions = [Order.last_find_at.is_(None)]
    for age, factor in FIND_BACKOFF_STEPS:
        conditions.append(
            and_(
                Order.created_at > now - age,
                Order.last_find_at <= now - min(interval * factor, cap),
            )
        )
    conditions.append(
        Order.last_find_at <= now - min(interval * FIND_BACKOFF_FACTOR, cap)
    )
    return or_(*conditions)


def find_pending(db: Session, unit: Unit, max_orders: int | None = None) -> int:
    now = datetime.now()
    interval = timedelta(seconds=unit.find_interval_seconds or 30)
    # A claim gravada antes da chamada ao PACS impede que os executores da
    # mesma unidade escolham o mesmo pedido enquanto o findscu está rodando.
    in_flight_cutoff = now - timedelta(seconds=max(180, 2 * FIND_TIMEOUT_SECONDS))
    orders = list(
        db.scalars(
            select(Order)
            .where(
                Order.unit_id == unit.id,
                Order.archived_at.is_(None),
                Order.status == "watching",
                or_(
                    Order.heartbeat_at.is_(None),
                    Order.heartbeat_at <= in_flight_cutoff,
                ),
                find_due(now, interval),
            )
            # PostgreSQL puts NULL last in ascending order by default. Without
            # this, repeatedly due orders can starve orders never searched.
            .order_by(Order.last_find_at.asc().nulls_first(), Order.id)
            .limit(max_orders if max_orders is not None else FIND_BATCH_SIZE)
            .with_for_update(skip_locked=True)
        )
    )
    for order in orders:
        order.last_find_at = now
        order.heartbeat_at = now
    if orders:
        db.commit()
    for order in orders:
        try:
            _find_one(db, unit, order, now)
        except SQLAlchemyError as exc:
            # One malformed PACS response must not poison the whole unit queue.
            order_id = order.id
            db.rollback()
            failed_order = db.get(Order, order_id)
            if failed_order is not None:
                failed_order.status = "error"
                failed_order.last_find_at = now
                failed_order.heartbeat_at = None
                failed_order.last_error = (
                    "Falha ao salvar o resultado do C-FIND no banco de dados"
                )
                add_event(
                    db,
                    failed_order,
                    "Resultado do C-FIND rejeitado pelo banco de dados",
                    level="error",
                )
                db.commit()
            log_event(
                log,
                logging.ERROR,
                "dicom.find.persist",
                resource=f"order:{order_id}",
                status="failure",
                error=exc,
                order_id=order_id,
                unit_id=unit.id,
            )
        except Exception as exc:
            # Erros de SO/parser/programação ficam contidos neste pedido. O pedido
            # permanece observável e elegível para uma nova consulta.
            order_id = order.id
            db.rollback()
            failed_order = db.get(Order, order_id)
            if failed_order is not None and failed_order.status == "watching":
                failed_order.last_find_at = now
                failed_order.heartbeat_at = None
                failed_order.last_error = (
                    f"Falha interna no C-FIND: {safe_error_detail(exc)}"
                )
                add_event(
                    db,
                    failed_order,
                    "C-FIND interrompido; consulta será repetida",
                    safe_error_detail(exc),
                    "warn",
                )
                db.commit()
            log_event(
                log,
                logging.ERROR,
                "dicom.find.internal",
                resource=f"order:{order_id}",
                status="retry",
                error=exc,
                order_id=order_id,
                unit_id=unit.id,
            )

    return len(orders)


def _find_one(db: Session, unit: Unit, order: Order, now: datetime) -> None:
    order.last_find_at = now
    order.heartbeat_at = now
    correlation_id = ensure_order_correlation(order)
    started_at = perf_counter()
    with log_context(correlation_id):
        log_event(
            log,
            logging.INFO,
            "dicom.find",
            resource=f"order:{order.id}",
            status="started",
            order_id=order.id,
            unit_id=unit.id,
        )
        node = PacsNode.from_unit(unit)
        found = find_study(
            node, order.acc, order.birth_date, timeout=FIND_TIMEOUT_SECONDS
        )

        if not found.ok:
            order.heartbeat_at = None
            order.last_error = bounded_db_text(f"C-FIND falhou: {found.error}", 500)
            add_event(
                db,
                order,
                f"C-FIND falhou ({found.error}); consulta será repetida",
                found.summary,
                "warn",
                kind="find_failed",
            )
            db.commit()
            log_event(
                log,
                logging.WARNING,
                "dicom.find",
                resource=f"order:{order.id}",
                status="retry",
                started_at=started_at,
                order_id=order.id,
                unit_id=unit.id,
                dicom_status=found.status,
                error_type=found.error,
            )
            return

        responses = tuple(study_response(ds) for ds in found.responses)
        diagnostic = _find_diagnostic(found, responses)
        study_uids = list(
            dict.fromkeys(
                response.study_uid for response in responses if response.study_uid
            )
        )
        if study_uids:
            conflict, mismatched_fields = _find_identity_conflict(
                unit, order, responses, study_uids
            )
            if conflict:
                order.status = "error"
                order.heartbeat_at = None
                order.last_error = conflict
                add_event(
                    db,
                    order,
                    f"{conflict}; nenhum C-MOVE foi agendado. Confira o pedido e "
                    "o PACS antes de reprocessar.",
                    diagnostic,
                    "error",
                )
                db.commit()
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.find",
                    resource=f"order:{order.id}",
                    status="conflict",
                    started_at=started_at,
                    error_type="FindIdentityConflict",
                    order_id=order.id,
                    unit_id=unit.id,
                    study_count=len(study_uids),
                    mismatched_fields=mismatched_fields,
                )
                return
            if _find_patient_id_diverges(unit, order, responses, study_uids[0]):
                add_event(
                    db,
                    order,
                    "PatientID no PACS difere do pedido; estudo aceito por "
                    "Accession e data de nascimento",
                    diagnostic,
                    "warn",
                )
        response = next(
            (item for item in responses if item.study_uid), StudyFindResponse()
        )
        study_uid = response.study_uid
        modalities = response.modalities
        patient_name = response.patient_name
        body_part = response.body_part
        if not study_uid:
            order.heartbeat_at = None
            order.last_error = ""
            add_event(
                db,
                order,
                "C-FIND sem StudyInstanceUID (exame ainda não no PACS)",
                diagnostic,
                kind="find_not_found",
            )
            db.commit()
            log_event(
                log,
                logging.INFO,
                "dicom.find",
                resource=f"order:{order.id}",
                status="not_found",
                started_at=started_at,
                order_id=order.id,
                unit_id=unit.id,
            )
            return
        allowed_modalities = find_modalities_for_unit(db, unit.id)
        modality = first_allowed_modality(modalities, allowed_modalities)
        # A consulta de regras abriu uma transação; não retenha a conexão
        # enquanto o C-FIND complementar aguarda a resposta do PACS.
        db.commit()
        series = find_study_series(node, study_uid, timeout=FIND_TIMEOUT_SECONDS)
        safe_output = f"{diagnostic}\nSéries: {series.summary}"
        if series.ok:
            series_modality, series_body_part = series_metadata(
                series.responses, allowed_modalities
            )
            if series_modality:
                modality = series_modality
                body_part = series_body_part
            elif series_have_modality(series.responses):
                # The PACS returned series, but none belongs to the accepted
                # compression catalog. Keep watching instead of choosing SR/PR.
                modality = ""
                body_part = ""
            elif modality and not body_part:
                # Compatibility with PACS nodes that omit Modality at SERIES.
                body_part = first_series_body_part(series.responses)
        if not modality:
            order.heartbeat_at = None
            order.last_error = ""
            add_event(
                db,
                order,
                "C-FIND sem modalidade clínica válida; consulta será repetida",
                safe_output,
                kind="find_no_modality",
            )
            db.commit()
            log_event(
                log,
                logging.INFO,
                "dicom.find",
                resource=f"order:{order.id}",
                status="not_ready",
                started_at=started_at,
                order_id=order.id,
                unit_id=unit.id,
            )
            return

        modality, retrieve_at = schedule_from_now(db, modality)
        order.study_uid = study_uid
        order.modality = modality[:32]
        order.patient_name = patient_name[:255]
        order.body_part = body_part[:64]
        order.retrieve_at = retrieve_at
        reset_monitoring(order)
        order.found_at = now
        order.status = "wait_retrieve"
        order.heartbeat_at = None
        # A partir daqui, attempts mede somente tentativas do C-MOVE atual.
        order.attempts = 0
        order.last_error = ""
        prior_skip = prior_skip_reason(db, unit, modality)
        if prior_skip is None:
            queue_prior_retrieve(order, now, refresh_window=True)
        else:
            order.prior_status = "disabled"
        add_event(
            db,
            order,
            f"Exame encontrado UID={study_uid} mod={modality}. "
            f"1º retrieve em {retrieve_at:%H:%M}",
            safe_output,
        )
        if prior_skip is None:
            add_event(
                db,
                order,
                "Retrieve histórico enfileirado para execução imediata: "
                f"{order.prior_date_from}-{order.prior_date_to}",
            )
        elif prior_skip:
            add_event(db, order, prior_skip)
        db.commit()
        log_event(
            log,
            logging.INFO,
            "dicom.find",
            resource=f"order:{order.id}",
            status="success",
            started_at=started_at,
            order_id=order.id,
            unit_id=unit.id,
            modality=modality,
        )


def _find_diagnostic(
    found: FindResult, responses: tuple[StudyFindResponse, ...]
) -> str:
    """Event detail for a C-FIND: counts and study UIDs, no patient data."""
    lines = [found.summary]
    for response in responses:
        if response.study_uid:
            count = (
                ", " + counted(response.instance_count, "imagem", "imagens")
                if response.instance_count
                else ""
            )
            lines.append(
                f"Estudo {response.study_uid} "
                f"({response.modalities or 'sem modalidade'}{count})"
            )
    return "\n".join(lines)


def _find_identity_conflict(
    unit: Unit,
    order: Order,
    responses: tuple[StudyFindResponse, ...],
    study_uids: list[str],
) -> tuple[str, list[str]]:
    """Refuse ambiguous or foreign C-FIND matches before any C-MOVE.

    Accession + birth date are the order's identity: a PACS may ignore one of
    them as a matching key, so the returned values must confirm both. PatientID
    is not checked here because units type it by hand when the worklist is down.
    """
    if len(study_uids) > 1:
        return (
            f"C-FIND retornou {len(study_uids)} estudos diferentes para o mesmo "
            "accession",
            ["StudyInstanceUID"],
        )
    mismatched: list[str] = []
    for response in responses:
        if response.study_uid != study_uids[0]:
            continue
        checks = (
            ("PatientBirthDate", response.birth_date.strip() == order.birth_date),
            ("AccessionNumber", response.accession.strip() == order.acc.strip()),
        )
        mismatched.extend(
            name for name, matches in checks if not matches and name not in mismatched
        )
    if mismatched:
        return (
            "C-FIND retornou estudo com identificação divergente do pedido "
            f"({', '.join(mismatched)})",
            mismatched,
        )
    return "", []


def _find_patient_id_diverges(
    unit: Unit, order: Order, responses: tuple[StudyFindResponse, ...], study_uid: str
) -> bool:
    """True when the PACS names the study under another PatientID."""
    return any(
        not patient_id_matches(
            response.patient_id,
            order.pat_id,
            allow_suffix=unit.pacs_patient_id_wildcard,
        )
        for response in responses
        if response.study_uid == study_uid
    )


def prior_identity_matches(unit: Unit, order: Order, result: PriorSeriesResult) -> bool:
    """Accept a historical series only when it names the order's patient."""
    return (
        patient_id_matches(
            result.patient_id,
            order.pat_id,
            allow_suffix=unit.pacs_patient_id_wildcard,
        )
        and bool(result.birth_date)
        and result.birth_date.strip() == order.birth_date
    )
