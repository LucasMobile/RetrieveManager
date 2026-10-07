"""DICOM network services as SCU: C-ECHO, C-FIND and C-MOVE with pynetdicom.

Responses are returned as datasets and status codes instead of command-line
output. Every C-MOVE reports its sub-operation counts, so a retrieve in which
the PACS failed to send part of the images is not mistaken for a success.
"""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic

from pydicom.dataset import Dataset
from pynetdicom import AE
from pynetdicom.association import Association
from pynetdicom.sop_class import (
    StudyRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelMove,
    Verification,
)

from app.config import MOVE_IDLE_TIMEOUT_SECONDS
from app.wording import counted

# Same as the receiver: DICOM limits AE titles to 16 characters.
_PENDING = frozenset({0xFF00, 0xFF01})
STATUS_MOVE_DESTINATION_UNKNOWN = 0xA801
STATUS_SUBOPERATIONS_FAILED = 0xB000
STATUS_CANCELLED = 0xFE00


@dataclass(frozen=True)
class PacsNode:
    host: str
    port: int
    called_aet: str
    calling_aet: str

    @classmethod
    def from_unit(cls, unit) -> PacsNode:
        return cls(
            host=unit.pacs_ip.strip(),
            port=int(unit.pacs_port),
            called_aet=unit.pacs_aet.strip(),
            calling_aet=unit.calling_aet.strip(),
        )


@dataclass(frozen=True)
class FindResult:
    ok: bool
    responses: tuple[Dataset, ...] = ()
    status: int | None = None
    error: str = ""

    @property
    def summary(self) -> str:
        """Diagnostic line without patient data."""
        parts = [counted(len(self.responses), "resposta", "respostas")]
        if self.status is not None:
            parts.append(f"status final 0x{self.status:04X}")
        if self.error:
            parts.append(self.error)
        return "C-FIND: " + "; ".join(parts)


@dataclass(frozen=True)
class MoveResult:
    ok: bool
    status: int | None = None
    completed: int = 0
    failed: int = 0
    warning: int = 0
    remaining: int = 0
    error: str = ""

    @property
    def summary(self) -> str:
        parts = [
            counted(self.completed, "imagem enviada", "imagens enviadas"),
            f"falhas={self.failed}",
            f"avisos={self.warning}",
        ]
        if self.remaining:
            parts.append(f"pendentes={self.remaining}")
        if self.status is not None:
            parts.append(f"status final 0x{self.status:04X}")
        if self.error:
            parts.append(self.error)
        return "C-MOVE: " + "; ".join(parts)


@dataclass(frozen=True)
class EchoResult:
    ok: bool
    status: int | None = None
    error: str = ""


def _associate(node: PacsNode, context, timeout: float) -> tuple[Association, str]:
    ae = AE(ae_title=node.calling_aet)
    ae.add_requested_context(context)
    ae.connection_timeout = min(timeout, 15)
    ae.acse_timeout = min(timeout, 30)
    # Longest wait for a single response; the caller also enforces a deadline.
    ae.dimse_timeout = timeout
    ae.network_timeout = timeout
    assoc = ae.associate(node.host, node.port, ae_title=node.called_aet)
    if assoc.is_established:
        return assoc, ""
    if assoc.is_rejected:
        return assoc, "associação recusada pelo PACS"
    if assoc.is_aborted:
        return assoc, "sem conexão com o PACS ou associação abortada"
    return assoc, "falha ao conectar no PACS"


def _close(assoc: Association) -> None:
    if assoc.is_established:
        assoc.release()


def _status_error(status: int) -> str:
    if status == STATUS_MOVE_DESTINATION_UNKNOWN:
        return "PACS não conhece o AE de destino do C-MOVE"
    if status == STATUS_CANCELLED:
        return "operação cancelada"
    if status == STATUS_SUBOPERATIONS_FAILED:
        return "parte das imagens não foi enviada pelo PACS"
    return f"status de falha 0x{status:04X}"


def echo(node: PacsNode, *, timeout: float = 10) -> EchoResult:
    assoc, error = _associate(node, Verification, timeout)
    if error:
        return EchoResult(False, error=error)
    try:
        status = assoc.send_c_echo()
    finally:
        _close(assoc)
    if not status:
        return EchoResult(False, error="sem resposta ao C-ECHO")
    code = int(status.Status)
    return EchoResult(code == 0x0000, code, "" if code == 0 else _status_error(code))


def find(node: PacsNode, identifier: Dataset, *, timeout: float) -> FindResult:
    """Study Root C-FIND; only a final Success status counts as a result."""
    deadline = monotonic() + timeout
    assoc, error = _associate(node, StudyRootQueryRetrieveInformationModelFind, timeout)
    if error:
        return FindResult(False, error=error)
    responses: list[Dataset] = []
    final: int | None = None
    try:
        for status, response in assoc.send_c_find(
            identifier, StudyRootQueryRetrieveInformationModelFind
        ):
            if not status:
                error = "tempo esgotado ou associação interrompida"
                break
            code = int(status.Status)
            if code in _PENDING:
                if response is not None:
                    responses.append(response)
                if monotonic() > deadline:
                    error = "tempo esgotado"
                    assoc.abort()
                    break
                continue
            final = code
            break
    finally:
        _close(assoc)
    if not error and final != 0x0000:
        error = _status_error(final) if final is not None else "sem status final"
    return FindResult(not error, tuple(responses), final, error)


def move(
    node: PacsNode,
    identifier: Dataset,
    *,
    destination_aet: str,
    timeout: float,
    idle_timeout: float | None = None,
) -> MoveResult:
    """Study Root C-MOVE; success requires Success and zero failed sub-ops.

    ``timeout`` bounds the whole operation. ``idle_timeout`` bounds the time
    without progress once the PACS has sent its first pending response: a
    PACS that reports each sub-operation and then goes silent, or keeps
    answering without sending images, has stalled. The first response may
    take longer, since some PACS only answer after preparing the study.
    """
    if idle_timeout is None:
        idle_timeout = MOVE_IDLE_TIMEOUT_SECONDS
    deadline = monotonic() + timeout
    assoc, error = _associate(node, StudyRootQueryRetrieveInformationModelMove, timeout)
    if error:
        return MoveResult(False, error=error)
    counts = {"completed": 0, "failed": 0, "warning": 0, "remaining": 0}
    final: int | None = None
    progress: tuple[int, ...] | None = None
    last_response = last_progress = monotonic()
    idle_armed = False
    try:
        for status, _identifier in assoc.send_c_move(
            identifier, destination_aet, StudyRootQueryRetrieveInformationModelMove
        ):
            now = monotonic()
            if not status:
                if idle_armed and now - last_response >= idle_timeout - 1:
                    error = f"PACS sem enviar imagens por {int(idle_timeout)} s"
                else:
                    error = "tempo esgotado ou associação interrompida"
                break
            last_response = now
            for key, keyword in (
                ("completed", "NumberOfCompletedSuboperations"),
                ("failed", "NumberOfFailedSuboperations"),
                ("warning", "NumberOfWarningSuboperations"),
                ("remaining", "NumberOfRemainingSuboperations"),
            ):
                value = status.get(keyword)
                if value is not None:
                    counts[key] = int(value)
            code = int(status.Status)
            if code in _PENDING:
                if now > deadline:
                    # Aborting stops the PACS from sending further sub-ops.
                    error = "tempo esgotado"
                    assoc.abort()
                    break
                done = tuple(counts.values())
                if done != progress:
                    progress, last_progress = done, now
                elif idle_timeout and now - last_progress > idle_timeout:
                    error = f"PACS sem enviar imagens por {int(idle_timeout)} s"
                    assoc.abort()
                    break
                # pynetdicom reads the association's DIMSE timeout before each
                # wait: a silent PACS is now cut at the idle limit, and never
                # past the overall deadline.
                wait = max(1.0, deadline - now)
                if idle_timeout:
                    idle_armed = True
                    wait = min(wait, idle_timeout)
                assoc.dimse_timeout = wait
                continue
            final = code
            # Pending counts are stale once the final response arrives.
            counts["remaining"] = int(status.get("NumberOfRemainingSuboperations") or 0)
            break
    finally:
        _close(assoc)
    if not error and final is not None and final != 0x0000:
        error = _status_error(final)
    elif not error and final is None:
        error = "sem status final"
    if not error and counts["failed"]:
        error = _status_error(STATUS_SUBOPERATIONS_FAILED)
    return MoveResult(not error, final, error=error, **counts)


# -- identifiers ---------------------------------------------------------


def study_query(accession: str, birth_date: str) -> Dataset:
    ds = Dataset()
    ds.QueryRetrieveLevel = "STUDY"
    ds.AccessionNumber = accession
    ds.PatientBirthDate = birth_date
    ds.PatientID = ""
    ds.PatientName = ""
    ds.StudyInstanceUID = ""
    ds.ModalitiesInStudy = ""
    ds.BodyPartExamined = ""
    ds.NumberOfStudyRelatedInstances = ""
    return ds


def study_series_query(study_uid: str) -> Dataset:
    ds = Dataset()
    ds.QueryRetrieveLevel = "SERIES"
    ds.StudyInstanceUID = study_uid
    ds.SeriesInstanceUID = ""
    ds.Modality = ""
    ds.BodyPartExamined = ""
    ds.NumberOfSeriesRelatedInstances = ""
    return ds


def prior_series_query(
    *,
    body_part: str,
    modality: str,
    patient_id: str,
    birth_date: str,
    date_range: str,
    patient_id_wildcard: bool = False,
) -> Dataset:
    ds = Dataset()
    ds.QueryRetrieveLevel = "SERIES"
    ds.StudyInstanceUID = ""
    ds.SeriesInstanceUID = ""
    ds.AccessionNumber = ""
    ds.StudyDescription = ""
    ds.BodyPartExamined = body_part
    ds.Modality = modality
    ds.PatientID = f"{patient_id}*" if patient_id_wildcard else patient_id
    ds.PatientBirthDate = birth_date
    ds.StudyDate = date_range
    return ds


def study_move_identifier(study_uid: str) -> Dataset:
    ds = Dataset()
    ds.QueryRetrieveLevel = "STUDY"
    ds.StudyInstanceUID = study_uid
    return ds


def series_move_identifier(study_uid: str, series_uid: str) -> Dataset:
    ds = Dataset()
    ds.QueryRetrieveLevel = "SERIES"
    ds.StudyInstanceUID = study_uid
    ds.SeriesInstanceUID = series_uid
    return ds


# -- operations used by the pipeline (patched in tests) ------------------


def find_study(
    node: PacsNode, accession: str, birth_date: str, *, timeout: float
) -> FindResult:
    return find(node, study_query(accession, birth_date), timeout=timeout)


def find_study_series(node: PacsNode, study_uid: str, *, timeout: float) -> FindResult:
    return find(node, study_series_query(study_uid), timeout=timeout)


def find_prior_series(node: PacsNode, *, timeout: float, **criteria) -> FindResult:
    return find(node, prior_series_query(**criteria), timeout=timeout)


def move_study(node: PacsNode, study_uid: str, *, timeout: float) -> MoveResult:
    # The unit's Calling AET is also the receiver's AE: the PACS sends the
    # images back to us, as movescu did with its default move destination.
    return move(
        node,
        study_move_identifier(study_uid),
        destination_aet=node.calling_aet,
        timeout=timeout,
    )


def move_series(
    node: PacsNode, study_uid: str, series_uid: str, *, timeout: float
) -> MoveResult:
    return move(
        node,
        series_move_identifier(study_uid, series_uid),
        destination_aet=node.calling_aet,
        timeout=timeout,
    )
