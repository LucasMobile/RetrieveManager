"""Interpret C-FIND responses (pydicom datasets) returned by the PACS."""

import re
from collections.abc import Iterable
from dataclasses import dataclass

from pydicom.dataset import Dataset
from pydicom.multival import MultiValue

from app.config import KNOWN_MODALITIES


@dataclass(frozen=True)
class PriorSeriesResult:
    study_uid: str
    series_uid: str
    accession: str = ""
    study_date: str = ""
    modality: str = ""
    body_part: str = ""
    description: str = ""
    patient_id: str = ""
    birth_date: str = ""


@dataclass(frozen=True)
class StudyFindResponse:
    study_uid: str = ""
    patient_id: str = ""
    birth_date: str = ""
    accession: str = ""
    modalities: str = ""
    patient_name: str = ""
    body_part: str = ""
    instance_count: str = ""


def dataset_text(ds: Dataset, keyword: str) -> str:
    """Text value of a response element; multi-values joined with a backslash."""
    value = ds.get(keyword, "")
    if isinstance(value, MultiValue | list | tuple):
        value = "\\".join(str(item) for item in value)
    # PostgreSQL text columns reject NUL bytes from non-conformant padding.
    return str(value if value is not None else "").replace("\x00", "").strip()


def normalize_modality(
    modalities_in_study: str, known: tuple[str, ...] = KNOWN_MODALITIES
) -> str:
    raw = (modalities_in_study or "").strip()
    chosen = ""
    for mod in known:
        if mod and mod in raw:
            chosen = mod
    if chosen:
        return chosen
    if not raw:
        return ""
    return raw.replace("\\", " ").split()[0]


def study_response(ds: Dataset) -> StudyFindResponse:
    return StudyFindResponse(
        study_uid=dataset_text(ds, "StudyInstanceUID"),
        patient_id=dataset_text(ds, "PatientID"),
        birth_date=dataset_text(ds, "PatientBirthDate"),
        accession=dataset_text(ds, "AccessionNumber"),
        modalities=dataset_text(ds, "ModalitiesInStudy"),
        patient_name=dataset_text(ds, "PatientName"),
        body_part=dataset_text(ds, "BodyPartExamined"),
        instance_count=dataset_text(ds, "NumberOfStudyRelatedInstances"),
    )


def prior_series_results(
    responses: Iterable[Dataset],
) -> tuple[PriorSeriesResult, ...]:
    """One result per Series UID; responses without both UIDs are ignored."""
    unique: dict[str, PriorSeriesResult] = {}
    for ds in responses:
        result = PriorSeriesResult(
            study_uid=dataset_text(ds, "StudyInstanceUID"),
            series_uid=dataset_text(ds, "SeriesInstanceUID"),
            accession=dataset_text(ds, "AccessionNumber"),
            study_date=dataset_text(ds, "StudyDate"),
            modality=dataset_text(ds, "Modality"),
            body_part=dataset_text(ds, "BodyPartExamined"),
            description=dataset_text(ds, "StudyDescription"),
            patient_id=dataset_text(ds, "PatientID"),
            birth_date=dataset_text(ds, "PatientBirthDate"),
        )
        if result.study_uid and result.series_uid:
            unique[result.series_uid] = result
    return tuple(unique.values())


def patient_id_matches(returned: str, expected: str, *, allow_suffix: bool) -> bool:
    """Compare a PACS PatientID with the order's ID; prefix only when enabled."""
    returned, expected = (returned or "").strip(), (expected or "").strip()
    if not returned or not expected:
        return False
    return returned == expected or (allow_suffix and returned.startswith(expected))


def first_allowed_modality(
    raw: str, allowed_modalities: set[str] | frozenset[str]
) -> str:
    """Return the first exact DICOM modality present in the allowed catalog."""
    allowed = {value.strip().upper() for value in allowed_modalities if value.strip()}
    for value in re.split(r"[\\,\s]+", (raw or "").strip().upper()):
        if value in allowed:
            return value
    return ""


def series_metadata(
    responses: Iterable[Dataset], allowed_modalities: set[str] | frozenset[str]
) -> tuple[str, str]:
    """Select the first eligible series, preferring one with BodyPartExamined."""
    first_valid: tuple[str, str] | None = None
    for ds in responses:
        modality = first_allowed_modality(
            dataset_text(ds, "Modality"), allowed_modalities
        )
        if not modality:
            continue
        body_part = dataset_text(ds, "BodyPartExamined")
        if body_part:
            return modality, body_part
        if first_valid is None:
            first_valid = (modality, body_part)
    return first_valid or ("", "")


def series_have_modality(responses: Iterable[Dataset]) -> bool:
    """Whether any SERIES response carries a non-empty Modality."""
    return any(dataset_text(ds, "Modality") for ds in responses)


def first_series_body_part(responses: Iterable[Dataset]) -> str:
    """First non-empty BodyPartExamined across the SERIES responses."""
    for ds in responses:
        body_part = dataset_text(ds, "BodyPartExamined")
        if body_part:
            return body_part
    return ""
