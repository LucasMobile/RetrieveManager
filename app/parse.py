import re
from dataclasses import dataclass

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


def parse_findscu_output(output: str) -> tuple[str, str, str, str]:
    """Return study UID, modalities, patient name and Body Part Examined."""
    study_uid = _first_bracket(output, "StudyInstanceUID")
    modalities = _first_bracket(output, "ModalitiesInStudy")
    name = _first_bracket(output, "PatientName")
    body_part = _first_bracket(output, "BodyPartExamined")
    return study_uid, modalities, name, body_part


def parse_series_body_part(output: str) -> str:
    """Return the first non-empty BodyPartExamined across all SERIES responses."""
    return _first_bracket(output, "BodyPartExamined")


def first_allowed_modality(
    raw: str, allowed_modalities: set[str] | frozenset[str]
) -> str:
    """Return the first exact DICOM modality present in the allowed catalog."""
    allowed = {value.strip().upper() for value in allowed_modalities if value.strip()}
    for value in re.split(r"[\\,\s]+", (raw or "").strip().upper()):
        if value in allowed:
            return value
    return ""


def parse_series_metadata(
    output: str, allowed_modalities: set[str] | frozenset[str]
) -> tuple[str, str]:
    """Select the first eligible SERIES response and its BodyPartExamined."""
    current: dict[str, str] | None = None
    responses: list[tuple[str, str]] = []

    def flush() -> None:
        nonlocal current
        if current is not None:
            responses.append(
                (current.get("modality", ""), current.get("body_part", ""))
            )
        current = None

    for line in (output or "").splitlines():
        if "Find Response:" in line:
            flush()
            current = {} if "Pending" in line else None
            continue
        if "Received Final Find Response" in line:
            flush()
            continue
        if current is None:
            continue
        if "(0008,0060)" in line:
            current["modality"] = _bracket_value(line)
        elif "(0018,0015)" in line:
            current["body_part"] = _bracket_value(line)
    flush()

    first_valid: tuple[str, str] | None = None
    for raw_modality, body_part in responses:
        modality = first_allowed_modality(raw_modality, allowed_modalities)
        if not modality:
            continue
        candidate = (modality, body_part)
        if body_part:
            return candidate
        if first_valid is None:
            first_valid = candidate
    return first_valid or ("", "")


def series_response_has_modality(output: str) -> bool:
    """Whether a pending SERIES response contains a non-empty Modality value."""
    pending = False
    for line in (output or "").splitlines():
        if "Find Response:" in line:
            pending = "Pending" in line
            continue
        if "Received Final Find Response" in line:
            pending = False
            continue
        if pending and "(0008,0060)" in line and _bracket_value(line):
            return True
    return False


def parse_prior_findscu_output(output: str) -> tuple[PriorSeriesResult, ...]:
    """Parse each pending SERIES response returned by DCMTK findscu."""
    field_names = {
        "StudyInstanceUID": "study_uid",
        "SeriesInstanceUID": "series_uid",
        "AccessionNumber": "accession",
        "StudyDate": "study_date",
        "Modality": "modality",
        "BodyPartExamined": "body_part",
        "StudyDescription": "description",
    }
    current: dict[str, str] | None = None
    results: list[PriorSeriesResult] = []

    def flush() -> None:
        nonlocal current
        if current and current.get("study_uid") and current.get("series_uid"):
            results.append(
                PriorSeriesResult(
                    **{
                        field: current.get(field, "")
                        for field in PriorSeriesResult.__dataclass_fields__
                    }
                )
            )
        current = None

    for line in (output or "").splitlines():
        if "Find Response:" in line:
            flush()
            current = {} if "Pending" in line else None
            continue
        if "Received Final Find Response" in line:
            flush()
            continue
        if current is None:
            continue
        for tag_name, field in field_names.items():
            if tag_name in line:
                current[field] = _bracket_value(line)
                break
    flush()

    unique: dict[str, PriorSeriesResult] = {}
    for result in results:
        unique[result.series_uid] = result
    return tuple(unique.values())


def _first_bracket(text: str, tag_name: str) -> str:
    for line in (text or "").splitlines():
        if tag_name in line and "[" in line and "]" in line:
            start = line.find("[")
            end = line.find("]", start)
            if start >= 0 and end > start:
                return _clean_dicom_text(line[start + 1 : end])
    return ""


def _bracket_value(line: str) -> str:
    if "[" not in line or "]" not in line:
        return ""
    start = line.find("[")
    end = line.find("]", start)
    return _clean_dicom_text(line[start + 1 : end]) if end > start else ""


def _clean_dicom_text(value: str) -> str:
    """Remove padding that cannot be persisted in PostgreSQL text columns."""
    return (value or "").replace("\x00", "").strip()
