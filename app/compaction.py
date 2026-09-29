"""Per-file compaction step executed inside the isolated codec worker.

The worker reads the received file, applies charset, token and DICOM rules,
encodes the pixel data and writes a temporary output. It never moves or
deletes the source: promotion, discard and quarantine stay in the parent so a
killed worker cannot leave a half-applied result behind.
"""

from __future__ import annotations

import hashlib
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pydicom

from app.codec import METHOD_COPY, PROFILE_LOSSLESS, copy_reason, encode
from app.dicom_rules import RuleExecutionError, RuleMatch, RuleSpec, apply_rule_specs


@dataclass(frozen=True)
class CompactResult:
    source_name: str
    output_name: str
    study_uid: str
    status: str
    error_type: str = ""
    patient_id: str = ""
    birth_date: str = ""
    study_date: str = ""
    accession: str = ""
    modality: str = ""
    body_part: str = ""
    description: str = ""
    observed_at: datetime | None = None
    rule_matches: tuple[RuleMatch, ...] = ()
    codec_method: str = ""
    codec_reason: str = ""
    sender_aet: str = ""
    instance_id: int | None = None
    # Set for "compressed": the output waits in temp_output until its transfer
    # row (with this hash) is committed, then the parent publishes it.
    source_path: str = ""
    temp_output: str = ""
    output_sha256: str = ""

    @property
    def codec_skipped(self) -> bool:
        return self.codec_method == METHOD_COPY


@dataclass(frozen=True)
class CompactJob:
    source: str
    output_name: str
    temp_output: str
    token: str
    drops: frozenset[str]
    compress_map: dict[str, str]
    dicom_rules: tuple[RuleSpec, ...]
    allowed_senders: frozenset[str]
    max_encode_bytes: int
    # Last resort after a codec crash, timeout or failure: keep the pixel data.
    force_copy: bool = False


_ASCII_CHARACTER_SETS = frozenset({"", "ISO_IR 6", "ISO 2022 IR 6"})


def filename_modality(name: str) -> str:
    return name[:2].upper() if len(name) >= 2 else ""


def declare_latin1_when_undeclared(image: pydicom.Dataset) -> None:
    """Declare ISO_IR 100 only where the source relies on the ASCII default.

    Equipment often sends accented Latin-1 bytes without (0008,0005); declaring
    Latin-1 makes them readable and cannot alter valid ASCII. A declared charset
    (e.g. ISO_IR 192/UTF-8) is kept: relabeling it would re-encode the text and
    replace unsupported characters with "?".
    """
    raw = image.get("SpecificCharacterSet", "")
    values = [raw] if isinstance(raw, str) else list(raw or [])
    if all(str(value or "").strip() in _ASCII_CHARACTER_SETS for value in values):
        image.SpecificCharacterSet = "ISO_IR 100"


def prepare(job: CompactJob) -> CompactResult:
    source = Path(job.source)
    name = source.name
    prefix = filename_modality(name)
    try:
        image = pydicom.dcmread(source, defer_size="1 MB")
    except Exception as exc:
        return CompactResult(name, name, "", "compression_error", type(exc).__name__)

    if job.allowed_senders:
        # The receiver records the association's calling AE title in (0002,0016).
        file_meta = getattr(image, "file_meta", None)
        sender = str(getattr(file_meta, "SourceApplicationEntityTitle", "") or "")
        sender = sender.replace("\x00", " ").strip()
        if sender.upper() not in job.allowed_senders:
            return CompactResult(
                name,
                name,
                "",
                "rejected_sender",
                "UnauthorizedSender",
                sender_aet=sender[:16],
            )

    modality = str(getattr(image, "Modality", "") or prefix).upper()
    study_uid = str(getattr(image, "StudyInstanceUID", "") or "")
    identity = {
        "patient_id": str(getattr(image, "PatientID", "") or ""),
        "birth_date": str(getattr(image, "PatientBirthDate", "") or ""),
        "study_date": str(getattr(image, "StudyDate", "") or ""),
        "accession": str(getattr(image, "AccessionNumber", "") or ""),
        "modality": modality,
        "body_part": str(getattr(image, "BodyPartExamined", "") or ""),
        "description": str(getattr(image, "StudyDescription", "") or ""),
        "observed_at": datetime.fromtimestamp(source.stat().st_mtime),
    }
    # System-managed values. Rules cannot replace or remove these tags.
    declare_latin1_when_undeclared(image)
    image.InstitutionalDepartmentName = job.token
    try:
        rules = apply_rule_specs(image, job.dicom_rules)
    except RuleExecutionError as exc:
        return CompactResult(
            name,
            name,
            study_uid,
            "rule_error",
            type(exc).__name__,
            **identity,
            rule_matches=exc.matches,
        )
    if rules.delete_image:
        return CompactResult(
            name,
            "",
            study_uid,
            "discarded_rule",
            **identity,
            rule_matches=rules.matches,
        )
    processing_modality = str(getattr(image, "Modality", "") or prefix).upper()
    if prefix in job.drops or processing_modality in job.drops:
        return CompactResult(
            name,
            "",
            study_uid,
            "discarded_modality",
            **identity,
            rule_matches=rules.matches,
        )

    profile = job.compress_map.get(
        processing_modality, job.compress_map.get("*", PROFILE_LOSSLESS)
    )
    temp_output = Path(job.temp_output)
    try:
        reason = "forced" if job.force_copy else ""
        reason = reason or copy_reason(
            image, source.stat().st_size, job.max_encode_bytes
        )
        if reason:
            method = METHOD_COPY
        else:
            outcome = encode(image, profile)
            method, reason = outcome.method, outcome.reason
        image.save_as(temp_output)
        digest = file_sha256(temp_output)
    except Exception as exc:
        with suppress(OSError):
            temp_output.unlink(missing_ok=True)
        return CompactResult(
            name,
            name,
            study_uid,
            "compression_error",
            type(exc).__name__,
            **identity,
            rule_matches=rules.matches,
        )
    return CompactResult(
        name,
        job.output_name,
        study_uid,
        "compressed",
        **identity,
        rule_matches=rules.matches,
        codec_method=method,
        codec_reason=reason,
        source_path=str(source),
        temp_output=str(temp_output),
        output_sha256=digest,
    )


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
