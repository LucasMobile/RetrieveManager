"""Instance registry shared by the DICOM receiver and the compaction stage.

Every received object becomes a ``dicom_instances`` row identified by its
unit, SOP Instance UID and SHA-256. The same bytes arriving again (the second
C-MOVE, a PACS retry) are duplicates; the same SOP with different bytes is kept
as a separate ``conflict`` row and file instead of replacing the first version.
"""

from __future__ import annotations

import hashlib
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pydicom
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import DicomInstance, DicomStudy

ACTIVE_STATES = frozenset({"received", "compacting"})
DONE_STATES = frozenset({"compacted", "discarded", "rejected"})
REVIVABLE_STATES = frozenset({"error", "missing"})


@dataclass(frozen=True)
class ReceivedObject:
    unit_id: int
    study_uid: str
    series_uid: str
    sop_uid: str
    sop_class_uid: str
    transfer_syntax: str
    modality: str
    sha256: str
    size: int
    path: str
    conflict_path: str
    calling_aet: str = ""
    peer_ip: str = ""


@dataclass(frozen=True)
class Decision:
    """What happened to a received file and what the caller must do with it.

    ``keep`` False: the file is a redundant copy of content already handled and
    must be removed. ``row_path`` different from the incoming path: the file
    must be moved there (a conflicting version kept in the error folder).
    """

    outcome: str  # new | revived | duplicate | conflict
    instance_id: int
    row_state: str
    row_path: str
    keep: bool


def dataset_sha256(path: str | Path) -> tuple[str, int]:
    """Hash the dataset bytes after the File Meta group (as the receiver does).

    The same object therefore has the same identity whether it arrived through
    the Store SCP or was adopted from the receive folder.
    """
    path = Path(path)
    offset = 0
    try:
        meta = pydicom.filereader.read_file_meta_info(path)
        group_length = int(meta.get("FileMetaInformationGroupLength", 0) or 0)
        if group_length:
            offset = 128 + 4 + 12 + group_length
    except Exception:
        offset = 0
    size = path.stat().st_size - offset
    with path.open("rb") as stream:
        stream.seek(offset)
        return hashlib.file_digest(stream, "sha256").hexdigest(), size


def apply_decision(decision: Decision, path: Path) -> None:
    """Remove a redundant copy or move a conflicting version aside."""
    if not decision.keep:
        path.unlink(missing_ok=True)
    elif decision.row_path != str(path):
        target = Path(decision.row_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), str(target))


def record_instance(db: Session, obj: ReceivedObject) -> Decision:
    """Register one received file inside the caller's transaction."""
    rows = list(
        db.scalars(
            select(DicomInstance).where(
                DicomInstance.unit_id == obj.unit_id,
                DicomInstance.sop_uid == obj.sop_uid,
            )
        )
    )
    same = next((row for row in rows if row.source_sha256 == obj.sha256), None)
    if same is not None:
        return _same_content(same, obj)

    now = datetime.now()
    study = db.scalar(
        select(DicomStudy).where(
            DicomStudy.unit_id == obj.unit_id,
            DicomStudy.study_uid == obj.study_uid,
        )
    )
    if study is None:
        study = DicomStudy(
            unit_id=obj.unit_id,
            study_uid=obj.study_uid,
            instance_count=0,
            first_received_at=now,
            last_received_at=now,
        )
        db.add(study)
        db.flush()
    conflict = bool(rows)
    if not conflict:
        study.instance_count += 1
    study.last_received_at = now
    instance = DicomInstance(
        unit_id=obj.unit_id,
        study_id=study.id,
        series_uid=obj.series_uid,
        sop_uid=obj.sop_uid,
        sop_class_uid=obj.sop_class_uid,
        transfer_syntax=obj.transfer_syntax,
        modality=obj.modality,
        source_path=obj.conflict_path if conflict else obj.path,
        source_bytes=obj.size,
        source_sha256=obj.sha256,
        calling_aet=obj.calling_aet,
        peer_ip=obj.peer_ip,
        state="conflict" if conflict else "received",
        last_error=(
            "Mesmo SOP Instance UID recebido com conteúdo diferente" if conflict else ""
        ),
        received_at=now,
    )
    db.add(instance)
    db.flush()
    return Decision(
        "conflict" if conflict else "new",
        instance.id,
        instance.state,
        instance.source_path,
        keep=True,
    )


def _same_content(row: DicomInstance, obj: ReceivedObject) -> Decision:
    if row.state in REVIVABLE_STATES:
        # A failed or lost instance was sent again: process this copy.
        row.state = "received"
        row.source_path = obj.path
        row.claimed_at = None
        row.last_error = ""
        return Decision("revived", row.id, row.state, row.source_path, keep=True)
    if row.state in DONE_STATES:
        # Already compacted/discarded; its source was consumed on purpose.
        return Decision("duplicate", row.id, row.state, row.source_path, keep=False)
    if row.source_path == obj.path:
        return Decision("duplicate", row.id, row.state, row.source_path, keep=True)
    if Path(row.source_path).exists():
        return Decision("duplicate", row.id, row.state, row.source_path, keep=False)
    # The tracked file vanished; this identical copy takes its place.
    row.source_path = obj.conflict_path if row.state == "conflict" else obj.path
    return Decision("duplicate", row.id, row.state, row.source_path, keep=True)
