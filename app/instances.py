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
from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import Session

from app.models import DicomInstance, DicomStudy, Unit

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
        # A copy sent again still counts as a delivery: the worker checks
        # last_received_at to confirm that a C-MOVE reached this receiver.
        db.execute(
            update(DicomStudy)
            .where(DicomStudy.id == same.study_id)
            .values(last_received_at=datetime.now())
        )
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


@dataclass(frozen=True)
class ClearResult:
    instances: int
    files_removed: int
    files_kept: int


def clear_issue_instances(
    db: Session, states: list[str], unit_id: int | None = None
) -> ClearResult:
    """Forget instances in ``states`` and delete their files, inside the caller's
    transaction, so the PACS can send the same objects again as new ones.

    The DELETE re-checks the state and locks the rows: an instance the receiver
    revives at the same moment is either committed first (and kept here) or
    waits for this transaction, fails and is acknowledged 0xA700, so the PACS
    sends it again (the receiver acknowledges early only SOPs it never
    recorded, never a revival). Files go before the commit, while the rows are
    locked.
    Only files inside the unit's receive or error folder are touched.
    """
    query = delete(DicomInstance).where(DicomInstance.state.in_(states))
    if unit_id:
        query = query.where(DicomInstance.unit_id == unit_id)
    rows = db.execute(
        query.returning(
            DicomInstance.unit_id,
            DicomInstance.study_id,
            DicomInstance.state,
            DicomInstance.source_path,
        ).execution_options(synchronize_session=False)
    ).all()
    if not rows:
        return ClearResult(0, 0, 0)

    counted: dict[int, int] = {}
    for row in rows:
        if row.state != "conflict":  # conflicts never counted in the study
            counted[row.study_id] = counted.get(row.study_id, 0) + 1
    for study_id, amount in counted.items():
        db.execute(
            update(DicomStudy)
            .where(DicomStudy.id == study_id)
            .values(instance_count=func.greatest(DicomStudy.instance_count - amount, 0))
        )

    folders = {
        unit.id: (Path(unit.receive_dir).resolve(), Path(unit.error_dir).resolve())
        for unit in db.scalars(
            select(Unit).where(Unit.id.in_({row.unit_id for row in rows}))
        )
    }
    removed = kept = 0
    for row in rows:
        path = Path(row.source_path).resolve(strict=False)
        if not any(path.is_relative_to(folder) for folder in folders[row.unit_id]):
            kept += 1
            continue
        try:
            path.unlink()
            removed += 1
        except FileNotFoundError:
            pass
        except OSError:
            kept += 1
    return ClearResult(len(rows), removed, kept)
