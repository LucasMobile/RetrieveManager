"""Instance registry shared by the DICOM receiver and the compaction stage.

Every received object becomes a ``dicom_instances`` row identified by its
unit, SOP Instance UID and SHA-256. The same content arriving again (the second
C-MOVE, a PACS retry) is a duplicate; the same SOP with different content is
kept as a separate ``conflict`` row and file instead of replacing the first
version. The identity hash leaves out top-level private elements: some PACS
stamp one on every send (e.g. the user who last opened the study).
"""

from __future__ import annotations

import hashlib
import shutil
import struct
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pydicom
from pydicom.dataelem import RawDataElement
from pydicom.dataset import Dataset
from pydicom.filereader import read_dataset
from pydicom.uid import UID, DeflatedExplicitVRLittleEndian
from sqlalchemy import and_, delete, func, or_, select, text, update
from sqlalchemy.orm import Session

from app.db import INSTANCES_RECEIVED_CHANNEL
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
    # Hash of the exact bytes: rows recorded before the identity hash
    # left out private elements still match this copy.
    raw_sha256: str = ""

    def matches(self, stored_sha256: str) -> bool:
        return bool(stored_sha256) and stored_sha256 in (self.sha256, self.raw_sha256)


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


# Float, Double Float and (plain) Pixel Data.
PIXEL_DATA_TAGS = frozenset({0x7FE00008, 0x7FE00009, 0x7FE00010})
_UNDEFINED_LENGTH = 0xFFFFFFFF
# Explicit VR elements with a 2-byte reserved field and a 4-byte length.
_LONG_HEADER_VRS = frozenset(
    {"OB", "OD", "OF", "OL", "OV", "OW", "SQ", "SV", "UC", "UN", "UR", "UT", "UV"}
)


def _stop_at_pixel_data(tag, _vr, _length) -> bool:
    return int(tag) in PIXEL_DATA_TAGS


def parse_header(raw: bytes, transfer_syntax: UID) -> Dataset | None:
    """Unconverted elements up to Pixel Data, or None when the stream has no
    byte offsets to work with (deflated, big endian, unparseable)."""
    if (
        not transfer_syntax.is_little_endian
        or transfer_syntax == DeflatedExplicitVRLittleEndian
    ):
        return None
    try:
        return read_dataset(
            BytesIO(raw),
            transfer_syntax.is_implicit_VR,
            True,
            stop_when=_stop_at_pixel_data,
        )
    except Exception:
        return None


def _private_spans(
    raw: bytes, header: Dataset, is_implicit_VR: bool
) -> list[tuple[int, int]]:
    """Byte ranges of the top-level private elements of ``raw``.

    Only defined-length elements whose tag is found where expected are
    returned; anything else stays in the hash.
    """
    spans = []
    tags = list(header.keys())  # tags only: iterating would convert values
    for tag in tags:
        if not tag.is_private:
            continue
        element = header.get_item(tag)
        if (
            not isinstance(element, RawDataElement)
            or element.length == _UNDEFINED_LENGTH
            or element.value_tell is None
        ):
            continue
        if is_implicit_VR:
            header_size = 8
        else:
            header_size = 12 if element.VR in _LONG_HEADER_VRS else 8
        start = element.value_tell - header_size
        end = element.value_tell + element.length
        if start < 0 or end > len(raw):
            continue
        if raw[start : start + 4] != struct.pack("<HH", tag.group, tag.element):
            continue
        spans.append((start, end))
    return sorted(spans)


def identity_sha256(
    raw: bytes, transfer_syntax: UID, header: Dataset | None = None
) -> str:
    """SHA-256 of the dataset bytes without top-level private elements.

    Equal to the plain SHA-256 when there is nothing to leave out. ``header``
    must come from :func:`parse_header` on the same ``raw``.
    """
    if header is None:
        header = parse_header(raw, transfer_syntax)
    try:
        spans = (
            _private_spans(raw, header, transfer_syntax.is_implicit_VR)
            if header is not None
            else []
        )
    except Exception:
        spans = []  # never fail a C-STORE over the hash: fall back to the bytes
    digest = hashlib.sha256()
    position = 0
    for start, end in spans:
        if start < position:
            continue
        digest.update(raw[position:start])
        position = end
    digest.update(raw[position:])
    return digest.hexdigest()


def dataset_sha256(path: str | Path) -> tuple[str, int]:
    """Identity hash of the dataset after the File Meta group (as the receiver
    computes it), and the dataset size.

    The same object therefore has the same identity whether it arrived through
    the Store SCP or was adopted from the receive folder.
    """
    path = Path(path)
    offset = 0
    transfer_syntax = None
    try:
        meta = pydicom.filereader.read_file_meta_info(path)
        group_length = int(meta.get("FileMetaInformationGroupLength", 0) or 0)
        if group_length:
            offset = 128 + 4 + 12 + group_length
        if meta.get("TransferSyntaxUID"):
            transfer_syntax = UID(meta.TransferSyntaxUID)
    except Exception:
        offset = 0
    with path.open("rb") as stream:
        stream.seek(offset)
        raw = stream.read()
    if transfer_syntax is None:
        return hashlib.sha256(raw).hexdigest(), len(raw)
    return identity_sha256(raw, transfer_syntax), len(raw)


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
    return record_instances(db, [obj])[0]


def _by_unit(pairs: set[tuple[int, str]], unit_column, value_column):
    """``(unit, value) IN pairs`` as ``unit = u AND value IN (...)`` per unit,
    which the (unit_id, ...) indexes answer with one scan per unit."""
    values: dict[int, set[str]] = defaultdict(set)
    for unit_id, value in pairs:
        values[unit_id].add(value)
    return or_(
        *(
            and_(unit_column == unit_id, value_column.in_(sorted(unit_values)))
            for unit_id, unit_values in values.items()
        )
    )


def record_instances(db: Session, objs: Sequence[ReceivedObject]) -> list[Decision]:
    """Register received files, in order, inside the caller's transaction.

    The outcome is the same as recording them one at a time (a later object
    sees the rows of the earlier ones), with a fixed number of statements
    instead of several per object: one read of the rows of these SOPs, one of
    their studies, one flush for the new studies and one for the new
    instances; the study counters go with the commit.
    """
    if not objs:
        return []
    now = datetime.now()
    rows: dict[tuple[int, str], list[DicomInstance]] = defaultdict(list)
    for row in db.scalars(
        select(DicomInstance).where(
            _by_unit(
                {(obj.unit_id, obj.sop_uid) for obj in objs},
                DicomInstance.unit_id,
                DicomInstance.sop_uid,
            )
        )
    ):
        rows[(row.unit_id, row.sop_uid)].append(row)
    studies = {
        (study.unit_id, study.study_uid): study
        for study in db.scalars(
            select(DicomStudy).where(
                _by_unit(
                    {(obj.unit_id, obj.study_uid) for obj in objs},
                    DicomStudy.unit_id,
                    DicomStudy.study_uid,
                )
            )
        )
    }
    # Each decision keeps the row it refers to: a new row has no id until the
    # flush below.
    planned: list[tuple[DicomInstance, Decision]] = []
    created: list[tuple[DicomInstance, DicomStudy]] = []
    redelivered: set[int] = set()
    for obj in objs:
        known = rows[(obj.unit_id, obj.sop_uid)]
        same = next((row for row in known if obj.matches(row.source_sha256)), None)
        if same is not None:
            # A copy sent again still counts as a delivery: the worker checks
            # last_received_at to confirm that a C-MOVE reached this receiver.
            # (A row created in this batch already set it on its study.)
            if same.study_id is not None:
                redelivered.add(same.study_id)
            planned.append((same, _same_content(same, obj)))
            continue

        study = studies.get((obj.unit_id, obj.study_uid))
        if study is None:
            study = DicomStudy(
                unit_id=obj.unit_id,
                study_uid=obj.study_uid,
                instance_count=0,
                first_received_at=now,
                last_received_at=now,
            )
            db.add(study)
            studies[(obj.unit_id, obj.study_uid)] = study
        conflict = bool(known)
        if not conflict:
            study.instance_count += 1
        study.last_received_at = now
        instance = DicomInstance(
            unit_id=obj.unit_id,
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
                "Mesmo SOP Instance UID recebido com conteúdo diferente"
                if conflict
                else ""
            ),
            received_at=now,
        )
        known.append(instance)
        created.append((instance, study))
        planned.append(
            (
                instance,
                Decision(
                    "conflict" if conflict else "new",
                    0,
                    instance.state,
                    instance.source_path,
                    keep=True,
                ),
            )
        )
    if created:
        db.flush()  # new studies get their ids
        for instance, study in created:
            instance.study_id = study.id
        db.add_all(instance for instance, _study in created)
        db.flush()
    if redelivered:
        db.execute(
            update(DicomStudy)
            .where(DicomStudy.id.in_(sorted(redelivered)))
            .values(last_received_at=now)
        )
    return [replace(decision, instance_id=row.id) for row, decision in planned]


def notify_instances_received(db: Session, unit_ids: set[int]) -> None:
    """Wake the worker's compaction of these units once the caller commits."""
    for unit_id in sorted(unit_ids):
        db.execute(
            text("SELECT pg_notify(:channel, :payload)"),
            {"channel": INSTANCES_RECEIVED_CHANNEL, "payload": str(unit_id)},
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
