"""DICOM Store SCP: one pynetdicom listener per unit, run by ``python -m app.receiver``.

Each C-STORE is written exactly as received (the header is parsed, the pixel
data is never decoded), hashed, and recorded in ``dicom_instances``. The PACS
only gets success after that row is committed; any failure before the commit
answers 0xA700 so the sender retries. Rows are committed by a single writer
thread that groups whatever is pending into one transaction.
"""

from __future__ import annotations

import hashlib
import logging
import os
import queue
import shutil
import signal
import threading
from concurrent.futures import Future
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from time import monotonic
from uuid import uuid4

from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.filebase import DicomBytesIO
from pydicom.filereader import read_dataset
from pydicom.filewriter import write_file_meta_info
from pydicom.uid import (
    JPEG2000,
    JPEG2000MC,
    UID,
    DeflatedExplicitVRLittleEndian,
    ExplicitVRBigEndian,
    ExplicitVRLittleEndian,
    ImplicitVRLittleEndian,
    JPEG2000Lossless,
    JPEG2000MCLossless,
    JPEGBaseline8Bit,
    JPEGExtended12Bit,
    JPEGLossless,
    JPEGLosslessSV1,
    JPEGLSLossless,
    JPEGLSNearLossless,
    RLELossless,
)
from pynetdicom import AE, AllStoragePresentationContexts, evt
from pynetdicom.sop_class import Verification
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.config import (
    RECEIVER_COMMIT_TIMEOUT_SECONDS,
    RECEIVER_FSYNC,
    RECEIVER_HEALTH_FILE,
    RECEIVER_MAX_ASSOCIATIONS,
    RECEIVER_MAX_PDU,
    RECEIVER_MIN_FREE_MB,
    WORKER_INTERVAL_SECONDS,
    store_bind_port,
)
from app.db import SessionLocal, init_db
from app.instances import (
    ACTIVE_STATES,
    DONE_STATES,
    Decision,
    ReceivedObject,
    apply_decision,
    record_instance,
)
from app.models import DicomInstance, Unit
from app.observability import configure_logging, log_event, safe_error_detail
from app.validation import (
    StoreNetwork,
    peer_ip_allowed,
    store_allowed_networks,
    store_allowed_senders,
)

log = logging.getLogger("receiver")

IMPLEMENTATION_CLASS_UID = UID("2.25.166979988622118132694098151125955657883")
IMPLEMENTATION_VERSION_NAME = "RETRIEVE_MGR"
ACCEPTED_TRANSFER_SYNTAXES = [
    ExplicitVRLittleEndian,
    ImplicitVRLittleEndian,
    DeflatedExplicitVRLittleEndian,
    ExplicitVRBigEndian,
    JPEGBaseline8Bit,
    JPEGExtended12Bit,
    JPEGLossless,
    JPEGLosslessSV1,
    JPEGLSLossless,
    JPEGLSNearLossless,
    JPEG2000Lossless,
    JPEG2000,
    JPEG2000MCLossless,
    JPEG2000MC,
    RLELossless,
]

STATUS_SUCCESS = 0x0000
STATUS_OUT_OF_RESOURCES = 0xA700  # retryable by the sender
STATUS_CANNOT_UNDERSTAND = 0xC000
STATUS_PROCESSING_FAILURE = 0x0110

# A-ASSOCIATE-RJ: rejected-permanent (0x01) by the service user (0x01).
REJECT_NO_REASON = 0x01
REJECT_CALLING_AE = 0x03
REJECT_CALLED_AE = 0x07

_PIXEL_DATA_TAGS = frozenset({0x7FE00008, 0x7FE00009, 0x7FE00010})


def _stop_at_pixel_data(tag, _vr, _length) -> bool:
    return int(tag) in _PIXEL_DATA_TAGS


def _decode_aet(value) -> str:
    if isinstance(value, bytes):
        value = value.decode("ascii", errors="replace")
    return str(value or "").strip()


def _safe_uid(value: str) -> str:
    return "".join(ch for ch in value if ch.isdigit() or ch == ".") or "UNKNOWN"


def _safe_modality(value: str) -> str:
    return "".join(ch for ch in value.upper() if ch.isalnum())[:8] or "OT"


def read_header(raw: bytes, transfer_syntax: UID, event=None) -> Dataset:
    """Parse the dataset up to (not including) Pixel Data.

    Deflated and big-endian streams, or any parse failure, fall back to the
    fully decoded dataset from pynetdicom; the file is still written raw.
    """
    if (
        transfer_syntax.is_little_endian
        and transfer_syntax != DeflatedExplicitVRLittleEndian
    ):
        try:
            return read_dataset(
                BytesIO(raw),
                transfer_syntax.is_implicit_VR,
                True,
                stop_when=_stop_at_pixel_data,
            )
        except Exception:
            if event is None:
                raise
    if event is None:
        raise ValueError("header parse requires the decoded C-STORE dataset")
    return event.dataset


def build_file_meta(
    header: Dataset, transfer_syntax: UID, calling_aet: str
) -> FileMetaDataset:
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = str(header.get("SOPClassUID", "") or "")
    meta.MediaStorageSOPInstanceUID = str(header.get("SOPInstanceUID", "") or "")
    meta.TransferSyntaxUID = transfer_syntax
    meta.ImplementationClassUID = IMPLEMENTATION_CLASS_UID
    meta.ImplementationVersionName = IMPLEMENTATION_VERSION_NAME
    if calling_aet:
        # The compaction stage re-checks the unit's sender allowlist here.
        meta.SourceApplicationEntityTitle = calling_aet[:16]
    return meta


def write_part10(path: Path, meta: FileMetaDataset, raw: bytes) -> None:
    """Write preamble + meta + the dataset bytes exactly as received, atomically."""
    encoded_meta = DicomBytesIO()
    encoded_meta.is_little_endian = True
    encoded_meta.is_implicit_VR = False
    write_file_meta_info(encoded_meta, meta, enforce_standard=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Hidden temporary name: folder scanners ignore it until the rename.
    temporary = path.with_name(f".rx-{uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(b"\x00" * 128)
            stream.write(b"DICM")
            stream.write(encoded_meta.getvalue())
            stream.write(raw)
            if RECEIVER_FSYNC:
                stream.flush()
                os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class InstanceWriter:
    """Single writer thread; every pending instance shares one commit."""

    def __init__(self, session_factory=SessionLocal, batch_size: int = 200) -> None:
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._queue: queue.Queue[tuple[ReceivedObject, Future]] = queue.Queue()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="receiver-db-writer", daemon=True
        )
        self._thread.start()

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def stop(self, timeout: float = 10) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def submit(self, obj: ReceivedObject) -> Future:
        future: Future = Future()
        self._queue.put((obj, future))
        return future

    def _run(self) -> None:
        while not (self._stop.is_set() and self._queue.empty()):
            try:
                first = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue
            batch = [first]
            while len(batch) < self._batch_size:
                try:
                    batch.append(self._queue.get_nowait())
                except queue.Empty:
                    break
            self._commit(
                [item for item in batch if item[1].set_running_or_notify_cancel()]
            )

    def _commit(self, batch: list[tuple[ReceivedObject, Future]]) -> None:
        if not batch:
            return
        try:
            with self._session_factory() as db:
                decisions = [record_instance(db, obj) for obj, _future in batch]
                db.commit()
        except Exception as exc:
            log_event(
                log,
                logging.WARNING,
                "dicom.receive.persist.batch",
                status="fallback",
                error=exc,
                error_detail=safe_error_detail(exc),
                instance_count=len(batch),
            )
        else:
            for (_obj, future), decision in zip(batch, decisions, strict=True):
                future.set_result(decision)
            return
        for obj, future in batch:
            try:
                future.set_result(self._commit_one(obj))
            except Exception as exc:
                future.set_exception(exc)

    def _commit_one(self, obj: ReceivedObject) -> Decision:
        # A concurrent registration (the worker adopting the same file) makes
        # the insert fail once; the retry then sees that row as a duplicate.
        for attempt in (1, 2):
            try:
                with self._session_factory() as db:
                    decision = record_instance(db, obj)
                    db.commit()
                    return decision
            except IntegrityError:
                if attempt == 2:
                    raise
        raise RuntimeError("unreachable")


@dataclass(frozen=True)
class ReceiverRoute:
    unit_id: int
    ae_title: str
    port: int
    receive_dir: Path
    error_dir: Path
    allowed_senders: frozenset[str]
    allowed_networks: tuple[StoreNetwork, ...] = ()

    @classmethod
    def from_unit(cls, unit: Unit) -> ReceiverRoute:
        return cls(
            unit_id=unit.id,
            ae_title=unit.calling_aet.strip(),
            port=store_bind_port(unit.store_port),
            receive_dir=Path(unit.receive_dir),
            error_dir=Path(unit.error_dir),
            allowed_senders=store_allowed_senders(unit.store_allowed_aets),
            allowed_networks=store_allowed_networks(unit.store_allowed_ips),
        )


@dataclass
class _AssociationStats:
    unit_id: int
    calling_aet: str
    peer_ip: str
    started_at: float
    stored: int = 0
    duplicate: int = 0
    conflict: int = 0
    failed: int = 0


class Receiver:
    def __init__(self, session_factory=SessionLocal, host: str = "0.0.0.0") -> None:
        self._session_factory = session_factory
        self._host = host
        self.writer = InstanceWriter(session_factory)
        self._servers: dict[int, tuple[ReceiverRoute, object]] = {}
        self._stats: dict[int, _AssociationStats] = {}
        self._stats_lock = threading.Lock()
        self._free_space: dict[Path, tuple[bool, float]] = {}

    def start(self) -> None:
        self.writer.start()

    def healthy(self) -> bool:
        return self.writer.is_alive()

    def stop(self) -> None:
        for unit_id in list(self._servers):
            self._stop_server(unit_id)
        self.writer.stop()

    def listening_units(self) -> dict[int, int]:
        return {unit_id: route.port for unit_id, (route, _) in self._servers.items()}

    def reconcile(self, units: list[Unit]) -> None:
        wanted = {
            unit.id: ReceiverRoute.from_unit(unit)
            for unit in units
            if unit.enabled and unit.deleted_at is None
        }
        for unit_id, (route, _server) in list(self._servers.items()):
            if wanted.get(unit_id) != route:
                self._stop_server(unit_id)
        for unit_id, route in wanted.items():
            if unit_id not in self._servers:
                self._start_server(route)

    def _start_server(self, route: ReceiverRoute) -> None:
        ae = AE(ae_title=route.ae_title)
        ae.maximum_pdu_size = RECEIVER_MAX_PDU
        ae.maximum_associations = RECEIVER_MAX_ASSOCIATIONS
        # AE titles are compared case-insensitively in _on_requested.
        ae.require_called_aet = False
        ae.implementation_class_uid = IMPLEMENTATION_CLASS_UID
        ae.implementation_version_name = IMPLEMENTATION_VERSION_NAME
        for context in AllStoragePresentationContexts:
            ae.add_supported_context(
                context.abstract_syntax, ACCEPTED_TRANSFER_SYNTAXES
            )
        ae.add_supported_context(
            Verification, [ExplicitVRLittleEndian, ImplicitVRLittleEndian]
        )
        handlers = [
            (evt.EVT_REQUESTED, self._on_requested, [route]),
            (evt.EVT_C_ECHO, self._on_echo),
            (evt.EVT_C_STORE, self._on_store, [route]),
            (evt.EVT_RELEASED, self._on_finished, ["released"]),
            (evt.EVT_ABORTED, self._on_finished, ["aborted"]),
        ]
        try:
            server = ae.start_server(
                (self._host, route.port), block=False, evt_handlers=handlers
            )
        except OSError as exc:
            log_event(
                log,
                logging.ERROR,
                "dicom.receive.start",
                resource=f"unit:{route.unit_id}",
                status="failure",
                error=exc,
                error_detail=safe_error_detail(exc),
                unit_id=route.unit_id,
                bind_port=route.port,
            )
            return
        self._servers[route.unit_id] = (route, server)
        log_event(
            log,
            logging.INFO,
            "dicom.receive.start",
            resource=f"unit:{route.unit_id}",
            status="success",
            unit_id=route.unit_id,
            bind_port=route.port,
            ae_title=route.ae_title,
            sender_allowlist=bool(route.allowed_senders),
        )

    def _stop_server(self, unit_id: int) -> None:
        _route, server = self._servers.pop(unit_id)
        try:
            server.shutdown()  # type: ignore[attr-defined]
        except Exception as exc:
            log_event(
                log,
                logging.WARNING,
                "dicom.receive.stop",
                resource=f"unit:{unit_id}",
                status="failure",
                error=exc,
                unit_id=unit_id,
            )

    # -- association -----------------------------------------------------

    def _on_requested(self, event, route: ReceiverRoute) -> None:
        assoc = event.assoc
        primitive = assoc.requestor.primitive
        called = _decode_aet(primitive.called_ae_title)
        calling = _decode_aet(primitive.calling_ae_title)
        peer_ip = str(assoc.requestor.address or "")
        if not peer_ip_allowed(peer_ip, route.allowed_networks):
            # Refused before any object is transferred, echo included.
            reason, diagnostic = "CallingAddressNotAllowed", REJECT_NO_REASON
        elif called.upper() != route.ae_title.upper():
            reason, diagnostic = "CalledAETitleNotRecognized", REJECT_CALLED_AE
        elif route.allowed_senders and calling.upper() not in route.allowed_senders:
            reason, diagnostic = "CallingAETitleNotRecognized", REJECT_CALLING_AE
        else:
            with self._stats_lock:
                self._stats[id(assoc)] = _AssociationStats(
                    route.unit_id, calling, peer_ip, monotonic()
                )
            return
        assoc.acse.send_reject(0x01, 0x01, diagnostic)
        # As pynetdicom does for its own rejections: kill() waits for the DUL
        # to flush the A-ASSOCIATE-RJ before the socket is closed.
        assoc.kill()
        log_event(
            log,
            logging.WARNING,
            "dicom.receive.association",
            resource=f"unit:{route.unit_id}",
            status="rejected",
            error_type=reason,
            unit_id=route.unit_id,
            calling_aet=calling,
            called_aet=called,
            peer_ip=peer_ip,
        )

    def _on_finished(self, event, outcome: str) -> None:
        with self._stats_lock:
            stats = self._stats.pop(id(event.assoc), None)
        if stats is None or not (
            stats.stored + stats.duplicate + stats.conflict + stats.failed
        ):
            return
        log_event(
            log,
            logging.WARNING if stats.failed or stats.conflict else logging.INFO,
            "dicom.receive.association",
            resource=f"unit:{stats.unit_id}",
            status=outcome,
            unit_id=stats.unit_id,
            calling_aet=stats.calling_aet,
            peer_ip=stats.peer_ip,
            stored_count=stats.stored,
            duplicate_count=stats.duplicate,
            conflict_count=stats.conflict,
            failed_count=stats.failed,
            duration_ms=round((monotonic() - stats.started_at) * 1000, 2),
        )

    def _count(self, event, field: str) -> None:
        with self._stats_lock:
            stats = self._stats.get(id(event.assoc))
            if stats is not None:
                setattr(stats, field, getattr(stats, field) + 1)

    @staticmethod
    def _on_echo(_event) -> int:
        return STATUS_SUCCESS

    # -- C-STORE ---------------------------------------------------------

    def _on_store(self, event, route: ReceiverRoute) -> int:
        calling = _decode_aet(event.assoc.requestor.ae_title)
        peer_ip = str(event.assoc.requestor.address or "")
        try:
            transfer_syntax = UID(event.context.transfer_syntax)
            raw = event.encoded_dataset(include_meta=False)
            header = read_header(raw, transfer_syntax, event)
            sop_uid = str(header.get("SOPInstanceUID", "") or "").strip()
            study_uid = str(header.get("StudyInstanceUID", "") or "").strip()
            if not sop_uid or not study_uid or max(len(sop_uid), len(study_uid)) > 64:
                self._count(event, "failed")
                log_event(
                    log,
                    logging.WARNING,
                    "dicom.receive.store",
                    resource=f"unit:{route.unit_id}",
                    status="rejected",
                    error_type="MissingInstanceIdentity",
                    unit_id=route.unit_id,
                    calling_aet=calling,
                )
                return STATUS_CANNOT_UNDERSTAND
            if not self._has_free_space(route.receive_dir):
                self._count(event, "failed")
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.receive.store",
                    resource=f"unit:{route.unit_id}",
                    status="refused",
                    error_type="InsufficientDiskSpace",
                    unit_id=route.unit_id,
                )
                return STATUS_OUT_OF_RESOURCES

            digest = hashlib.sha256(raw).hexdigest()
            modality = str(header.get("Modality", "") or "").strip()
            name = f"{_safe_modality(modality)}.{_safe_uid(sop_uid)}.{digest[:16]}"
            path = route.receive_dir / name
            obj = ReceivedObject(
                unit_id=route.unit_id,
                study_uid=study_uid,
                series_uid=str(header.get("SeriesInstanceUID", "") or "")[:64],
                sop_uid=sop_uid,
                sop_class_uid=str(header.get("SOPClassUID", "") or "")[:64],
                transfer_syntax=str(transfer_syntax),
                modality=modality[:16],
                sha256=digest,
                size=len(raw),
                path=str(path),
                conflict_path=str(route.error_dir / name),
                calling_aet=calling[:16],
                peer_ip=peer_ip[:64],
            )
            if self._already_recorded(obj):
                self._count(event, "duplicate")
                return STATUS_SUCCESS
            if not path.exists():
                write_part10(
                    path, build_file_meta(header, transfer_syntax, calling), raw
                )
            try:
                decision = self.writer.submit(obj).result(
                    timeout=RECEIVER_COMMIT_TIMEOUT_SECONDS
                )
            except Exception as exc:
                # Not acknowledged: the sender retries. The file stays; the
                # worker adopts it (or drops it as a duplicate) later.
                self._count(event, "failed")
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.receive.persist",
                    resource=f"unit:{route.unit_id}",
                    status="failure",
                    error=exc,
                    error_detail=safe_error_detail(exc),
                    unit_id=route.unit_id,
                    calling_aet=calling,
                )
                return STATUS_OUT_OF_RESOURCES
            apply_decision(decision, path)
            if decision.outcome == "conflict":
                self._count(event, "conflict")
                log_event(
                    log,
                    logging.WARNING,
                    "dicom.receive.conflict",
                    resource=f"instance:{decision.instance_id}",
                    status="quarantined",
                    unit_id=route.unit_id,
                    instance_id=decision.instance_id,
                    calling_aet=calling,
                )
            elif decision.outcome == "duplicate":
                self._count(event, "duplicate")
            else:
                self._count(event, "stored")
            return STATUS_SUCCESS
        except Exception as exc:
            self._count(event, "failed")
            log_event(
                log,
                logging.ERROR,
                "dicom.receive.store",
                resource=f"unit:{route.unit_id}",
                status="failure",
                error=exc,
                error_detail=safe_error_detail(exc),
                unit_id=route.unit_id,
                calling_aet=calling,
            )
            return STATUS_PROCESSING_FAILURE

    def _already_recorded(self, obj: ReceivedObject) -> bool:
        """Cheap read before writing: a resend of handled content is acked."""
        with self._session_factory() as db:
            row = db.execute(
                select(DicomInstance.state, DicomInstance.source_path).where(
                    DicomInstance.unit_id == obj.unit_id,
                    DicomInstance.sop_uid == obj.sop_uid,
                    DicomInstance.source_sha256 == obj.sha256,
                )
            ).first()
        if row is None:
            return False
        if row.state in DONE_STATES:
            return True
        return (row.state in ACTIVE_STATES or row.state == "conflict") and Path(
            row.source_path
        ).exists()

    def _has_free_space(self, directory: Path) -> bool:
        """Free-space check, cached briefly: it runs on every C-STORE."""
        now = monotonic()
        cached = self._free_space.get(directory)
        if cached is not None and now < cached[1]:
            return cached[0]
        try:
            directory.mkdir(parents=True, exist_ok=True)
            free = shutil.disk_usage(directory).free
        except OSError:
            return True  # let the write itself fail with the real error
        enough = free >= RECEIVER_MIN_FREE_MB * 1024 * 1024
        self._free_space[directory] = (enough, now + 2)
        return enough


def main() -> None:
    configure_logging()
    # Per-PDU INFO messages would flood the JSON log; warnings still pass.
    logging.getLogger("pynetdicom").setLevel(logging.WARNING)
    init_db()
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_args: stop.set())
    signal.signal(signal.SIGTERM, lambda *_args: stop.set())
    receiver = Receiver()
    receiver.start()
    try:
        while not stop.is_set():
            try:
                with SessionLocal() as db:
                    units = list(
                        db.scalars(select(Unit).where(Unit.deleted_at.is_(None)))
                    )
                    receiver.reconcile(units)
                if receiver.healthy():
                    RECEIVER_HEALTH_FILE.touch()
            except Exception as exc:
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.receive.reconcile",
                    status="failure",
                    error=exc,
                    error_detail=safe_error_detail(exc),
                )
            stop.wait(WORKER_INTERVAL_SECONDS)
    finally:
        receiver.stop()
        RECEIVER_HEALTH_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
