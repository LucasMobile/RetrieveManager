"""DICOM Store SCP run by ``python -m app.receiver``.

One pynetdicom listener per port. Units may share a port and called AE title;
the routing table (``app.store_routing``) picks the unit of each association
from its called and calling AE titles, and the association keeps that unit.

Each C-STORE is written exactly as received (the header is parsed, the pixel
data is never decoded), hashed, and recorded in ``dicom_instances``. Rows are
committed by a single writer thread that groups whatever is pending into one
transaction.

A SOP the unit never recorded is acknowledged once its file is on disk; its row
is committed right after, without the PACS waiting for it (``RECEIVER_EARLY_ACK``).
Everything else waits for the commit before success, as does every object
while the writer is failing or its queue is long: resends, revivals of failed
instances and conflicting versions. Before any file is written the database is
read, so an unreachable database answers 0xA700 and the sender retries. An
acknowledged file whose row could not be committed stays in the receive folder,
where the worker adopts it.
"""

from __future__ import annotations

import hashlib
import logging
import os
import queue
import shutil
import signal
import threading
from collections import Counter, defaultdict
from collections.abc import Mapping
from concurrent.futures import Future
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from pathlib import Path
from time import monotonic
from uuid import uuid4

import psycopg
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
from sqlalchemy import URL, select, update
from sqlalchemy.exc import IntegrityError

from app.config import (
    DATABASE_URL,
    RECEIVER_COMMIT_TIMEOUT_SECONDS,
    RECEIVER_EARLY_ACK,
    RECEIVER_EARLY_ACK_MAX_PENDING,
    RECEIVER_FSYNC,
    RECEIVER_HEALTH_FILE,
    RECEIVER_MAX_ASSOCIATIONS,
    RECEIVER_MAX_PDU,
    RECEIVER_MIN_FREE_MB,
    WORKER_INTERVAL_SECONDS,
    store_bind_port,
)
from app.db import UNITS_CHANGED_CHANNEL, SessionLocal, init_db
from app.instances import (
    ACTIVE_STATES,
    DONE_STATES,
    Decision,
    ReceivedObject,
    apply_decision,
    record_instance,
)
from app.models import DicomInstance, DicomStudy, Unit
from app.observability import configure_logging, log_event
from app.store_routing import (
    RoutingPlan,
    StoreEndpoint,
    build_routing_plan,
    normalize_aet,
    parse_senders,
)
from app.validation import (
    StoreNetwork,
    peer_ip_allowed,
    store_allowed_networks,
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

# A-ASSOCIATE-RJ result, source and diagnostic (PS3.8 9.3.4).
REJECT_PERMANENT = 0x01
REJECT_TRANSIENT = 0x02  # the sender may try again later
SOURCE_SERVICE_USER = 0x01
SOURCE_PROVIDER_PRESENTATION = 0x03
REJECT_NO_REASON = 0x01
REJECT_CALLING_AE = 0x03
REJECT_CALLED_AE = 0x07
REJECT_LOCAL_LIMIT = 0x02  # with SOURCE_PROVIDER_PRESENTATION

_PIXEL_DATA_TAGS = frozenset({0x7FE00008, 0x7FE00009, 0x7FE00010})

# What the database knows about an incoming object before its file is written.
KNOWN_DUPLICATE = "duplicate"  # same content already handled: ack, write nothing
NEVER_RECORDED = "new"  # the unit has no row for this SOP: may be acked early
KNOWN_SOP = "known"  # revival, conflict or lost source: wait for the commit
# A duplicate marks its study as delivered again at most this often per
# association (see Receiver._note_duplicate).
DUPLICATE_TOUCH_SECONDS = 5.0


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
    """Single writer thread; every pending instance shares one commit.

    A future's callbacks run on this thread when its commit ends, before the
    instance counts as done for ``wait_idle``.
    """

    def __init__(self, session_factory=SessionLocal, batch_size: int = 200) -> None:
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._queue: queue.Queue[tuple[ReceivedObject, Future]] = queue.Queue()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # Set when the last commit lost an instance; cleared by the next one
        # that records everything. Read without a lock: a stale value only
        # makes one C-STORE wait for its commit, or not.
        self._failing = False

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

    def pending(self) -> int:
        """Instances submitted and not yet committed (or failed)."""
        return self._queue.unfinished_tasks

    def accepts_early_ack(self) -> bool:
        """Whether a new object may be acknowledged before its commit."""
        return (
            self.is_alive()
            and not self._failing
            and self.pending() < RECEIVER_EARLY_ACK_MAX_PENDING
        )

    def wait_idle(self, timeout: float) -> bool:
        """Wait until every submitted instance is committed or failed."""
        deadline = monotonic() + timeout
        with self._queue.all_tasks_done:
            while self._queue.unfinished_tasks:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    return False
                self._queue.all_tasks_done.wait(remaining)
        return True

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
            try:
                self._commit(
                    [item for item in batch if item[1].set_running_or_notify_cancel()]
                )
            finally:
                for _item in batch:
                    self._queue.task_done()

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
                instance_count=len(batch),
            )
        else:
            self._failing = False
            for (_obj, future), decision in zip(batch, decisions, strict=True):
                future.set_result(decision)
            return
        failed = False
        for obj, future in batch:
            try:
                decision = self._commit_one(obj)
            except Exception as exc:
                failed = True
                self._failing = True
                future.set_exception(exc)
            else:
                future.set_result(decision)
        self._failing = failed

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
    """Where one unit's associations land; any change aborts the open ones."""

    unit_id: int
    port: int
    called_aet: str
    receive_dir: Path
    error_dir: Path
    allowed_senders: frozenset[str]
    allowed_networks: tuple[StoreNetwork, ...] = ()

    @classmethod
    def from_unit(cls, unit: Unit) -> ReceiverRoute:
        return cls(
            unit_id=unit.id,
            port=store_bind_port(unit.store_port),
            called_aet=normalize_aet(unit.calling_aet),
            receive_dir=Path(unit.receive_dir),
            error_dir=Path(unit.error_dir),
            allowed_senders=parse_senders(unit.store_allowed_aets),
            allowed_networks=store_allowed_networks(unit.store_allowed_ips),
        )


@dataclass(frozen=True)
class RoutingTable:
    """One consistent snapshot of the units; replaced whole on reconcile."""

    plan: RoutingPlan = field(default_factory=RoutingPlan)
    # Units able to receive right now: enabled and without routing problems.
    routes: Mapping[int, ReceiverRoute] = field(default_factory=dict)

    @classmethod
    def from_units(cls, units: list[Unit]) -> RoutingTable:
        current = [unit for unit in units if unit.deleted_at is None]
        # Paused units keep their routing keys: they are refused, never
        # handed to a neighbour on the same port.
        plan = build_routing_plan(StoreEndpoint.from_unit(unit) for unit in current)
        return cls(
            plan=plan,
            routes={
                unit.id: ReceiverRoute.from_unit(unit)
                for unit in current
                if unit.enabled and unit.id not in plan.unit_problems
            },
        )


@dataclass(frozen=True)
class _Rejection:
    reason: str
    result: int
    diagnostic: int
    source: int = SOURCE_SERVICE_USER


_REJECT_CALLED = _Rejection(
    "CalledAETitleNotRecognized", REJECT_PERMANENT, REJECT_CALLED_AE
)
_REJECT_CALLING = _Rejection(
    "CallingAETitleNotRecognized", REJECT_PERMANENT, REJECT_CALLING_AE
)
_REJECT_AMBIGUOUS = _Rejection(
    "AmbiguousStoreRoute", REJECT_PERMANENT, REJECT_CALLING_AE
)
_REJECT_ADDRESS = _Rejection(
    "CallingAddressNotAllowed", REJECT_PERMANENT, REJECT_NO_REASON
)
_REJECT_UNAVAILABLE = _Rejection("UnitUnavailable", REJECT_TRANSIENT, REJECT_NO_REASON)
_REJECT_ROUTING_ERROR = _Rejection(
    "StoreRoutingError", REJECT_TRANSIENT, REJECT_NO_REASON
)
_REJECT_LIMIT = _Rejection(
    "UnitAssociationLimit",
    REJECT_TRANSIENT,
    REJECT_LOCAL_LIMIT,
    SOURCE_PROVIDER_PRESENTATION,
)

# Attribute of the pynetdicom Association that carries its pinned route.
_STATE_ATTR = "_retrieve_state"


@dataclass
class _AssociationState:
    route: ReceiverRoute
    calling_aet: str
    peer_ip: str
    started_at: float
    stored: int = 0
    duplicate: int = 0
    conflict: int = 0
    failed: int = 0
    # Acknowledged early, then not recorded: left for the worker to adopt.
    unrecorded: int = 0
    finished: bool = False
    # Study UID -> monotonic time its duplicates last refreshed last_received_at.
    duplicate_touched: dict[str, float] = field(default_factory=dict, repr=False)
    # Early acknowledgements whose commit has not ended yet. The writer thread
    # updates the counters of those, hence the lock.
    _early_pending: int = field(default=0, repr=False)
    _lock: threading.Condition = field(
        default_factory=threading.Condition, repr=False, compare=False
    )

    def count(self, counter: str) -> None:
        with self._lock:
            setattr(self, counter, getattr(self, counter) + 1)

    def early_ack_started(self) -> None:
        with self._lock:
            self._early_pending += 1

    def early_ack_ended(self) -> None:
        with self._lock:
            self._early_pending -= 1
            if not self._early_pending:
                self._lock.notify_all()

    def wait_early_acks(self, timeout: float) -> int:
        """Wait for the early acknowledgements' commits; return those left."""
        with self._lock:
            self._lock.wait_for(lambda: not self._early_pending, timeout)
            return self._early_pending


def _state(assoc) -> _AssociationState | None:
    return getattr(assoc, _STATE_ATTR, None)


class Receiver:
    """One listener per port; the routing table picks the unit per association."""

    def __init__(self, session_factory=SessionLocal, host: str = "0.0.0.0") -> None:
        self._session_factory = session_factory
        self._host = host
        self.writer = InstanceWriter(session_factory)
        self._table = RoutingTable()
        self._listeners: dict[int, object] = {}
        # Guards the table swap and the registry of open associations: an
        # association is either aborted by a reconcile or admitted against
        # the table that reconcile installed, never missed in between.
        self._lock = threading.Lock()
        self._associations: dict[int, set] = defaultdict(set)
        self._free_space: dict[Path, tuple[bool, float]] = {}
        self._stopping = False

    def start(self) -> None:
        self.writer.start()

    def healthy(self) -> bool:
        return self.writer.is_alive()

    def stop(self) -> None:
        # Associations aborted by the shutdown do not wait for their commits;
        # the writer still drains its queue below.
        self._stopping = True
        for port in list(self._listeners):
            self._stop_listener(port)
        self.writer.stop()
        if self.writer.pending():
            # Their files stay in the receive folder; the worker adopts them.
            log_event(
                log,
                logging.ERROR,
                "dicom.receive.persist",
                status="interrupted",
                error_type="WriterStoppedWithPendingInstances",
                instance_count=self.writer.pending(),
            )

    def listening_ports(self) -> set[int]:
        return set(self._listeners)

    def reconcile(self, units: list[Unit]) -> None:
        table = RoutingTable.from_units(units)
        with self._lock:
            previous, self._table = self._table, table
            stale = []
            for unit_id, open_assocs in self._associations.items():
                current = table.routes.get(unit_id)
                for assoc in list(open_assocs):
                    state = _state(assoc)
                    if state is None or not assoc.is_alive():
                        open_assocs.discard(assoc)
                    elif state.route != current:
                        open_assocs.discard(assoc)
                        stale.append(assoc)
        self._log_route_problems(previous.plan, table.plan)
        for assoc in stale:
            self._abort_stale(assoc)

        per_port = Counter(route.port for route in table.routes.values())
        for port in list(self._listeners):
            if port not in per_port:
                self._stop_listener(port)
        for port, unit_count in per_port.items():
            server = self._listeners.get(port) or self._start_listener(port, table)
            if server is not None:
                # pynetdicom's limit covers the whole port; each unit's own
                # share is checked on admission.
                server.ae.maximum_associations = (  # type: ignore[attr-defined]
                    RECEIVER_MAX_ASSOCIATIONS * unit_count
                )

    def _abort_stale(self, assoc) -> None:
        """Abort an association whose unit was paused, archived or changed."""
        state = _state(assoc)
        unit_id = state.route.unit_id if state else None
        log_event(
            log,
            logging.WARNING,
            "dicom.receive.association",
            resource=f"unit:{unit_id}",
            status="aborted",
            error_type="StoreRouteChanged",
            unit_id=unit_id,
            calling_aet=state.calling_aet if state else None,
        )
        # Blocking abort, as pynetdicom's own shutdown does, but off this
        # thread: a stalled peer must not hold up the reconcile loop.
        threading.Thread(target=assoc.abort, daemon=True).start()

    @staticmethod
    def _log_route_problems(previous: RoutingPlan, current: RoutingPlan) -> None:
        for unit_id, problem in current.unit_problems.items():
            if previous.unit_problems.get(unit_id) != problem:
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.receive.route",
                    resource=f"unit:{unit_id}",
                    status="blocked",
                    error_type="StoreRouteConflict",
                    unit_id=unit_id,
                    reason=problem,
                )
        for unit_id in previous.unit_problems.keys() - current.unit_problems.keys():
            log_event(
                log,
                logging.INFO,
                "dicom.receive.route",
                resource=f"unit:{unit_id}",
                status="success",
                unit_id=unit_id,
            )

    def _start_listener(self, port: int, table: RoutingTable):
        unit_ids = sorted(
            route.unit_id for route in table.routes.values() if route.port == port
        )
        # The A-ASSOCIATE-AC echoes each request's called AE title, so this
        # title is never seen by the senders.
        ae = AE(ae_title="RETRIEVE")
        ae.maximum_pdu_size = RECEIVER_MAX_PDU
        # Called and calling AE titles are matched by the routing table.
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
            (evt.EVT_REQUESTED, self._on_requested, [port]),
            (evt.EVT_C_ECHO, self._on_echo),
            (evt.EVT_C_STORE, self._on_store),
            (evt.EVT_RELEASED, self._on_finished, ["released"]),
            (evt.EVT_ABORTED, self._on_finished, ["aborted"]),
        ]
        try:
            server = ae.start_server(
                (self._host, port), block=False, evt_handlers=handlers
            )
        except OSError as exc:
            log_event(
                log,
                logging.ERROR,
                "dicom.receive.start",
                resource=f"port:{port}",
                status="failure",
                error=exc,
                bind_port=port,
                unit_ids=unit_ids,
            )
            return None
        self._listeners[port] = server
        log_event(
            log,
            logging.INFO,
            "dicom.receive.start",
            resource=f"port:{port}",
            status="success",
            bind_port=port,
            unit_ids=unit_ids,
        )
        return server

    def _stop_listener(self, port: int) -> None:
        server = self._listeners.pop(port)
        try:
            server.shutdown()  # type: ignore[attr-defined]
        except Exception as exc:
            log_event(
                log,
                logging.WARNING,
                "dicom.receive.stop",
                resource=f"port:{port}",
                status="failure",
                error=exc,
                bind_port=port,
            )

    # -- association -----------------------------------------------------

    def _on_requested(self, event, port: int) -> None:
        assoc = event.assoc
        called = calling = peer_ip = ""
        unit_id = None
        try:
            primitive = assoc.requestor.primitive
            called = _decode_aet(primitive.called_ae_title)
            calling = _decode_aet(primitive.calling_ae_title)
            peer_ip = str(assoc.requestor.address or "")
            rejection, unit_id = self._admit(assoc, port, called, calling, peer_ip)
        except Exception as exc:
            # pynetdicom logs and swallows errors of this handler, then
            # accepts the association: refuse it here instead.
            log_event(
                log,
                logging.ERROR,
                "dicom.receive.association",
                resource=f"port:{port}",
                status="failure",
                error=exc,
                bind_port=port,
            )
            rejection = _REJECT_ROUTING_ERROR
        if rejection is None:
            return
        # Logged first: the sender may see the rejection before kill() returns.
        log_event(
            log,
            logging.ERROR if rejection is _REJECT_AMBIGUOUS else logging.WARNING,
            "dicom.receive.association",
            resource=f"unit:{unit_id}" if unit_id else f"port:{port}",
            status="rejected",
            error_type=rejection.reason,
            unit_id=unit_id,
            bind_port=port,
            calling_aet=calling,
            called_aet=called,
            peer_ip=peer_ip,
        )
        assoc.acse.send_reject(rejection.result, rejection.source, rejection.diagnostic)
        # As pynetdicom does for its own rejections: kill() waits for the DUL
        # to flush the A-ASSOCIATE-RJ before the socket is closed.
        assoc.kill()

    def _admit(
        self, assoc, port: int, called: str, calling: str, peer_ip: str
    ) -> tuple[_Rejection | None, int | None]:
        """Pin the association to one unit, or say why it is refused."""
        with self._lock:
            table = self._table
            plan = table.plan
            unit_id = plan.lookup(port, called, calling)
            if unit_id is None:
                if plan.is_ambiguous(port, called, calling):
                    return _REJECT_AMBIGUOUS, None
                if plan.knows_listener(port, called):
                    return _REJECT_CALLING, None
                return _REJECT_CALLED, None
            route = table.routes.get(unit_id)
            if route is None:
                # Paused, or blocked by a routing problem already logged.
                return _REJECT_UNAVAILABLE, unit_id
            if not peer_ip_allowed(peer_ip, route.allowed_networks):
                return _REJECT_ADDRESS, unit_id
            open_assocs = self._associations[unit_id]
            for other in list(open_assocs):
                if not other.is_alive():
                    open_assocs.discard(other)
            if len(open_assocs) >= RECEIVER_MAX_ASSOCIATIONS:
                return _REJECT_LIMIT, unit_id
            setattr(
                assoc,
                _STATE_ATTR,
                _AssociationState(route, calling, peer_ip, monotonic()),
            )
            open_assocs.add(assoc)
        return None, unit_id

    def _on_finished(self, event, outcome: str) -> None:
        state = _state(event.assoc)
        if state is None or state.finished:
            return
        state.finished = True
        with self._lock:
            self._associations[state.route.unit_id].discard(event.assoc)
        # The PACS already has its answers; waiting here only makes the summary
        # include the outcome of the early acknowledgements.
        pending = state.wait_early_acks(
            0 if self._stopping else RECEIVER_COMMIT_TIMEOUT_SECONDS
        )
        if not (
            state.stored
            + state.duplicate
            + state.conflict
            + state.failed
            + state.unrecorded
            + pending
        ):
            return
        log_event(
            log,
            logging.WARNING
            if state.failed or state.conflict or state.unrecorded or pending
            else logging.INFO,
            "dicom.receive.association",
            resource=f"unit:{state.route.unit_id}",
            status=outcome,
            unit_id=state.route.unit_id,
            calling_aet=state.calling_aet,
            peer_ip=state.peer_ip,
            stored_count=state.stored,
            duplicate_count=state.duplicate,
            conflict_count=state.conflict,
            failed_count=state.failed,
            unrecorded_count=state.unrecorded,
            pending_count=pending,
            duration_ms=round((monotonic() - state.started_at) * 1000, 2),
        )

    @staticmethod
    def _on_echo(_event) -> int:
        return STATUS_SUCCESS

    # -- C-STORE ---------------------------------------------------------

    def _on_store(self, event) -> int:
        state = _state(event.assoc)
        if state is None:
            # Accepted without a route (cannot happen unless the admission
            # path breaks): never guess a unit, let the sender retry.
            log_event(
                log,
                logging.ERROR,
                "dicom.receive.store",
                status="refused",
                error_type="MissingStoreRoute",
            )
            return STATUS_OUT_OF_RESOURCES
        route = state.route
        calling = state.calling_aet
        peer_ip = state.peer_ip
        if self._table.routes.get(route.unit_id) != route:
            # The unit was paused or changed after this association began;
            # the reconcile aborts it, and the sender sends again later.
            state.count("failed")
            log_event(
                log,
                logging.WARNING,
                "dicom.receive.store",
                resource=f"unit:{route.unit_id}",
                status="refused",
                error_type="StoreRouteChanged",
                unit_id=route.unit_id,
                calling_aet=calling,
            )
            return STATUS_OUT_OF_RESOURCES
        try:
            transfer_syntax = UID(event.context.transfer_syntax)
            raw = event.encoded_dataset(include_meta=False)
            header = read_header(raw, transfer_syntax, event)
            sop_uid = str(header.get("SOPInstanceUID", "") or "").strip()
            study_uid = str(header.get("StudyInstanceUID", "") or "").strip()
            if not sop_uid or not study_uid or max(len(sop_uid), len(study_uid)) > 64:
                state.count("failed")
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
                state.count("failed")
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
            try:
                known = self._recorded_state(obj)
            except Exception as exc:
                # The database cannot be read: nothing is written and nothing
                # is acknowledged, the sender retries.
                state.count("failed")
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.receive.persist",
                    resource=f"unit:{route.unit_id}",
                    status="failure",
                    error=exc,
                    unit_id=route.unit_id,
                    calling_aet=calling,
                )
                return STATUS_OUT_OF_RESOURCES
            if known == KNOWN_DUPLICATE:
                state.count("duplicate")
                self._note_duplicate(state, route.unit_id, study_uid)
                return STATUS_SUCCESS
            # Decided before the write: the queue only grows by this object.
            early = (
                RECEIVER_EARLY_ACK
                and known == NEVER_RECORDED
                and self.writer.accepts_early_ack()
            )
            if not path.exists():
                write_part10(
                    path, build_file_meta(header, transfer_syntax, calling), raw
                )
            future = self.writer.submit(obj)
            if early:
                # The file is on disk under its final name; if the row cannot
                # be committed, the worker adopts the file.
                state.early_ack_started()
                future.add_done_callback(
                    lambda done: self._finish_early_ack(state, path, done)
                )
                return STATUS_SUCCESS
            try:
                decision = future.result(timeout=RECEIVER_COMMIT_TIMEOUT_SECONDS)
            except Exception as exc:
                # Not acknowledged: the sender retries. The file stays; the
                # worker adopts it (or drops it as a duplicate) later.
                state.count("failed")
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.receive.persist",
                    resource=f"unit:{route.unit_id}",
                    status="failure",
                    error=exc,
                    unit_id=route.unit_id,
                    calling_aet=calling,
                )
                return STATUS_OUT_OF_RESOURCES
            apply_decision(decision, path)
            self._count_decision(state, decision)
            return STATUS_SUCCESS
        except Exception as exc:
            state.count("failed")
            log_event(
                log,
                logging.ERROR,
                "dicom.receive.store",
                resource=f"unit:{route.unit_id}",
                status="failure",
                error=exc,
                unit_id=route.unit_id,
                calling_aet=calling,
            )
            return STATUS_PROCESSING_FAILURE

    def _finish_early_ack(
        self, state: _AssociationState, path: Path, future: Future
    ) -> None:
        """Outcome of an acknowledged object's commit, on the writer thread."""
        route = state.route
        try:
            try:
                decision = future.result()
            except Exception as exc:
                # Already acknowledged, so the sender will not retry: the file
                # stays in the receive folder and the worker adopts it.
                state.count("unrecorded")
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.receive.persist",
                    resource=f"unit:{route.unit_id}",
                    status="failure",
                    error=exc,
                    unit_id=route.unit_id,
                    calling_aet=state.calling_aet,
                    acknowledged=True,
                    path_name=path.name,
                )
                return
            try:
                apply_decision(decision, path)
            except Exception as exc:
                # The row is committed; the worker reconciles the file with it.
                state.count("failed")
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.receive.store",
                    resource=f"instance:{decision.instance_id}",
                    status="failure",
                    error=exc,
                    unit_id=route.unit_id,
                    instance_id=decision.instance_id,
                    acknowledged=True,
                )
                return
            self._count_decision(state, decision)
        except Exception as exc:
            log_event(
                log,
                logging.ERROR,
                "dicom.receive.persist",
                resource=f"unit:{route.unit_id}",
                status="failure",
                error=exc,
                unit_id=route.unit_id,
                acknowledged=True,
            )
        finally:
            state.early_ack_ended()

    @staticmethod
    def _count_decision(state: _AssociationState, decision: Decision) -> None:
        if decision.outcome == "conflict":
            state.count("conflict")
            log_event(
                log,
                logging.WARNING,
                "dicom.receive.conflict",
                resource=f"instance:{decision.instance_id}",
                status="quarantined",
                unit_id=state.route.unit_id,
                instance_id=decision.instance_id,
                calling_aet=state.calling_aet,
            )
        elif decision.outcome == "duplicate":
            state.count("duplicate")
        else:
            state.count("stored")

    def _note_duplicate(
        self, state: _AssociationState, unit_id: int, study_uid: str
    ) -> None:
        """Refresh the study's last_received_at for a duplicate.

        A duplicate is acknowledged without writing anything, but the worker
        reads that timestamp to confirm that a C-MOVE reached this receiver,
        also when it only brought images already held. Each association
        writes on its first duplicate of a study, then every few seconds.
        """
        now = monotonic()
        last = state.duplicate_touched.get(study_uid)
        if last is not None and now - last < DUPLICATE_TOUCH_SECONDS:
            return
        state.duplicate_touched[study_uid] = now
        try:
            with self._session_factory() as db:
                db.execute(
                    update(DicomStudy)
                    .where(
                        DicomStudy.unit_id == unit_id,
                        DicomStudy.study_uid == study_uid,
                    )
                    .values(last_received_at=datetime.now())
                )
                db.commit()
        except Exception as exc:
            log_event(
                log,
                logging.WARNING,
                "dicom.receive.duplicate",
                resource=f"unit:{unit_id}",
                status="failure",
                error=exc,
                unit_id=unit_id,
            )

    def _recorded_state(self, obj: ReceivedObject) -> str:
        """Cheap read before writing.

        Handled content is a duplicate, acknowledged without writing. Only a
        SOP the unit never recorded may be acknowledged before its commit: a
        revival, a conflicting version or a lost source keeps waiting for it,
        so a concurrent cleanup of the old row can still refuse it with 0xA700.
        """
        with self._session_factory() as db:
            rows = db.execute(
                select(
                    DicomInstance.state,
                    DicomInstance.source_path,
                    DicomInstance.source_sha256,
                ).where(
                    DicomInstance.unit_id == obj.unit_id,
                    DicomInstance.sop_uid == obj.sop_uid,
                )
            ).all()
        if not rows:
            return NEVER_RECORDED
        same = next((row for row in rows if row.source_sha256 == obj.sha256), None)
        if same is not None:
            if same.state in DONE_STATES:
                return KNOWN_DUPLICATE
            if (same.state in ACTIVE_STATES or same.state == "conflict") and Path(
                same.source_path
            ).exists():
                return KNOWN_DUPLICATE
        return KNOWN_SOP

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


class UnitChangeListener:
    """Waits for the ``units_changed`` NOTIFY; the poll interval stays the cap.

    Notifications sent while the receiver reconciles queue on the listening
    connection, so none is lost. While the connection is down, the wait falls
    back to plain polling and reconnects on the next call.
    """

    def __init__(self, url: URL = DATABASE_URL) -> None:
        self._url = url
        self._connection: psycopg.Connection | None = None
        self._failing = False

    def _connect(self) -> psycopg.Connection:
        if self._connection is None or self._connection.closed:
            connection = psycopg.connect(
                host=self._url.host,
                port=self._url.port,
                dbname=self._url.database,
                user=self._url.username,
                password=self._url.password,
                autocommit=True,
                connect_timeout=5,
            )
            connection.execute(f"LISTEN {UNITS_CHANGED_CHANNEL}")
            self._connection = connection
        return self._connection

    def wait(self, stop: threading.Event, timeout: float) -> bool:
        """True when a unit changed; False after ``timeout`` or on stop."""
        deadline = monotonic() + timeout
        while not stop.is_set():
            remaining = deadline - monotonic()
            if remaining <= 0:
                return False
            # Short slices keep SIGTERM handling as prompt as stop.wait().
            slice_seconds = min(remaining, 1.0)
            try:
                connection = self._connect()
                for _notify in connection.notifies(timeout=slice_seconds, stop_after=1):
                    self._recovered()
                    return True
                self._recovered()
            except psycopg.Error as exc:
                self.close()
                if not self._failing:
                    self._failing = True
                    log_event(
                        log,
                        logging.WARNING,
                        "dicom.receive.listen",
                        status="failure",
                        error=exc,
                    )
                stop.wait(slice_seconds)
        return False

    def _recovered(self) -> None:
        if self._failing:
            self._failing = False
            log_event(log, logging.INFO, "dicom.receive.listen", status="success")

    def close(self) -> None:
        if self._connection is not None:
            with suppress(Exception):
                self._connection.close()
            self._connection = None


def main() -> None:
    configure_logging()
    init_db()
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_args: stop.set())
    signal.signal(signal.SIGTERM, lambda *_args: stop.set())
    receiver = Receiver()
    receiver.start()
    changes = UnitChangeListener()
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
                )
            # A unit saved in the web interface applies at once; the interval
            # still bounds how stale the routing table can get.
            changes.wait(stop, WORKER_INTERVAL_SECONDS)
    finally:
        changes.close()
        receiver.stop()
        RECEIVER_HEALTH_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
