"""Store SCP with units sharing one port and called AE title.

The sender's AE title picks the unit; nothing may ever land in another
unit's folder or under another unit's id, whatever the timing.
"""

import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from pydicom.uid import CTImageStorage, ExplicitVRLittleEndian, generate_uid
from pynetdicom import AE
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from app.config import DATABASE_URL
from app.db import ensure_units_changed_trigger
from app.models import DicomInstance
from app.receiver import (
    STATUS_OUT_OF_RESOURCES,
    STATUS_SUCCESS,
    Receiver,
    UnitChangeListener,
)
from tests.support import DatabaseTestCase, make_unit, postgres_test_engine
from tests.test_receiver import ct_image, free_port

CALLED = "MOBILEMED"


def wait_until(condition, timeout=5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return condition()


class SharedPortReceiverTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.engine = postgres_test_engine(self)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.port = free_port()
        self.a = self._unit("Empresa1", "a", "SERVERPACS1")
        self.b = self._unit("Empresa2", "b", "SERVERPACS2")
        with self.Session() as db:
            db.add_all([self.a, self.b])
            db.commit()
        self.units = [self.a, self.b]
        self.receiver = Receiver(self.Session, host="127.0.0.1")
        self.receiver.start()
        self.receiver.reconcile(self.units)
        self.addCleanup(self._cleanup)

    def _unit(self, name, folder, sender, **overrides):
        values = {
            "name": name,
            "calling_aet": CALLED,
            "store_port": self.port,
            "store_allowed_aets": sender,
            "receive_dir": str(self.root / folder / "receive"),
            "send_dir": str(self.root / folder / "send"),
            "error_dir": str(self.root / folder / "error"),
        }
        values.update(overrides)
        return make_unit(**values)

    def _cleanup(self):
        self.receiver.stop()
        self.engine.dispose()
        self.tmp.cleanup()

    def reconcile(self):
        self.receiver.reconcile(self.units)

    def associate(self, calling, called=CALLED):
        ae = AE(ae_title=calling)
        ae.add_requested_context(CTImageStorage, [ExplicitVRLittleEndian])
        return ae.associate("127.0.0.1", self.port, ae_title=called)

    def send(self, calling, *datasets, called=CALLED):
        assoc = self.associate(calling, called)
        if not assoc.is_established:
            return assoc, []
        statuses = [self.store(assoc, ds) for ds in datasets]
        assoc.release()
        self.wait_writer()
        return assoc, statuses

    def wait_writer(self):
        # New objects are acknowledged before their commit ends.
        self.assertTrue(self.receiver.writer.wait_idle(10))

    @staticmethod
    def store(assoc, ds):
        status = assoc.send_c_store(ds)
        return status.Status if "Status" in status else None

    def rows(self):
        self.wait_writer()
        with self.Session() as db:
            return list(db.scalars(select(DicomInstance).order_by(DicomInstance.id)))

    def files(self, folder):
        self.wait_writer()
        directory = self.root / folder / "receive"
        if not directory.is_dir():
            return []
        return [path for path in directory.iterdir() if path.is_file()]

    def assert_rows_belong_to_their_sender(self):
        owner = {"SERVERPACS1": self.a, "SERVERPACS2": self.b}
        for row in self.rows():
            unit = owner[row.calling_aet]
            self.assertEqual(row.unit_id, unit.id)
            self.assertEqual(Path(row.source_path).parent, Path(unit.receive_dir))

    # -- routing ---------------------------------------------------------

    def test_each_sender_lands_in_its_own_unit(self):
        first, second = ct_image(), ct_image()

        _assoc, statuses_a = self.send("SERVERPACS1", first)
        _assoc, statuses_b = self.send("serverpacs2", second)

        self.assertEqual(statuses_a + statuses_b, [STATUS_SUCCESS, STATUS_SUCCESS])
        self.assertEqual(self.receiver.listening_ports(), {self.port})
        rows = {row.sop_uid: row for row in self.rows()}
        self.assertEqual(rows[first.SOPInstanceUID].unit_id, self.a.id)
        self.assertEqual(rows[second.SOPInstanceUID].unit_id, self.b.id)
        self.assertEqual(len(self.files("a")), 1)
        self.assertEqual(len(self.files("b")), 1)

    def test_unknown_sender_or_called_aet_is_rejected_before_any_image(self):
        with self.assertLogs("receiver", "WARNING") as logs:
            unknown_sender, _ = self.send("OUTROPACS", ct_image())
            unknown_called, _ = self.send("SERVERPACS1", ct_image(), called="OUTRO")

        self.assertTrue(unknown_sender.is_rejected)
        self.assertTrue(unknown_called.is_rejected)
        reasons = {getattr(record, "error_type", None) for record in logs.records}
        self.assertIn("CallingAETitleNotRecognized", reasons)
        self.assertIn("CalledAETitleNotRecognized", reasons)
        self.assertEqual(self.rows(), [])

    def add_third_unit(self):
        third = self._unit("Empresa3", "c", "SERVERPACS3")
        with self.Session() as db:
            db.add(third)
            db.commit()
        self.units.append(third)
        return third

    def test_ambiguous_database_state_is_refused_never_guessed(self):
        third = self.add_third_unit()
        # Bypasses the form validation, as a manual SQL change would.
        self.b.store_allowed_aets = "SERVERPACS2,SERVERPACS1"
        with self.assertLogs("receiver", "WARNING") as logs:
            self.reconcile()
            ambiguous, _ = self.send("SERVERPACS1", ct_image())
            blocked, _ = self.send("SERVERPACS2", ct_image())
        _assoc, statuses = self.send("SERVERPACS3", ct_image())

        self.assertTrue(ambiguous.is_rejected)
        self.assertTrue(blocked.is_rejected)
        reasons = {getattr(record, "error_type", None) for record in logs.records}
        self.assertTrue(
            {"StoreRouteConflict", "AmbiguousStoreRoute", "UnitUnavailable"} <= reasons
        )
        self.assertEqual(statuses, [STATUS_SUCCESS])
        (row,) = self.rows()
        self.assertEqual(row.unit_id, third.id)

    def test_every_unit_of_the_port_blocked_closes_the_port(self):
        self.b.store_allowed_aets = "SERVERPACS2,SERVERPACS1"
        with self.assertLogs("receiver", "ERROR"):
            self.reconcile()
        assoc, _ = self.send("SERVERPACS1", ct_image())

        self.assertFalse(assoc.is_established)
        self.assertEqual(self.receiver.listening_ports(), set())
        self.assertEqual(self.rows(), [])

    def test_shared_folders_block_the_units_involved(self):
        self.add_third_unit()
        self.b.receive_dir = self.a.receive_dir
        with self.assertLogs("receiver", "WARNING"):
            self.reconcile()
            assoc_a, _ = self.send("SERVERPACS1", ct_image())
            assoc_b, _ = self.send("SERVERPACS2", ct_image())

        self.assertTrue(assoc_a.is_rejected)
        self.assertTrue(assoc_b.is_rejected)
        self.assertEqual(self.rows(), [])

    def test_ip_filter_applies_to_the_chosen_unit_only(self):
        self.a.store_allowed_ips = "10.20.0.0/16"
        self.reconcile()
        with self.assertLogs("receiver", "WARNING"):
            refused, _ = self.send("SERVERPACS1", ct_image())
        _assoc, statuses = self.send("SERVERPACS2", ct_image())

        self.assertTrue(refused.is_rejected)
        self.assertEqual(statuses, [STATUS_SUCCESS])

    def test_routing_error_rejects_instead_of_accepting_unrouted(self):
        with (
            patch.object(self.receiver, "_admit", side_effect=RuntimeError("boom")),
            self.assertLogs("receiver", "ERROR"),
        ):
            assoc, _ = self.send("SERVERPACS1", ct_image())

        self.assertTrue(assoc.is_rejected)
        self.assertEqual(self.rows(), [])

    def test_store_without_a_pinned_route_is_refused(self):
        # An admission that returned without pinning a route.
        with (
            patch.object(self.receiver, "_admit", return_value=(None, self.a.id)),
            self.assertLogs("receiver", "ERROR"),
        ):
            _assoc, statuses = self.send("SERVERPACS1", ct_image())

        self.assertEqual(statuses, [STATUS_OUT_OF_RESOURCES])
        self.assertEqual(self.rows(), [])

    # -- configuration changes while associations are open ----------------

    def test_editing_one_unit_keeps_the_other_units_association(self):
        listener = self.receiver._listeners[self.port]
        assoc = self.associate("SERVERPACS2")
        self.assertTrue(assoc.is_established)

        self.a.store_allowed_aets = "SERVERPACS1,NOVOPACS"
        self.a.receive_dir = str(self.root / "a" / "receive2")
        self.reconcile()

        self.assertEqual(self.store(assoc, ct_image()), STATUS_SUCCESS)
        assoc.release()
        self.assertIs(self.receiver._listeners[self.port], listener)
        (row,) = self.rows()
        self.assertEqual(row.unit_id, self.b.id)

    def test_pausing_a_unit_aborts_its_open_association(self):
        assoc_a = self.associate("SERVERPACS1")
        assoc_b = self.associate("SERVERPACS2")
        self.assertTrue(assoc_a.is_established and assoc_b.is_established)

        self.a.enabled = False
        with self.assertLogs("receiver", "WARNING") as logs:
            self.reconcile()
            self.assertTrue(wait_until(lambda: not assoc_a.is_established))
            refused, _ = self.send("SERVERPACS1", ct_image())

        self.assertIn(
            "StoreRouteChanged",
            {getattr(record, "error_type", None) for record in logs.records},
        )
        self.assertTrue(refused.is_rejected)
        self.assertEqual(self.store(assoc_b, ct_image()), STATUS_SUCCESS)
        assoc_b.release()
        (row,) = self.rows()
        self.assertEqual(row.unit_id, self.b.id)

    def test_store_on_a_route_that_changed_is_refused_for_retry(self):
        assoc = self.associate("SERVERPACS1")
        self.assertTrue(assoc.is_established)
        old_folder = Path(self.a.receive_dir)

        # The abort has not reached this association yet.
        with patch.object(self.receiver, "_abort_stale"):
            self.a.receive_dir = str(self.root / "a" / "moved")
            self.reconcile()
        with self.assertLogs("receiver", "WARNING"):
            status = self.store(assoc, ct_image())
        assoc.release()

        self.assertEqual(status, STATUS_OUT_OF_RESOURCES)
        self.assertEqual(self.rows(), [])
        self.assertFalse(old_folder.exists() and any(old_folder.iterdir()))

    def test_one_unit_cannot_use_up_the_other_units_associations(self):
        with patch("app.receiver.RECEIVER_MAX_ASSOCIATIONS", 1):
            self.reconcile()
            first = self.associate("SERVERPACS1")
            with self.assertLogs("receiver", "WARNING") as logs:
                second = self.associate("SERVERPACS1")
            other = self.associate("SERVERPACS2")
            try:
                self.assertTrue(first.is_established)
                self.assertTrue(second.is_rejected)
                self.assertTrue(other.is_established)
                self.assertIn(
                    "UnitAssociationLimit",
                    {getattr(record, "error_type", None) for record in logs.records},
                )
            finally:
                for assoc in (first, other):
                    if assoc.is_established:
                        assoc.release()

        # A released association frees its slot.
        self.assertTrue(wait_until(lambda: self.associate_and_release("SERVERPACS1")))

    def associate_and_release(self, calling):
        assoc = self.associate(calling)
        established = assoc.is_established
        if established:
            assoc.release()
        return established

    def test_last_unit_leaving_the_port_closes_the_listener(self):
        self.a.enabled = False
        self.b.enabled = False
        self.reconcile()

        self.assertEqual(self.receiver.listening_ports(), set())

    # -- concurrency -----------------------------------------------------

    def test_concurrent_senders_and_reconciles_never_cross_units(self):
        third = self.add_third_unit()
        self.reconcile()

        stop = threading.Event()
        errors: list[BaseException] = []
        statuses: dict[str, list] = {"SERVERPACS1": [], "SERVERPACS2": []}

        def flip_unrelated_unit():
            while not stop.is_set():
                third.enabled = not third.enabled
                third.store_allowed_ips = "" if third.enabled else "10.0.0.0/8"
                self.reconcile()
                time.sleep(0.005)

        def sender(calling):
            try:
                for _ in range(4):
                    images = [ct_image(sop_uid=generate_uid()) for _ in range(3)]
                    _assoc, result = self.send(calling, *images)
                    statuses[calling].extend(result)
            except BaseException as exc:  # surfaced below
                errors.append(exc)

        flipper = threading.Thread(target=flip_unrelated_unit)
        senders = [
            threading.Thread(target=sender, args=(calling,))
            for calling in ("SERVERPACS1", "SERVERPACS2")
            for _ in range(3)
        ]
        flipper.start()
        for thread in senders:
            thread.start()
        for thread in senders:
            thread.join(timeout=120)
        stop.set()
        flipper.join(timeout=10)

        self.assertEqual(errors, [])
        for calling, result in statuses.items():
            self.assertEqual(result, [STATUS_SUCCESS] * 36, calling)
        self.assertEqual(len(self.rows()), 72)
        self.assert_rows_belong_to_their_sender()
        self.assertEqual(len(self.files("a")), 36)
        self.assertEqual(len(self.files("b")), 36)
        self.assertEqual(self.files("c"), [])


if __name__ == "__main__":
    unittest.main()


class UnitChangeListenerTest(DatabaseTestCase):
    def setUp(self):
        super().setUp()
        ensure_units_changed_trigger(self.engine)
        self.listener = UnitChangeListener()
        self.addCleanup(self.listener.close)
        self.stop = threading.Event()

    def test_saving_a_unit_wakes_the_receiver_at_once(self):
        # The first call connects and starts listening.
        self.assertFalse(self.listener.wait(self.stop, 0.1))
        with self.Session() as db:
            db.add(make_unit(name="Empresa1"))
            db.commit()

        started = time.monotonic()
        self.assertTrue(self.listener.wait(self.stop, 10))
        self.assertLess(time.monotonic() - started, 2)
        # Nothing else changed: back to waiting for the interval.
        self.assertFalse(self.listener.wait(self.stop, 0.3))

    def test_stop_ends_the_wait(self):
        self.stop.set()
        started = time.monotonic()

        self.assertFalse(self.listener.wait(self.stop, 10))
        self.assertLess(time.monotonic() - started, 1)

    def test_database_down_falls_back_to_polling(self):
        listener = UnitChangeListener(DATABASE_URL.set(port=free_port()))
        started = time.monotonic()

        with self.assertLogs("receiver", "WARNING") as logs:
            self.assertFalse(listener.wait(self.stop, 1.5))

        self.assertGreaterEqual(time.monotonic() - started, 1.4)
        # Logged once, not on every retry.
        self.assertEqual(len(logs.records), 1)
