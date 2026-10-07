import hashlib
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pydicom
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.uid import (
    CTImageStorage,
    ExplicitVRLittleEndian,
    ImplicitVRLittleEndian,
    generate_uid,
)
from pynetdicom import AE
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from app.instances import record_instance
from app.models import DicomInstance, DicomStudy
from app.receiver import (
    STATUS_CANNOT_UNDERSTAND,
    STATUS_OUT_OF_RESOURCES,
    STATUS_SUCCESS,
    Receiver,
)
from tests.support import make_unit, postgres_test_engine


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def ct_image(
    *, sop_uid=None, study_uid="1.2.826.0.1.99.1", pixel=b"\x01\x02" * 256, **extra
):
    ds = Dataset()
    ds.file_meta = FileMetaDataset()
    ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds.SOPClassUID = CTImageStorage
    ds.SOPInstanceUID = sop_uid or generate_uid()
    if study_uid is not None:
        ds.StudyInstanceUID = study_uid
    ds.SeriesInstanceUID = "1.2.826.0.1.99.1.1"
    ds.Modality = "CT"
    ds.PatientID = "30211738"
    ds.PatientName = "PACIENTE^TESTE"
    ds.Rows = ds.Columns = 16
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 0
    ds.PixelData = pixel
    for keyword, value in extra.items():
        setattr(ds, keyword, value)
    return ds


def dataset_bytes(path: Path) -> bytes:
    """Bytes after the File Meta group: the dataset exactly as stored."""
    meta = pydicom.filereader.read_file_meta_info(path)
    return path.read_bytes()[132 + 12 + meta.FileMetaInformationGroupLength :]


class ReceiverTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.receive_dir = root / "receive"
        self.error_dir = root / "error"
        # The writer thread and handler threads use their own connections.
        self.engine = postgres_test_engine(self)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.port = free_port()
        self.unit = make_unit(
            calling_aet="RETRIEVE",
            store_port=self.port,
            receive_dir=str(self.receive_dir),
            error_dir=str(self.error_dir),
        )
        with self.Session() as db:
            db.add(self.unit)
            db.commit()
        self.receiver = Receiver(self.Session, host="127.0.0.1")
        self.receiver.start()
        self.receiver.reconcile([self.unit])
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        self.receiver.stop()
        self.engine.dispose()
        self.tmp.cleanup()

    def restart_with(self, **changes):
        for key, value in changes.items():
            setattr(self.unit, key, value)
        self.receiver.reconcile([self.unit])

    def send(self, *datasets, calling="SRVPACS", called="RETRIEVE", ts=None):
        ae = AE(ae_title=calling)
        ae.add_requested_context(CTImageStorage, [ts or ExplicitVRLittleEndian])
        assoc = ae.associate("127.0.0.1", self.port, ae_title=called)
        if not assoc.is_established:
            return assoc, []
        statuses = []
        for ds in datasets:
            if ts is not None:
                ds.file_meta.TransferSyntaxUID = ts
            status = assoc.send_c_store(ds)
            statuses.append(status.Status if "Status" in status else None)
        assoc.release()
        # New objects are acknowledged before their commit ends.
        self.wait_writer()
        return assoc, statuses

    def wait_writer(self):
        self.assertTrue(self.receiver.writer.wait_idle(10))

    def rows(self):
        self.wait_writer()
        with self.Session() as db:
            return list(db.scalars(select(DicomInstance).order_by(DicomInstance.id)))

    def test_store_writes_received_bytes_and_records_the_instance(self):
        image = ct_image()
        _assoc, statuses = self.send(image)

        self.assertEqual(statuses, [STATUS_SUCCESS])
        (row,) = self.rows()
        path = Path(row.source_path)
        self.assertEqual(path.parent, self.receive_dir)
        self.assertTrue(path.name.startswith(f"CT.{image.SOPInstanceUID}."))
        self.assertEqual(row.state, "received")
        self.assertEqual(row.calling_aet, "SRVPACS")
        self.assertEqual(row.sop_uid, image.SOPInstanceUID)
        stored = dataset_bytes(path)
        self.assertEqual(row.source_sha256, hashlib.sha256(stored).hexdigest())
        self.assertEqual(row.source_bytes, len(stored))
        saved = pydicom.dcmread(path)
        self.assertEqual(saved.file_meta.SourceApplicationEntityTitle, "SRVPACS")
        self.assertEqual(saved.PixelData, image.PixelData)
        self.assertEqual(str(saved.PatientName), "PACIENTE^TESTE")
        self.assertFalse(list(self.receive_dir.glob(".rx-*")))
        with self.Session() as db:
            study = db.scalar(select(DicomStudy))
            self.assertEqual(study.instance_count, 1)

    def test_implicit_vr_is_stored_raw_too(self):
        _assoc, statuses = self.send(ct_image(), ts=ImplicitVRLittleEndian)
        self.assertEqual(statuses, [STATUS_SUCCESS])
        (row,) = self.rows()
        self.assertEqual(row.transfer_syntax, str(ImplicitVRLittleEndian))
        saved = pydicom.dcmread(row.source_path)
        self.assertEqual(saved.file_meta.TransferSyntaxUID, ImplicitVRLittleEndian)

    def test_repeated_retrieve_of_same_content_is_a_duplicate(self):
        image = ct_image()
        self.send(image)
        _assoc, statuses = self.send(image)

        self.assertEqual(statuses, [STATUS_SUCCESS])
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(len(list(self.receive_dir.iterdir())), 1)

        # Once compacted the source is gone: a resend is acknowledged, not stored.
        (row,) = self.rows()
        Path(row.source_path).unlink()
        with self.Session() as db:
            db.get(DicomInstance, row.id).state = "compacted"
            db.commit()
        _assoc, statuses = self.send(image)
        self.assertEqual(statuses, [STATUS_SUCCESS])
        self.assertEqual(list(self.receive_dir.iterdir()), [])
        self.assertEqual(len(self.rows()), 1)

    def test_same_sop_with_different_content_is_kept_as_conflict(self):
        sop_uid = generate_uid()
        self.send(ct_image(sop_uid=sop_uid))
        _assoc, statuses = self.send(ct_image(sop_uid=sop_uid, pixel=b"\x09" * 512))

        self.assertEqual(statuses, [STATUS_SUCCESS])
        first, second = self.rows()
        self.assertEqual(first.state, "received")
        self.assertEqual(second.state, "conflict")
        self.assertEqual(Path(second.source_path).parent, self.error_dir)
        self.assertTrue(Path(first.source_path).exists())
        self.assertTrue(Path(second.source_path).exists())
        self.assertEqual(len(list(self.receive_dir.iterdir())), 1)

    def test_failed_instance_is_revived_when_sent_again(self):
        image = ct_image()
        self.send(image)
        (row,) = self.rows()
        Path(row.source_path).unlink()
        with self.Session() as db:
            db.get(DicomInstance, row.id).state = "error"
            db.commit()

        _assoc, statuses = self.send(image)

        self.assertEqual(statuses, [STATUS_SUCCESS])
        (row,) = self.rows()
        self.assertEqual(row.state, "received")
        self.assertTrue(Path(row.source_path).exists())

    def test_association_filters_called_and_calling_ae_case_insensitively(self):
        assoc, _ = self.send(ct_image(), called="OUTRO")
        self.assertTrue(assoc.is_rejected)

        self.restart_with(store_allowed_ips="10.20.0.0/16")
        assoc, _ = self.send(ct_image())
        self.assertTrue(assoc.is_rejected)
        echo = AE(ae_title="SRVPACS")
        echo.add_requested_context("1.2.840.10008.1.1")
        self.assertTrue(
            echo.associate("127.0.0.1", self.port, ae_title="RETRIEVE").is_rejected
        )
        self.restart_with(store_allowed_ips="10.20.0.0/16,127.0.0.1")
        _assoc, statuses = self.send(ct_image())
        self.assertEqual(statuses, [STATUS_SUCCESS])
        self.restart_with(store_allowed_ips="")

        self.restart_with(store_allowed_aets="SRVPACS")
        assoc, _ = self.send(ct_image(), calling="INTRUSO")
        self.assertTrue(assoc.is_rejected)
        _assoc, statuses = self.send(ct_image(), calling="srvpacs", called="retrieve")
        self.assertEqual(statuses, [STATUS_SUCCESS])
        self.assertEqual(len(self.rows()), 2)

    def test_new_object_is_acknowledged_before_its_commit(self):
        image = ct_image()
        commit_allowed = threading.Event()

        def held_record(db, obj):
            commit_allowed.wait(10)
            return record_instance(db, obj)

        ae = AE(ae_title="SRVPACS")
        ae.add_requested_context(CTImageStorage, [ExplicitVRLittleEndian])
        with patch("app.receiver.record_instance", side_effect=held_record):
            assoc = ae.associate("127.0.0.1", self.port, ae_title="RETRIEVE")
            self.assertTrue(assoc.is_established)
            status = assoc.send_c_store(image)

            # Acknowledged while its commit is still held: the file is final.
            self.assertEqual(status.Status, STATUS_SUCCESS)
            with self.Session() as db:
                self.assertEqual(
                    db.scalar(select(func.count()).select_from(DicomInstance)), 0
                )
            (path,) = self.receive_dir.iterdir()
            self.assertEqual(pydicom.dcmread(path).SOPInstanceUID, image.SOPInstanceUID)

            commit_allowed.set()
            assoc.release()
            self.wait_writer()
        (row,) = self.rows()
        self.assertEqual(row.state, "received")
        self.assertEqual(Path(row.source_path), path)
        self.assertEqual(
            row.source_sha256, hashlib.sha256(dataset_bytes(path)).hexdigest()
        )

    def test_record_failure_after_the_ack_leaves_the_file_for_adoption(self):
        from app.pipeline.compact import _adopt_receive_files

        first, second, third = ct_image(), ct_image(), ct_image()
        with patch("app.receiver.record_instance", side_effect=RuntimeError("down")):
            with self.assertLogs("receiver", "ERROR"):
                _assoc, statuses = self.send(first)
            # Already acknowledged when its commit failed.
            self.assertEqual(statuses, [STATUS_SUCCESS])
            self.assertFalse(self.receiver.writer.accepts_early_ack())

            # While the writer fails, new objects wait for their commit again.
            _assoc, statuses = self.send(second)
            self.assertEqual(statuses, [STATUS_OUT_OF_RESOURCES])
        self.assertEqual(self.rows(), [])
        self.assertEqual(len(list(self.receive_dir.iterdir())), 2)

        # The next successful commit lifts the fallback.
        _assoc, statuses = self.send(third)
        self.assertEqual(statuses, [STATUS_SUCCESS])
        self.assertTrue(self.receiver.writer.accepts_early_ack())

        # The worker adopts the files that have no row.
        with (
            self.Session() as db,
            patch("app.pipeline.compact.RECEIVE_ADOPT_MIN_AGE_SECONDS", 0),
        ):
            _adopt_receive_files(db, self.unit, self.receive_dir, self.error_dir)
        rows = {row.sop_uid: row for row in self.rows()}
        self.assertEqual(
            set(rows),
            {first.SOPInstanceUID, second.SOPInstanceUID, third.SOPInstanceUID},
        )
        adopted = rows[first.SOPInstanceUID]
        self.assertEqual(adopted.state, "received")
        self.assertEqual(
            adopted.source_sha256,
            hashlib.sha256(dataset_bytes(Path(adopted.source_path))).hexdigest(),
        )

    def test_unreadable_database_is_refused_before_any_write(self):
        with patch.object(
            Receiver, "_recorded_state", side_effect=RuntimeError("down")
        ):
            _assoc, statuses = self.send(ct_image())
        self.assertEqual(statuses, [STATUS_OUT_OF_RESOURCES])
        self.assertFalse(self.receive_dir.exists() and any(self.receive_dir.iterdir()))

    def test_revival_still_waits_for_its_commit(self):
        image = ct_image()
        self.send(image)
        (row,) = self.rows()
        Path(row.source_path).unlink()
        with self.Session() as db:
            db.get(DicomInstance, row.id).state = "error"
            db.commit()

        with patch("app.receiver.record_instance", side_effect=RuntimeError("down")):
            _assoc, statuses = self.send(image)

        self.assertEqual(statuses, [STATUS_OUT_OF_RESOURCES])
        (row,) = self.rows()
        self.assertEqual(row.state, "error")

    def test_new_object_waits_for_its_commit_when_early_ack_is_off_or_queue_long(self):
        for setting, value in (
            ("RECEIVER_EARLY_ACK", False),
            ("RECEIVER_EARLY_ACK_MAX_PENDING", 0),
        ):
            with (
                self.subTest(setting),
                patch(f"app.receiver.{setting}", value),
                patch("app.receiver.record_instance", side_effect=RuntimeError("down")),
            ):
                _assoc, statuses = self.send(ct_image())
                self.assertEqual(statuses, [STATUS_OUT_OF_RESOURCES])
            # A successful commit clears the writer's failure for the next case.
            _assoc, statuses = self.send(ct_image())
            self.assertEqual(statuses, [STATUS_SUCCESS])

    def test_association_summary_counts_early_acknowledgements(self):
        with self.assertLogs("receiver", "INFO") as logs:
            self.send(*[ct_image() for _ in range(3)])
            # The summary is logged by the receiver once the release ends.
            deadline = time.monotonic() + 10
            summaries = []
            while not summaries and time.monotonic() < deadline:
                summaries = [
                    record
                    for record in logs.records
                    if record.getMessage() == "dicom.receive.association"
                ]
                if not summaries:
                    time.sleep(0.05)
        (summary,) = summaries
        self.assertEqual(
            (summary.stored_count, summary.unrecorded_count, summary.pending_count),
            (3, 0, 0),
        )

    def test_low_disk_space_is_refused_for_retry(self):
        with patch(
            "app.receiver.shutil.disk_usage",
            return_value=SimpleNamespace(free=0, total=1, used=1),
        ):
            _assoc, statuses = self.send(ct_image())
        self.assertEqual(statuses, [STATUS_OUT_OF_RESOURCES])
        self.assertFalse(self.receive_dir.exists() and any(self.receive_dir.iterdir()))

    def test_object_without_study_uid_is_refused(self):
        _assoc, statuses = self.send(ct_image(study_uid=None))
        self.assertEqual(statuses, [STATUS_CANNOT_UNDERSTAND])
        self.assertEqual(self.rows(), [])

    def test_received_image_flows_to_compaction_and_a_repeated_move_is_skipped(self):
        from app.models import ImageTransfer
        from app.pipeline.compact import compact_unit

        image = ct_image(
            AccessionNumber="ACC-1",
            PatientBirthDate="19691027",
        )
        send_dir = Path(self.tmp.name) / "send"
        self.unit.send_dir = str(send_dir)
        self.send(image)

        with (
            self.Session() as db,
            patch(
                "app.pipeline.compact.compression_runtime_settings",
                return_value=(set(), {"*": "lossless"}),
            ),
            patch("app.pipeline.compact.load_rule_specs", return_value=()),
        ):
            compact_unit(db, self.unit)
            (row,) = self.rows()
            self.assertEqual(row.state, "compacted")
            transfer = db.get(ImageTransfer, row.transfer_id)
            self.assertEqual(transfer.status, "compressed")
            self.assertTrue((send_dir / transfer.filename).is_file())

            # Second C-MOVE of the same study: acknowledged, nothing to compact.
            _assoc, statuses = self.send(image)
            self.assertEqual(statuses, [STATUS_SUCCESS])
            self.assertEqual(list(self.receive_dir.iterdir()), [])
            self.assertFalse(compact_unit(db, self.unit))

    def test_many_images_in_one_association_are_all_committed(self):
        images = [ct_image() for _ in range(25)]
        _assoc, statuses = self.send(*images)
        self.assertEqual(statuses, [STATUS_SUCCESS] * 25)
        with self.Session() as db:
            self.assertEqual(
                db.scalar(select(func.count()).select_from(DicomInstance)), 25
            )


if __name__ == "__main__":
    unittest.main()
