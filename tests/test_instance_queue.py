import os
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import CTImageStorage, ExplicitVRLittleEndian, generate_uid
from sqlalchemy import select

from app.codec_worker import CodecTimeout
from app.instances import ReceivedObject, dataset_sha256, record_instance
from app.models import DicomInstance, ImageTransfer
from app.pipeline.compact import _receive_adopt_states, compact_unit
from tests.support import DatabaseTestCase, make_unit


def write_dicom(path: Path, *, sop_uid=None, sender="SRVPACS") -> str:
    file_meta = FileMetaDataset()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.MediaStorageSOPClassUID = CTImageStorage
    file_meta.MediaStorageSOPInstanceUID = sop_uid or generate_uid()
    file_meta.SourceApplicationEntityTitle = sender
    image = FileDataset(str(path), {}, file_meta=file_meta, preamble=b"\0" * 128)
    image.SOPClassUID = CTImageStorage
    image.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    image.StudyInstanceUID = "1.2.826.0.1.77"
    image.SeriesInstanceUID = "1.2.826.0.1.77.1"
    image.Modality = "CT"
    image.AccessionNumber = "ACC-77"
    image.PatientID = "30211738"
    image.PatientBirthDate = "19691027"
    image.save_as(path, enforce_file_format=True)
    return image.SOPInstanceUID


def age(path: Path, seconds: int = 3600) -> None:
    old = datetime.now().timestamp() - seconds
    os.utime(path, (old, old))


class InstanceQueueTest(DatabaseTestCase):
    def setUp(self):
        super().setUp()
        _receive_adopt_states.clear()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.receive = root / "receive"
        self.send = root / "send"
        self.error = root / "error"
        self.receive.mkdir()
        self.unit = make_unit(
            receive_dir=str(self.receive),
            send_dir=str(self.send),
            error_dir=str(self.error),
            compact_workers=1,
        )
        with self.Session() as db:
            db.add(self.unit)
            db.commit()

    def register(self, db, path: Path):
        digest, size = dataset_sha256(path)
        return record_instance(
            db,
            ReceivedObject(
                unit_id=self.unit.id,
                study_uid="1.2.826.0.1.77",
                series_uid="1.2.826.0.1.77.1",
                sop_uid=str(
                    pydicom.dcmread(path, stop_before_pixels=True).SOPInstanceUID
                ),
                sop_class_uid=str(CTImageStorage),
                transfer_syntax=str(ExplicitVRLittleEndian),
                modality="CT",
                sha256=digest,
                size=size,
                path=str(path),
                conflict_path=str(self.error / path.name),
            ),
        )

    def compact(self, db):
        with (
            patch(
                "app.pipeline.compact.compression_runtime_settings",
                return_value=(set(), {"*": "lossless"}),
            ),
            patch("app.pipeline.compact.load_rule_specs", return_value=()),
        ):
            return compact_unit(db, self.unit)

    def test_received_instance_is_compacted_and_linked_to_its_transfer(self):
        source = self.receive / "CT.received"
        write_dicom(source)
        with self.Session() as db:
            self.register(db, source)
            db.commit()
            self.compact(db)

            instance = db.scalar(select(DicomInstance))
            transfer = db.scalar(select(ImageTransfer))
            self.assertEqual(instance.state, "compacted")
            self.assertEqual(instance.transfer_id, transfer.id)
            self.assertIsNone(instance.claimed_at)
            self.assertEqual(transfer.status, "compressed")
            self.assertFalse(source.exists())
            self.assertTrue((self.send / "CT.received.dcm").is_file())

    def test_legacy_file_without_row_is_adopted_after_min_age(self):
        legacy = self.receive / "CT.1.2.3.legacy"
        write_dicom(legacy)
        with self.Session() as db:
            self.compact(db)
            self.assertIsNone(db.scalar(select(DicomInstance)))  # too recent

            age(legacy)
            _receive_adopt_states.clear()
            self.compact(db)
            instance = db.scalar(select(DicomInstance))
            self.assertEqual(instance.state, "compacted")
            self.assertEqual(instance.calling_aet, "SRVPACS")
            self.assertFalse(legacy.exists())

    def test_identical_copy_of_compacted_content_is_dropped(self):
        source = self.receive / "CT.original"
        write_dicom(source)
        late_copy = Path(self.tmp.name) / "late-copy"
        shutil.copyfile(source, late_copy)
        with self.Session() as db:
            self.register(db, source)
            db.commit()
            self.compact(db)

            # Same SOP and bytes under another name (e.g. a leftover copy).
            copy = self.receive / "CT.late-copy"
            shutil.move(str(late_copy), str(copy))
            age(copy)
            _receive_adopt_states.clear()
            self.compact(db)

            self.assertFalse(copy.exists())
            (instance,) = db.scalars(select(DicomInstance))
            self.assertEqual(instance.state, "compacted")

    def test_retry_errors_revives_the_failed_instance(self):
        source = self.receive / "CT.failing"
        write_dicom(source)
        with self.Session() as db:
            self.register(db, source)
            db.commit()
            with (
                patch("app.pipeline.compact._codec_pool.run", side_effect=CodecTimeout),
                patch(
                    "app.pipeline.compact.compression_runtime_settings",
                    return_value=(set(), {"*": "lossless"}),
                ),
                patch("app.pipeline.compact.load_rule_specs", return_value=()),
            ):
                compact_unit(db, self.unit)
            instance = db.scalar(select(DicomInstance))
            self.assertEqual(instance.state, "error")
            quarantined = self.error / source.name
            self.assertTrue(quarantined.exists())

            # "Reprocessar erros" moves the file back to the receive folder.
            shutil.move(str(quarantined), str(source))
            age(source)
            _receive_adopt_states.clear()
            self.compact(db)
            db.refresh(instance)
            self.assertEqual(instance.state, "compacted")
            self.assertEqual(len(list(db.scalars(select(DicomInstance)))), 1)

    def test_invalid_file_in_receive_folder_is_quarantined(self):
        junk = self.receive / "not-dicom"
        junk.write_bytes(b"garbage")
        age(junk)
        with self.Session() as db:
            self.compact(db)
            self.assertFalse(junk.exists())
            self.assertTrue((self.error / "not-dicom").exists())
            self.assertIsNone(db.scalar(select(DicomInstance)))

    def test_stale_claim_returns_to_queue_or_is_marked_missing(self):
        kept = self.receive / "CT.kept"
        lost = self.receive / "CT.lost"
        write_dicom(kept)
        write_dicom(lost)
        with self.Session() as db:
            self.register(db, kept)
            self.register(db, lost)
            for instance in db.scalars(select(DicomInstance)):
                instance.state = "compacting"
                instance.claimed_at = datetime.now() - timedelta(hours=2)
            db.commit()
            lost.unlink()

            self.compact(db)

            states = {
                Path(row.source_path).name: row.state
                for row in db.scalars(select(DicomInstance))
            }
            self.assertEqual(states, {"CT.kept": "compacted", "CT.lost": "missing"})

    def test_claimed_instance_whose_file_vanished_is_missing(self):
        source = self.receive / "CT.gone"
        write_dicom(source)
        with self.Session() as db:
            self.register(db, source)
            db.commit()
            source.unlink()
            self.assertFalse(self.compact(db))
            self.assertEqual(db.scalar(select(DicomInstance.state)), "missing")
