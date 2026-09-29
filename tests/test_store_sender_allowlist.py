import tempfile
import unittest
from pathlib import Path

from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import (
    ExplicitVRLittleEndian,
    SecondaryCaptureImageStorage,
    generate_uid,
)
from sqlalchemy import func, select

from app.compaction import CompactResult
from app.models import ImageTransfer, Order
from app.pipeline.compact import _compact_one
from app.pipeline.records import _record_compact_result
from app.validation import (
    peer_ip_allowed,
    store_allowed_networks,
    store_allowed_senders,
    validate_store_allowed_aets,
    validate_store_allowed_ips,
)
from tests.support import DatabaseTestCase, make_unit


def write_received_dicom(path: Path, sender: str | None) -> None:
    """Write an object the way storescp stores it, with the calling AE in meta."""
    file_meta = FileMetaDataset()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.MediaStorageSOPClassUID = SecondaryCaptureImageStorage
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    if sender is not None:
        file_meta.SourceApplicationEntityTitle = sender
    image = FileDataset(str(path), {}, file_meta=file_meta, preamble=b"\0" * 128)
    image.SOPClassUID = file_meta.MediaStorageSOPClassUID
    image.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    image.StudyInstanceUID = generate_uid()
    image.AccessionNumber = "ACC-1"
    image.PatientID = "30211738"
    image.PatientBirthDate = "19691027"
    image.Modality = "CT"
    image.save_as(path, enforce_file_format=True)


class StoreIpAllowlistTest(unittest.TestCase):
    def test_empty_list_accepts_every_address(self):
        self.assertEqual(validate_store_allowed_ips("  "), "")
        self.assertTrue(peer_ip_allowed("203.0.113.9", store_allowed_networks("")))

    def test_addresses_and_ranges_are_normalized(self):
        stored = validate_store_allowed_ips(
            " 192.168.3.103, 192.168.3.103;10.10.0.7/24\n192.168.0.11/32 "
        )
        self.assertEqual(stored, "192.168.3.103,10.10.0.0/24,192.168.0.11")

    def test_membership_including_ipv4_mapped_ipv6(self):
        networks = store_allowed_networks("192.168.3.103,10.10.0.0/24")
        self.assertTrue(peer_ip_allowed("192.168.3.103", networks))
        self.assertTrue(peer_ip_allowed("10.10.0.250", networks))
        self.assertTrue(peer_ip_allowed("::ffff:10.10.0.4", networks))
        self.assertFalse(peer_ip_allowed("192.168.3.104", networks))
        self.assertFalse(peer_ip_allowed("not-an-ip", networks))

    def test_invalid_entries_are_rejected(self):
        for value in ("192.168.3.300", "pacs.local", "10.0.0.0/33"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_store_allowed_ips(value)
        with self.assertRaises(ValueError):
            validate_store_allowed_ips(",".join(f"10.0.0.{i}" for i in range(65)))


class StoreAllowlistValidationTest(unittest.TestCase):
    def test_empty_list_accepts_everyone(self):
        self.assertEqual(validate_store_allowed_aets("  "), "")
        self.assertEqual(store_allowed_senders(""), frozenset())

    def test_normalizes_separators_and_deduplicates_case_insensitively(self):
        stored = validate_store_allowed_aets(" srvPACS, SRVPACS;MOD 1\nOTHER ")
        self.assertEqual(stored, "srvPACS,MOD 1,OTHER")
        self.assertEqual(
            store_allowed_senders(stored), frozenset({"SRVPACS", "MOD 1", "OTHER"})
        )

    def test_rejects_invalid_or_too_many_ae_titles(self):
        for value in ("PACS*", "A" * 17, "PACS\\OTHER"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_store_allowed_aets(value)
        with self.assertRaises(ValueError):
            validate_store_allowed_aets(",".join(f"AE{i}" for i in range(33)))


class StoreAllowlistCompactionTest(unittest.TestCase):
    def compact(self, directory: Path, sender: str | None, allowed: frozenset[str]):
        source = directory / "CT.1"
        output = directory / "send" / "CT.1.dcm"
        error = directory / "error" / "CT.1"
        output.parent.mkdir(exist_ok=True)
        error.parent.mkdir(exist_ok=True)
        write_received_dicom(source, sender)

        result = _compact_one(
            str(source),
            str(output),
            str(error),
            str(directory / ".work"),
            "token",
            set(),
            {"*": "lossless"},
            (),
            allowed,
        )
        return result, source, output, error

    def test_unlisted_sender_is_quarantined_before_any_processing(self):
        with tempfile.TemporaryDirectory() as directory:
            result, source, output, error = self.compact(
                Path(directory), "INTRUSO", frozenset({"SRVPACS"})
            )
            self.assertEqual(result.status, "rejected_sender")
            self.assertEqual(result.error_type, "UnauthorizedSender")
            self.assertEqual(result.sender_aet, "INTRUSO")
            self.assertEqual(result.study_uid, "")
            self.assertEqual(result.codec_method, "")
            self.assertFalse(source.exists())
            self.assertFalse(output.exists())
            self.assertTrue(error.exists())

    def test_missing_calling_ae_is_rejected_when_list_is_configured(self):
        with tempfile.TemporaryDirectory() as directory:
            result, *_ = self.compact(Path(directory), None, frozenset({"SRVPACS"}))
            self.assertEqual(result.status, "rejected_sender")

    def test_listed_sender_matches_case_insensitively(self):
        with tempfile.TemporaryDirectory() as directory:
            result, source, output, _error = self.compact(
                Path(directory), "srvpacs ", frozenset({"SRVPACS"})
            )
            self.assertEqual(result.status, "compressed")
            self.assertEqual(result.codec_method, "copy")
            self.assertTrue(Path(result.temp_output).is_file())
            self.assertFalse(output.exists())

    def test_empty_list_keeps_accepting_any_sender(self):
        with tempfile.TemporaryDirectory() as directory:
            result, *_ = self.compact(Path(directory), None, frozenset())
            self.assertEqual(result.status, "compressed")


class StoreAllowlistPersistenceTest(DatabaseTestCase):
    def test_rejected_sender_never_creates_or_links_an_order(self):
        with self.Session() as db:
            unit = make_unit(store_allowed_aets="SRVPACS")
            db.add(unit)
            db.commit()
            result = CompactResult(
                "CT.1",
                "CT.1",
                "1.2.3.foreign",
                "rejected_sender",
                "UnauthorizedSender",
                patient_id="30211738",
                birth_date="19691027",
                accession="ACC-1",
                modality="CT",
                sender_aet="INTRUSO",
            )
            _record_compact_result(db, unit, result)
            db.commit()

            self.assertEqual(db.scalar(select(func.count()).select_from(Order)), 0)
            transfer = db.scalar(select(ImageTransfer))
            self.assertIsNone(transfer.order_id)
            self.assertEqual(transfer.status, "rejected_sender")
            self.assertEqual(transfer.last_error, "UnauthorizedSender")
