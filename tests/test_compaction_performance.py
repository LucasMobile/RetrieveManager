import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.pipeline import _settled_files, compact_unit
from tests.support import DatabaseTestCase, make_unit


class SettledFilesTest(unittest.TestCase):
    def test_scan_stops_at_batch_limit_and_ignores_hidden_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index in range(10):
                (root / f"image-{index}.dcm").write_bytes(b"DICOM")
            (root / ".compression-in-progress.tmp").write_bytes(b"partial")

            files, errors = _settled_files(root, 0, limit=3)

            self.assertEqual(len(files), 3)
            self.assertFalse(errors)
            self.assertTrue(all(not path.name.startswith(".") for path in files))


class CompactionFailureIsolationTest(DatabaseTestCase):
    def test_unexpected_file_failure_is_quarantined(self):
        with tempfile.TemporaryDirectory() as directory, self.Session() as db:
            root = Path(directory)
            receive = root / "receive"
            send = root / "send"
            error = root / "error"
            receive.mkdir()
            source = receive / "broken-image"
            source.write_bytes(b"invalid")
            unit = make_unit(
                name="Unidade",
                receive_dir=str(receive),
                send_dir=str(send),
                error_dir=str(error),
                file_settle_seconds=0,
                compact_workers=1,
            )
            db.add(unit)
            db.commit()

            with (
                patch(
                    "app.pipeline.compression_runtime_settings",
                    return_value=(set(), {"*": "+e1"}),
                ),
                patch("app.pipeline.load_rule_specs", return_value=()),
                patch(
                    "app.pipeline._compact_one_limited",
                    side_effect=RuntimeError("codec failed unexpectedly"),
                ),
            ):
                has_more = compact_unit(db, unit)

            self.assertFalse(has_more)
            self.assertFalse(source.exists())
            self.assertTrue((error / source.name).is_file())


if __name__ == "__main__":
    unittest.main()
