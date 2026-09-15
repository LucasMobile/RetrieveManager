import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import func, select

from app.models import ImageTransfer, Order
from app.pipeline import (
    CompactResult,
    _cleanup_compaction_temps,
    _persist_compact_chunk,
    _settled_files,
    compact_unit,
)
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

    def test_only_stale_compaction_temporaries_are_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stale = root / ".old.prepared.tmp"
            active = root / ".active.output.tmp"
            stale.write_bytes(b"old")
            active.write_bytes(b"active")
            os.utime(stale, (1, 1))

            errors = _cleanup_compaction_temps(root)

            self.assertFalse(errors)
            self.assertFalse(stale.exists())
            self.assertTrue(active.exists())


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

    def test_compact_metadata_is_committed_once_per_chunk(self):
        with self.Session() as db:
            unit = make_unit(name="Unidade")
            db.add(unit)
            db.flush()
            order = Order(
                unit_id=unit.id,
                source_id="source",
                acc="ACC",
                birth_date="20000101",
                study_uid="1.2.current",
                status="receiving",
                correlation_id="correlation",
            )
            db.add(order)
            db.commit()
            results = [
                CompactResult(
                    f"image-{index}",
                    f"image-{index}.dcm",
                    order.study_uid,
                    "compressed",
                )
                for index in range(5)
            ]

            with patch.object(db, "commit", wraps=db.commit) as commit:
                processed, failed = _persist_compact_chunk(db, unit, results)

            self.assertEqual((processed, failed), (5, 0))
            self.assertEqual(commit.call_count, 1)
            self.assertEqual(
                db.scalar(select(func.count()).select_from(ImageTransfer)),
                5,
            )

    def test_chunk_failure_falls_back_and_isolates_bad_metadata(self):
        with self.Session() as db:
            unit = make_unit(name="Unidade")
            db.add(unit)
            db.commit()
            results = [
                CompactResult(name, f"{name}.dcm", "1.2.3", "compressed")
                for name in ("first", "bad", "last")
            ]

            def persist(_db, _unit, result, **caches):
                if caches:
                    raise RuntimeError("force conservative fallback")
                if result.source_name == "bad":
                    raise ValueError("invalid metadata")

            with patch("app.pipeline._record_compact_result", side_effect=persist):
                processed, failed = _persist_compact_chunk(db, unit, results)

            self.assertEqual((processed, failed), (2, 1))


if __name__ == "__main__":
    unittest.main()
