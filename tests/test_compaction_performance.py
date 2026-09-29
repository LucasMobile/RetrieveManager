import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import func, select

from app.compaction import CompactResult
from app.models import DicomInstance, DicomStudy, ImageTransfer, Order
from app.pipeline.compact import _cleanup_compaction_temps, compact_unit
from app.pipeline.records import persist_compact_chunk
from tests.support import DatabaseTestCase, make_unit


class CompactionTempsTest(unittest.TestCase):
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
                compact_workers=1,
            )
            db.add(unit)
            db.flush()
            study = DicomStudy(unit_id=unit.id, study_uid="1.2.3")
            db.add(study)
            db.flush()
            instance = DicomInstance(
                unit_id=unit.id,
                study_id=study.id,
                sop_uid="1.2.3.4",
                source_path=str(source),
                source_sha256="0" * 64,
                state="received",
            )
            db.add(instance)
            db.commit()
            transaction_during_codec = []

            def fail_codec(*_args):
                transaction_during_codec.append(db.in_transaction())
                raise RuntimeError("codec failed unexpectedly")

            with (
                patch(
                    "app.pipeline.compact.compression_runtime_settings",
                    return_value=(set(), {"*": "lossless"}),
                ),
                patch("app.pipeline.compact.load_rule_specs", return_value=()),
                patch(
                    "app.pipeline.compact._compact_one_limited",
                    side_effect=fail_codec,
                ),
            ):
                has_more = compact_unit(db, unit)

            self.assertEqual(transaction_during_codec, [False])
            self.assertFalse(has_more)
            self.assertFalse(source.exists())
            self.assertTrue((error / source.name).is_file())
            db.refresh(instance)
            self.assertEqual(instance.state, "error")
            self.assertEqual(instance.last_error, "RuntimeError")

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
                recorded, failed = persist_compact_chunk(db, unit, results)

            self.assertEqual((len(recorded), failed), (5, 0))
            self.assertEqual({row[2] for row in recorded}, {"publishing"})
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

            with patch(
                "app.pipeline.records._record_compact_result", side_effect=persist
            ):
                recorded, failed = persist_compact_chunk(db, unit, results)

            self.assertEqual(
                [row[0].source_name for row in recorded], ["first", "last"]
            )
            self.assertEqual(failed, 1)


if __name__ == "__main__":
    unittest.main()
