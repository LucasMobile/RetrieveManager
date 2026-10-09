import os
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import event, func, select

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

    def test_chunk_writes_about_one_statement_per_result(self):
        with self.Session() as db:
            unit = make_unit(name="Unidade")
            db.add(unit)
            db.flush()
            db.add(
                Order(
                    unit_id=unit.id,
                    acc="ACC",
                    birth_date="20000101",
                    study_uid="1.2.current",
                    status="monitoring",
                    correlation_id="correlation",
                )
            )
            study = DicomStudy(unit_id=unit.id, study_uid="1.2.current")
            db.add(study)
            db.flush()
            rows = [
                DicomInstance(
                    unit_id=unit.id,
                    study_id=study.id,
                    sop_uid=str(index),
                    source_path=f"/receive/{index}",
                    source_sha256="0" * 64,
                    state="compacting",
                )
                for index in range(25)
            ]
            db.add_all(rows)
            db.commit()
            results = [
                replace(
                    CompactResult(
                        f"image-{index}",
                        f"image-{index}.dcm",
                        "1.2.current",
                        "compressed",
                    ),
                    instance_id=row.id,
                )
                for index, row in enumerate(rows)
            ]
        statements = []

        def count(*_args):
            statements.append(1)

        event.listen(self.engine, "before_cursor_execute", count)
        self.addCleanup(event.remove, self.engine, "before_cursor_execute", count)
        with self.Session(autoflush=False) as db:
            recorded, failed = persist_compact_chunk(db, unit, results)

        self.assertEqual((len(recorded), failed), (25, 0))
        # Prefetch, one INSERT per transfer, one UPDATE for all queue rows.
        self.assertLess(len(statements), 35)
        with self.Session() as db:
            closed = {
                row.id: (row.state, row.transfer_id)
                for row in db.scalars(select(DicomInstance))
            }
        self.assertEqual(
            closed,
            {
                result.instance_id: ("compacted", row[1])
                for result, row in zip(results, recorded, strict=True)
            },
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


class CompactionStreamTest(DatabaseTestCase):
    """The codec slots are refilled file by file, not batch by batch."""

    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.receive = root / "receive"
        self.receive.mkdir()
        with self.Session() as db:
            self.unit = make_unit(
                receive_dir=str(self.receive),
                send_dir=str(root / "send"),
                error_dir=str(root / "error"),
                compact_workers=2,
            )
            db.add(self.unit)
            db.flush()
            study = DicomStudy(unit_id=self.unit.id, study_uid="1.2.3")
            db.add(study)
            db.flush()
            self.study_id = study.id
            db.commit()

    def queue(self, *names):
        with self.Session() as db:
            for name in names:
                source = self.receive / name
                source.write_bytes(b"received")
                db.add(
                    DicomInstance(
                        unit_id=self.unit.id,
                        study_id=self.study_id,
                        sop_uid=name,
                        source_path=str(source),
                        source_sha256="0" * 64,
                        state="received",
                    )
                )
            db.commit()

    def compact(self, db, codec, **settings):
        patches = [
            patch(
                "app.pipeline.compact.compression_runtime_settings",
                return_value=(set(), {"*": "lossless"}),
            ),
            patch("app.pipeline.compact.load_rule_specs", return_value=()),
            patch("app.pipeline.compact._compact_one_limited", side_effect=codec),
        ] + [
            patch(f"app.pipeline.compact.{name}", value)
            for name, value in settings.items()
        ]
        for active in patches:
            active.start()
        try:
            return compact_unit(db, self.unit)
        finally:
            for active in reversed(patches):
                active.stop()

    @staticmethod
    def discarded(source):
        name = Path(source).name
        return CompactResult(name, "", "", "discarded_modality")

    def states(self):
        with self.Session() as db:
            return dict(
                db.execute(select(DicomInstance.sop_uid, DicomInstance.state)).all()
            )

    def test_slow_file_does_not_hold_the_rest_of_the_queue(self):
        self.queue("slow", *(f"fast-{index}" for index in range(5)))
        fast_recorded = threading.Event()
        waited = []

        def codec(_unit_id, source, *_args):
            if Path(source).name == "slow":
                # Released once the fast files were recorded and published.
                waited.append(fast_recorded.wait(10))
            return self.discarded(source)

        def watch(_db, _unit, _results, _recorded):
            if sum(state == "discarded" for state in self.states().values()) >= 5:
                fast_recorded.set()

        with (
            self.Session() as db,
            patch(
                "app.pipeline.compact._publish_outputs",
                side_effect=watch,
            ),
        ):
            has_more = self.compact(db, codec)

        # 2 workers claim 4 files ahead; the other 2 enter as slots free up,
        # and every fast file is recorded while the slow one still runs.
        self.assertEqual(waited, [True])
        self.assertFalse(has_more)
        self.assertEqual(set(self.states().values()), {"discarded"})

    def test_drain_time_bounds_one_call(self):
        self.queue(*(f"file-{index}" for index in range(6)))

        def codec(_unit_id, source, *_args):
            return self.discarded(source)

        with self.Session() as db:
            # No drain time: one claim (2 workers x 2 ahead) per call.
            first = self.compact(db, codec, COMPACT_DRAIN_SECONDS=0)
            self.assertTrue(first)
            self.assertEqual(sorted(self.states().values()).count("discarded"), 4)
            second = self.compact(db, codec, COMPACT_DRAIN_SECONDS=0)
        self.assertFalse(second)
        self.assertEqual(set(self.states().values()), {"discarded"})

    def test_stop_ends_the_refill(self):
        self.queue(*(f"file-{index}" for index in range(6)))
        stop = threading.Event()

        def codec(_unit_id, source, *_args):
            stop.set()
            return self.discarded(source)

        with (
            self.Session() as db,
            patch(
                "app.pipeline.compact.compression_runtime_settings",
                return_value=(set(), {"*": "lossless"}),
            ),
            patch("app.pipeline.compact.load_rule_specs", return_value=()),
            patch("app.pipeline.compact._compact_one_limited", side_effect=codec),
        ):
            has_more = compact_unit(db, self.unit, stop=stop)

        # The first claim is finished; nothing more is claimed after stop.
        self.assertTrue(has_more)
        self.assertEqual(sorted(self.states().values()).count("received"), 2)


if __name__ == "__main__":
    unittest.main()
