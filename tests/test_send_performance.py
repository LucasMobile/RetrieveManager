import asyncio
import hashlib
import tempfile
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.compaction import CompactResult
from app.models import DicomInstance, DicomStudy, ImageTransfer
from app.pipeline.compact import _publish_outputs, _recover_publishing_transfers
from app.pipeline.send import (
    CircuitState,
    SendErrorProbe,
    SendResult,
    _cloud_circuits,
    _record_send_results,
    _send_error_probes,
    resend_failed_transfers,
    send_unit,
)
from app.worker import _send_job, stop_event
from tests.support import DatabaseTestCase, make_unit

URL = "https://cloud.example/upload"


def ok(transfer_id, correlation_id):
    return SendResult(transfer_id, correlation_id, True, http_status=200)


class SendQueueTest(DatabaseTestCase):
    def setUp(self):
        super().setUp()
        _cloud_circuits.clear()
        _send_error_probes.clear()
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.send_dir = Path(temporary.name)

    def make_queue(
        self, db, count, workers=2, status_override="compressed", **transfer_values
    ):
        unit = make_unit(send_dir=str(self.send_dir), send_workers=workers)
        db.add(unit)
        db.flush()
        for index in range(count):
            name = f"image-{index}.dcm"
            (self.send_dir / name).write_bytes(b"DICOM")
            db.add(
                ImageTransfer(
                    unit_id=unit.id,
                    filename=name,
                    correlation_id=f"corr-{index}",
                    status=status_override,
                    **{"attempts": 7, **transfer_values}
                    if status_override == "send_error"
                    else transfer_values,
                )
            )
        db.commit()
        return unit

    def statuses(self, db):
        db.expire_all()
        return list(db.scalars(select(ImageTransfer.status).order_by(ImageTransfer.id)))

    def test_uploads_start_in_fifo_order_without_open_transaction(self):
        with self.Session() as db:
            unit = self.make_queue(db, 5)
            started, transactions = [], []

            async def upload(_session, _sem, transfer_id, path, _url, correlation):
                started.append(path.name)
                transactions.append(db.in_transaction())
                return ok(transfer_id, correlation)

            with patch("app.pipeline.send._send_one", side_effect=upload):
                has_more = send_unit(db, unit, URL)

            self.assertFalse(has_more)
            self.assertEqual(started, [f"image-{index}.dcm" for index in range(5)])
            self.assertEqual(set(transactions), {False})
            self.assertEqual(self.statuses(db), ["uploaded"] * 5)
            self.assertFalse(any(self.send_dir.iterdir()))

    def test_slow_upload_holds_only_its_own_slot(self):
        with self.Session() as db:
            unit = self.make_queue(db, 6, workers=2)
            others_done = asyncio.Event()
            finished = []

            async def upload(_session, semaphore, transfer_id, path, _url, correlation):
                async with semaphore:
                    if path.name == "image-0.dcm":
                        await asyncio.wait_for(others_done.wait(), timeout=5)
                    finished.append(path.name)
                    if len(finished) == 5:
                        others_done.set()
                    return ok(transfer_id, correlation)

            with patch("app.pipeline.send._send_one", side_effect=upload):
                send_unit(db, unit, URL)

            # The other five went through the second slot meanwhile.
            self.assertEqual(finished[-1], "image-0.dcm")
            self.assertEqual(self.statuses(db), ["uploaded"] * 6)

    def test_drain_limit_returns_control_with_work_left(self):
        with self.Session() as db:
            unit = self.make_queue(db, 5, workers=1)

            async def upload(_session, _sem, transfer_id, _path, _url, correlation):
                return ok(transfer_id, correlation)

            with (
                patch("app.pipeline.send.SEND_DRAIN_SECONDS", 0),
                patch("app.pipeline.send._send_one", side_effect=upload),
            ):
                has_more = send_unit(db, unit, URL)

            self.assertTrue(has_more)
            self.assertEqual(self.statuses(db), ["uploaded"] * 2 + ["compressed"] * 3)

    def test_failure_waits_with_growing_delay(self):
        with self.Session() as db:
            unit = self.make_queue(db, 1)

            async def fail(_session, _sem, transfer_id, _path, _url, correlation):
                return SendResult(transfer_id, correlation, False, http_status=503)

            before = datetime.now()
            with patch("app.pipeline.send._send_one", side_effect=fail) as upload:
                self.assertFalse(send_unit(db, unit, URL))
            transfer = db.scalar(select(ImageTransfer))
            self.assertEqual(upload.call_count, 1)  # cooldown is filtered in SQL
            self.assertEqual((transfer.status, transfer.attempts), ("upload_error", 1))
            wait = (transfer.next_attempt_at - before).total_seconds()
            self.assertGreaterEqual(wait, 8)
            self.assertLessEqual(wait, 13)

            transfer.next_attempt_at = datetime.now()
            db.commit()
            with patch("app.pipeline.send._send_one", side_effect=fail):
                send_unit(db, unit, URL)
            db.refresh(transfer)
            wait = (transfer.next_attempt_at - datetime.now()).total_seconds()
            self.assertEqual(transfer.attempts, 2)
            self.assertGreaterEqual(wait, 23)

    def test_last_attempt_moves_to_send_error_and_can_be_resent(self):
        with self.Session() as db:
            unit = self.make_queue(db, 1, attempts=6)

            async def fail(_session, _sem, transfer_id, _path, _url, correlation):
                return SendResult(
                    transfer_id, correlation, False, error_type="ClientError"
                )

            with patch("app.pipeline.send._send_one", side_effect=fail):
                send_unit(db, unit, URL)
            transfer = db.scalar(select(ImageTransfer))
            self.assertEqual((transfer.status, transfer.attempts), ("send_error", 7))
            self.assertIsNone(transfer.next_attempt_at)
            self.assertTrue((self.send_dir / transfer.filename).exists())

            with patch("app.pipeline.send._send_one") as upload:
                send_unit(db, unit, URL)
            upload.assert_not_called()

            count = resend_failed_transfers(db, ImageTransfer.unit_id == unit.id)
            db.commit()
            db.refresh(transfer)
            self.assertEqual(count, 1)
            self.assertEqual((transfer.status, transfer.attempts), ("compressed", 0))
            self.assertEqual(transfer.last_error, "")

    def test_exhausted_uploads_are_probed_every_five_minutes(self):
        with self.Session() as db:
            unit = self.make_queue(db, 3, status_override="send_error")
            tried = []

            async def fail(_session, _sem, transfer_id, path, _url, correlation):
                tried.append(path.name)
                return SendResult(transfer_id, correlation, False, http_status=503)

            with patch("app.pipeline.send._send_one", side_effect=fail):
                send_unit(db, unit, URL)  # first sighting starts the clock
                self.assertEqual(tried, [])
                probe = _send_error_probes[unit.id]
                probe.next_at = 0
                send_unit(db, unit, URL)
                send_unit(db, unit, URL)  # not due again yet
                probe.next_at = 0
                send_unit(db, unit, URL)

            # One upload per check, rotating between the exhausted files.
            self.assertEqual(tried, ["image-0.dcm", "image-1.dcm"])
            self.assertGreater(probe.next_at - time.monotonic(), 290)
            self.assertEqual(self.statuses(db), ["send_error"] * 3)
            transfer = db.scalar(select(ImageTransfer))
            self.assertEqual(transfer.attempts, 7)

    def test_successful_probe_resumes_every_exhausted_upload(self):
        with self.Session() as db:
            unit = self.make_queue(db, 3, status_override="send_error")
            _send_error_probes[unit.id] = SendErrorProbe(next_at=0)

            async def upload(_session, _sem, transfer_id, _path, _url, correlation):
                return ok(transfer_id, correlation)

            with patch("app.pipeline.send._send_one", side_effect=upload) as sent:
                self.assertFalse(send_unit(db, unit, URL))

            self.assertEqual(sent.call_count, 3)
            self.assertEqual(self.statuses(db), ["uploaded"] * 3)
            self.assertNotIn(unit.id, _send_error_probes)

    def test_missing_file_is_marked_and_does_not_block_the_queue(self):
        with self.Session() as db:
            unit = self.make_queue(db, 3)
            (self.send_dir / "image-0.dcm").unlink()

            async def upload(_session, _sem, transfer_id, _path, _url, correlation):
                return ok(transfer_id, correlation)

            with patch("app.pipeline.send._send_one", side_effect=upload):
                send_unit(db, unit, URL)

            self.assertEqual(
                self.statuses(db), ["file_missing", "uploaded", "uploaded"]
            )

    def test_unavailable_send_directory_does_not_change_queue_state(self):
        with self.Session() as db:
            unit = make_unit(send_dir=str(self.send_dir / "unmounted"))
            db.add(unit)
            db.flush()
            db.add(
                ImageTransfer(
                    unit_id=unit.id,
                    filename="image.dcm",
                    correlation_id="corr",
                    status="compressed",
                )
            )
            db.commit()
            self.assertFalse(send_unit(db, unit, URL))
            self.assertEqual(self.statuses(db), ["compressed"])

    def test_open_circuit_stops_refilling(self):
        with self.Session() as db:
            unit = self.make_queue(db, 20, workers=1)

            async def fail(_session, _sem, transfer_id, _path, _url, correlation):
                return SendResult(transfer_id, correlation, False, http_status=503)

            with (
                patch("app.pipeline.send.SEND_DB_BATCH_SIZE", 1),
                patch("app.pipeline.send.CIRCUIT_BREAKER_FAILURES", 3),
                patch("app.pipeline.send._send_one", side_effect=fail) as upload,
            ):
                self.assertFalse(send_unit(db, unit, URL))
                self.assertFalse(send_unit(db, unit, URL))

            # 2 queued when the circuit opened on the third failure.
            self.assertLessEqual(upload.call_count, 4)
            self.assertIn("compressed", self.statuses(db))

    def test_send_results_are_committed_in_chunks(self):
        with self.Session() as db:
            unit = self.make_queue(db, 5)
            results = [
                ok(transfer.id, transfer.correlation_id)
                for transfer in db.scalars(select(ImageTransfer))
            ]
            with (
                patch("app.pipeline.send.SEND_DB_BATCH_SIZE", 3),
                patch.object(db, "commit", wraps=db.commit) as commit,
            ):
                failures, successes = _record_send_results(
                    db, unit, results, CircuitState()
                )
            self.assertEqual((failures, successes), (0, 5))
            self.assertEqual(commit.call_count, 2)

    def test_failed_batch_commit_falls_back_to_isolated_rows(self):
        with self.Session() as db:
            unit = self.make_queue(db, 2)
            results = [
                ok(transfer.id, transfer.correlation_id)
                for transfer in db.scalars(select(ImageTransfer))
            ]
            real_commit = db.commit
            commit_calls = 0

            def fail_first_commit():
                nonlocal commit_calls
                commit_calls += 1
                if commit_calls == 1:
                    raise SQLAlchemyError("forced batch failure")
                return real_commit()

            with (
                patch("app.pipeline.send.SEND_DB_BATCH_SIZE", 2),
                patch.object(db, "commit", side_effect=fail_first_commit),
            ):
                failures, successes = _record_send_results(
                    db, unit, results, CircuitState()
                )
            self.assertEqual((failures, successes), (0, 2))
            self.assertEqual(self.statuses(db), ["uploaded", "uploaded"])
            self.assertEqual(commit_calls, 3)


class PublishTest(DatabaseTestCase):
    def setUp(self):
        super().setUp()
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.receive, self.send, self.work = root / "rx", root / "tx", root / "work"
        for directory in (self.receive, self.send, self.work):
            directory.mkdir()
        with self.Session() as db:
            self.unit = make_unit(
                receive_dir=str(self.receive), send_dir=str(self.send)
            )
            db.add(self.unit)
            db.commit()

    def publishing(self, db, *, stale=True, written=False):
        source = self.receive / "CT.1"
        source.write_bytes(b"received")
        content = b"compacted"
        if written:
            (self.send / "CT.1.dcm").write_bytes(content)
        study = DicomStudy(unit_id=self.unit.id, study_uid="1.2")
        db.add(study)
        db.flush()
        transfer = ImageTransfer(
            unit_id=self.unit.id,
            filename="CT.1.dcm",
            correlation_id="corr",
            status="publishing",
            sha256=hashlib.sha256(content).hexdigest(),
        )
        db.add(transfer)
        db.flush()
        instance = DicomInstance(
            unit_id=self.unit.id,
            study_id=study.id,
            sop_uid="1.2.3",
            source_path=str(source),
            source_sha256="0" * 64,
            state="compacted",
            transfer_id=transfer.id,
        )
        db.add(instance)
        db.commit()
        if stale:
            transfer.updated_at = datetime.now() - timedelta(hours=1)
            db.commit()
        return transfer, instance, source

    def test_output_is_published_after_its_row_is_committed(self):
        with self.Session() as db:
            transfer, _instance, source = self.publishing(db, stale=False)
            temp = self.work / ".x.output.tmp"
            temp.write_bytes(b"compacted")
            result = CompactResult(
                "CT.1",
                "CT.1.dcm",
                "1.2",
                "compressed",
                source_path=str(source),
                temp_output=str(temp),
            )
            _publish_outputs(
                db, self.unit, [result], [(result, transfer.id, "publishing")]
            )

            db.refresh(transfer)
            self.assertEqual(transfer.status, "compressed")
            self.assertEqual((self.send / "CT.1.dcm").read_bytes(), b"compacted")
            self.assertFalse(source.exists())
            self.assertFalse(temp.exists())

    def test_unrecorded_output_is_dropped_and_source_kept(self):
        with self.Session() as db:
            source = self.receive / "CT.2"
            source.write_bytes(b"received")
            temp = self.work / ".y.output.tmp"
            temp.write_bytes(b"compacted")
            result = CompactResult(
                "CT.2",
                "CT.2.dcm",
                "1.2",
                "compressed",
                source_path=str(source),
                temp_output=str(temp),
            )
            _publish_outputs(db, self.unit, [result], [])
            self.assertFalse(temp.exists())
            self.assertTrue(source.exists())
            self.assertFalse(any(self.send.iterdir()))

    def test_recovery_releases_an_artifact_already_in_place(self):
        with self.Session() as db:
            transfer, _instance, source = self.publishing(db, written=True)
            _recover_publishing_transfers(db, self.unit)
            db.refresh(transfer)
            self.assertEqual(transfer.status, "compressed")
            self.assertFalse(source.exists())

    def test_recovery_recompacts_when_the_artifact_never_arrived(self):
        with self.Session() as db:
            transfer, instance, source = self.publishing(db)
            _recover_publishing_transfers(db, self.unit)
            db.refresh(transfer)
            db.refresh(instance)
            self.assertEqual(transfer.status, "recompact")
            self.assertEqual(instance.state, "received")
            self.assertTrue(source.exists())

    def test_recovery_marks_missing_when_nothing_is_left(self):
        with self.Session() as db:
            transfer, instance, source = self.publishing(db)
            source.unlink()
            _recover_publishing_transfers(db, self.unit)
            db.refresh(transfer)
            db.refresh(instance)
            self.assertEqual(transfer.status, "file_missing")
            self.assertEqual(instance.state, "missing")

    def test_recent_publishing_rows_are_left_alone(self):
        with self.Session() as db:
            transfer, _instance, _source = self.publishing(db, stale=False)
            _recover_publishing_transfers(db, self.unit)
            db.refresh(transfer)
            self.assertEqual(transfer.status, "publishing")


class SendWorkerDrainTest(unittest.TestCase):
    def test_send_job_drains_until_stage_reports_empty(self):
        stop_event.clear()
        with patch("app.worker._run_unit_stage", side_effect=[True, False]) as run:
            _send_job(7)
        self.assertEqual(run.call_count, 2)


if __name__ == "__main__":
    unittest.main()
