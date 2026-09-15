import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.models import ImageTransfer
from app.pipeline import (
    CircuitState,
    SendResult,
    _next_send_reconcile_paths,
    _reconcile_send_directory,
    _record_send_results,
    _send_reconcile_states,
    send_unit,
)
from app.worker import _send_job, stop_event
from tests.support import DatabaseTestCase, make_unit


class SendPerformanceTest(DatabaseTestCase):
    def test_send_uses_bounded_database_queue_and_reports_more_work(self):
        with tempfile.TemporaryDirectory() as send_dir, self.Session() as db:
            unit = make_unit(name="unit", send_dir=send_dir, send_workers=2)
            db.add(unit)
            db.flush()
            transfers = []
            for index in range(3):
                filename = f"image-{index}.dcm"
                Path(send_dir, filename).write_bytes(b"DICOM")
                transfer = ImageTransfer(
                    unit_id=unit.id,
                    filename=filename,
                    correlation_id=f"corr-{index}",
                    status="compressed",
                )
                db.add(transfer)
                transfers.append(transfer)
            db.commit()

            async def successful_batch(jobs, _url, _workers):
                return [
                    SendResult(job[0], job[2], True, http_status=200)
                    for job in jobs
                ]

            with (
                patch("app.pipeline.SEND_BATCH_SIZE", 2),
                patch(
                    "app.pipeline._reconcile_send_directory",
                    return_value=(0, False),
                ),
                patch("app.pipeline._send_all", side_effect=successful_batch),
            ):
                has_more = send_unit(db, unit, "https://cloud.example/upload", 0)

            statuses = list(
                db.scalars(
                    select(ImageTransfer.status).order_by(ImageTransfer.id)
                )
            )
            self.assertTrue(has_more)
            self.assertEqual(statuses, ["uploaded", "uploaded", "compressed"])

    def test_incremental_reconciliation_advances_through_directory(self):
        with tempfile.TemporaryDirectory() as send_dir:
            directory = Path(send_dir)
            for index in range(5):
                (directory / f"image-{index}.dcm").write_bytes(b"DICOM")
            _send_reconcile_states.clear()
            found = []
            with patch("app.pipeline.SEND_RECONCILE_BATCH_SIZE", 2):
                for _ in range(3):
                    paths, errors, _has_more = _next_send_reconcile_paths(
                        999,
                        directory,
                        0,
                        timestamp=datetime.now().timestamp(),
                    )
                    self.assertEqual(errors, [])
                    found.extend(path.name for path in paths)
            self.assertEqual(set(found), {f"image-{index}.dcm" for index in range(5)})

    def test_reconciliation_registers_an_orphan_file(self):
        with tempfile.TemporaryDirectory() as send_dir, self.Session() as db:
            unit = make_unit(name="unit", send_dir=send_dir)
            db.add(unit)
            db.commit()
            Path(send_dir, "orphan.dcm").write_bytes(b"DICOM")
            _send_reconcile_states.clear()

            registered, _has_more = _reconcile_send_directory(
                db, unit, Path(send_dir), 0, datetime.now()
            )

            transfer = db.scalar(select(ImageTransfer))
            self.assertEqual(registered, 1)
            self.assertIsNotNone(transfer)
            self.assertEqual(transfer.filename, "orphan.dcm")
            self.assertEqual(transfer.status, "compressed")

    def test_missing_send_file_is_terminal_and_does_not_starve_queue(self):
        with tempfile.TemporaryDirectory() as send_dir, self.Session() as db:
            unit = make_unit(name="unit", send_dir=send_dir)
            db.add(unit)
            db.flush()
            transfer = ImageTransfer(
                unit_id=unit.id,
                filename="missing.dcm",
                correlation_id="corr",
                status="compressed",
            )
            db.add(transfer)
            db.commit()

            with patch(
                "app.pipeline._reconcile_send_directory", return_value=(0, False)
            ):
                has_more = send_unit(db, unit, "https://cloud.example/upload", 0)

            db.refresh(transfer)
            self.assertFalse(has_more)
            self.assertEqual(transfer.status, "file_missing")

    def test_unavailable_send_directory_does_not_change_queue_state(self):
        with tempfile.TemporaryDirectory() as root_dir, self.Session() as db:
            send_dir = str(Path(root_dir) / "unmounted")
            unit = make_unit(name="unit", send_dir=send_dir)
            db.add(unit)
            db.flush()
            transfer = ImageTransfer(
                unit_id=unit.id,
                filename="image.dcm",
                correlation_id="corr",
                status="compressed",
            )
            db.add(transfer)
            db.commit()

            has_more = send_unit(db, unit, "https://cloud.example/upload", 0)

            db.refresh(transfer)
            self.assertFalse(has_more)
            self.assertEqual(transfer.status, "compressed")

    def test_reconciliation_recovers_a_file_that_reappears(self):
        with tempfile.TemporaryDirectory() as send_dir, self.Session() as db:
            unit = make_unit(name="unit", send_dir=send_dir)
            db.add(unit)
            db.flush()
            transfer = ImageTransfer(
                unit_id=unit.id,
                filename="recovered.dcm",
                correlation_id="corr",
                status="file_missing",
            )
            db.add(transfer)
            db.commit()
            Path(send_dir, transfer.filename).write_bytes(b"DICOM")
            _send_reconcile_states.clear()

            recovered, _has_more = _reconcile_send_directory(
                db, unit, Path(send_dir), 0, datetime.now()
            )

            db.refresh(transfer)
            self.assertEqual(recovered, 1)
            self.assertEqual(transfer.status, "compressed")

    def test_send_results_are_committed_in_chunks(self):
        with tempfile.TemporaryDirectory() as send_dir, self.Session() as db:
            unit = make_unit(name="unit", send_dir=send_dir)
            db.add(unit)
            db.flush()
            results = []
            for index in range(5):
                filename = f"image-{index}.dcm"
                Path(send_dir, filename).write_bytes(b"DICOM")
                transfer = ImageTransfer(
                    unit_id=unit.id,
                    filename=filename,
                    correlation_id=f"corr-{index}",
                    status="compressed",
                )
                db.add(transfer)
                db.flush()
                results.append(
                    SendResult(transfer.id, transfer.correlation_id, True, 200)
                )
            db.commit()

            with (
                patch("app.pipeline.SEND_DB_BATCH_SIZE", 3),
                patch.object(db, "commit", wraps=db.commit) as commit,
            ):
                failures, successes = _record_send_results(
                    db, unit, results, CircuitState()
                )

            self.assertEqual(failures, 0)
            self.assertEqual(successes, 5)
            self.assertEqual(commit.call_count, 2)

    def test_failed_batch_commit_falls_back_to_isolated_rows(self):
        with tempfile.TemporaryDirectory() as send_dir, self.Session() as db:
            unit = make_unit(name="unit", send_dir=send_dir)
            db.add(unit)
            db.flush()
            results = []
            for index in range(2):
                filename = f"fallback-{index}.dcm"
                Path(send_dir, filename).write_bytes(b"DICOM")
                transfer = ImageTransfer(
                    unit_id=unit.id,
                    filename=filename,
                    correlation_id=f"corr-{index}",
                    status="compressed",
                )
                db.add(transfer)
                db.flush()
                results.append(
                    SendResult(transfer.id, transfer.correlation_id, True, 200)
                )
            db.commit()
            real_commit = db.commit
            commit_calls = 0

            def fail_first_commit():
                nonlocal commit_calls
                commit_calls += 1
                if commit_calls == 1:
                    raise SQLAlchemyError("forced batch failure")
                return real_commit()

            with (
                patch("app.pipeline.SEND_DB_BATCH_SIZE", 2),
                patch.object(db, "commit", side_effect=fail_first_commit),
            ):
                failures, successes = _record_send_results(
                    db, unit, results, CircuitState()
                )

            statuses = list(db.scalars(select(ImageTransfer.status)))
            self.assertEqual(failures, 0)
            self.assertEqual(successes, 2)
            self.assertEqual(statuses, ["uploaded", "uploaded"])
            self.assertEqual(commit_calls, 3)


class SendWorkerDrainTest(unittest.TestCase):
    def test_send_job_drains_until_stage_reports_empty(self):
        stop_event.clear()
        with patch("app.worker._run_unit_stage", side_effect=[True, False]) as run:
            _send_job(7)
        self.assertEqual(run.call_count, 2)


if __name__ == "__main__":
    unittest.main()
