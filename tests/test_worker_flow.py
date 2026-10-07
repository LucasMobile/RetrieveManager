import asyncio
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.dicom_net import MoveResult
from app.models import ImageTransfer, ManualMoveRequest, Order, Unit
from app.orders_api import AckResult
from app.pipeline.move import Arrival, _run_manual_move
from app.pipeline.orders import acknowledge_pending_orders, recover_stale_locks
from app.pipeline.send import SendResult, send_unit
from tests.support import DatabaseTestCase, make_unit


class NetworkTransactionTest(DatabaseTestCase):
    def test_put_returns_connection_before_first_response(self):
        with self.Session() as db:
            unit = make_unit()
            db.add(unit)
            db.flush()
            db.add(
                Order(
                    unit_id=unit.id,
                    acc="ONE",
                    pat_id="P",
                    birth_date="20000101",
                    exam_date="20260917",
                    api_read_status="pending",
                )
            )
            db.commit()

            async def remote(*_args, **_kwargs):
                self.assertFalse(db.in_transaction())
                yield AckResult("ONE", True)

            with patch("app.pipeline.orders.iter_acknowledgements", remote):
                acknowledge_pending_orders(db, unit)
            self.assertEqual(db.scalar(select(Order.api_read_status)), "confirmed")

    def test_manual_move_returns_connection_before_pacs(self):
        with self.Session() as db:
            unit = make_unit()
            db.add(unit)
            db.flush()
            order = Order(
                unit_id=unit.id,
                acc="ONE",
                pat_id="P",
                birth_date="20000101",
                exam_date="20260917",
                study_uid="1.2.3",
                status="done",
            )
            db.add(order)
            db.flush()
            request = ManualMoveRequest(
                unit_id=unit.id,
                order_id=order.id,
                status="running",
                correlation_id="manual",
            )
            db.add(request)
            db.commit()
            request_id = request.id
        with self.Session() as db:

            def remote(*_args, **_kwargs):
                self.assertFalse(db.in_transaction())
                return MoveResult(True, 0x0000, completed=1)

            with (
                patch("app.pipeline.move.move_study", side_effect=remote),
                patch(
                    "app.pipeline.move._confirm_arrival",
                    side_effect=lambda _db, _unit, _uid, moved, _at: Arrival(moved, 1),
                ),
            ):
                _run_manual_move(db, request_id)
            self.assertEqual(db.get(ManualMoveRequest, request_id).status, "done")

    def test_long_running_move_is_not_reclaimed_at_twenty_minutes(self):
        with self.Session() as db:
            unit = make_unit(move_timeout_first=7200)
            db.add(unit)
            db.flush()
            order = Order(
                unit_id=unit.id,
                acc="ONE",
                pat_id="P",
                birth_date="20000101",
                exam_date="20260917",
                status="retrieving",
                heartbeat_at=datetime.now() - timedelta(minutes=30),
            )
            db.add(order)
            db.commit()
            recover_stale_locks(db)
            self.assertEqual(order.status, "retrieving")
            order.heartbeat_at = datetime.now() - timedelta(hours=3)
            db.commit()
            recover_stale_locks(db)
            self.assertEqual(order.status, "wait_retrieve")


class ProgressiveUploadTest(DatabaseTestCase):
    def make_transfers(self, db, directory):
        unit = make_unit(send_dir=directory, send_workers=2)
        db.add(unit)
        db.flush()
        for index in range(2):
            name = f"{index}.dcm"
            Path(directory, name).write_bytes(b"fixture")
            db.add(
                ImageTransfer(
                    unit_id=unit.id,
                    filename=name,
                    correlation_id=f"file-{index}",
                    status="compressed",
                )
            )
        db.commit()
        return unit

    def test_fast_200_is_committed_and_deleted_before_slow_upload_finishes(self):
        with tempfile.TemporaryDirectory() as directory, self.Session() as db:
            unit = self.make_transfers(db, directory)

            async def transport(_session, _sem, transfer_id, path, _url, correlation):
                if path.name == "1.dcm":

                    async def wait_for_fast_checkpoint():
                        while Path(directory, "0.dcm").exists():
                            await asyncio.sleep(0.005)

                    await asyncio.wait_for(wait_for_fast_checkpoint(), timeout=1)
                    with self.Session() as check:
                        self.assertEqual(
                            check.scalar(
                                select(ImageTransfer.status).where(
                                    ImageTransfer.filename == "0.dcm"
                                )
                            ),
                            "uploaded",
                        )
                return SendResult(transfer_id, correlation, True, http_status=200)

            with patch("app.pipeline.send._send_one", side_effect=transport):
                send_unit(db, unit, "https://cloud.example/upload")
            self.assertFalse(Path(directory, "1.dcm").exists())
            self.assertEqual(
                list(db.scalars(select(ImageTransfer.status))), ["uploaded"] * 2
            )

    def test_database_failure_after_200_preserves_source_files(self):
        with tempfile.TemporaryDirectory() as directory, self.Session() as db:
            unit = self.make_transfers(db, directory)

            async def transport(_session, _sem, transfer_id, _path, _url, correlation):
                return SendResult(transfer_id, correlation, True, http_status=200)

            with (
                patch("app.pipeline.send._send_one", side_effect=transport),
                patch(
                    "app.pipeline.send._persist_send_result_chunk",
                    side_effect=SQLAlchemyError("offline"),
                ),
                patch(
                    "app.pipeline.send._persist_send_result_with_retry",
                    return_value=None,
                ),
            ):
                send_unit(db, unit, "https://cloud.example/upload")
            self.assertTrue(Path(directory, "0.dcm").exists())
            self.assertTrue(Path(directory, "1.dcm").exists())
            self.assertEqual(
                list(db.scalars(select(ImageTransfer.status))), ["compressed"] * 2
            )

    def test_editing_unit_during_upload_does_not_delete_from_new_directory(self):
        with (
            tempfile.TemporaryDirectory() as original,
            tempfile.TemporaryDirectory() as replacement,
            self.Session() as db,
        ):
            unit = self.make_transfers(db, original)
            for name in ("0.dcm", "1.dcm"):
                Path(replacement, name).write_bytes(b"not uploaded")

            async def transport(_session, _sem, transfer_id, _path, _url, correlation):
                with self.Session() as edit:
                    edited = edit.get(Unit, unit.id)
                    edited.send_dir = replacement
                    edit.commit()
                return SendResult(transfer_id, correlation, True, http_status=200)

            with patch("app.pipeline.send._send_one", side_effect=transport):
                send_unit(db, unit, "https://cloud.example/upload")
            self.assertFalse(Path(original, "0.dcm").exists())
            self.assertFalse(Path(original, "1.dcm").exists())
            self.assertTrue(Path(replacement, "0.dcm").exists())
            self.assertTrue(Path(replacement, "1.dcm").exists())
