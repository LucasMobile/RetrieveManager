import unittest

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.main import (
    ACTIVE_ORDER_STATUSES,
    _delete_order_record,
    _reset_order_for_reprocess,
)
from app.models import Base, ImageTransfer, ModalityRule, Order, OrderEvent, Unit


class OrderActionsTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def tearDown(self):
        self.engine.dispose()

    @staticmethod
    def _unit() -> Unit:
        return Unit(
            name="unit",
            input_dir="/in",
            sent_dir="/sent",
            pacs_aet="PACS",
            pacs_ip="127.0.0.1",
            pacs_port=2104,
            calling_aet="RETRIEVE",
            dest_aet="RETRIEVE",
            store_port=444,
            receive_dir="/receive",
            send_dir="/send",
            error_dir="/error",
            token="token",
        )

    def test_delete_removes_history_and_preserves_transfer(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = Order(
                unit_id=unit.id,
                filename="order.txt",
                acc="123",
                birth_date="20000101",
                status="done",
            )
            db.add(order)
            db.flush()
            db.add(OrderEvent(order_id=order.id, message="done"))
            transfer = ImageTransfer(
                unit_id=unit.id,
                order_id=order.id,
                filename="image.dcm",
                correlation_id="correlation",
                status="uploaded",
            )
            db.add(transfer)
            db.commit()
            order_id = order.id
            transfer_id = transfer.id

            _delete_order_record(db, order)
            db.commit()

            self.assertIsNone(db.get(Order, order_id))
            self.assertEqual(
                list(
                    db.scalars(
                        select(OrderEvent).where(OrderEvent.order_id == order_id)
                    )
                ),
                [],
            )
            preserved = db.get(ImageTransfer, transfer_id)
            self.assertIsNotNone(preserved)
            self.assertIsNone(preserved.order_id)

    def test_active_statuses_protect_inflight_orders(self):
        self.assertEqual(
            ACTIVE_ORDER_STATUSES,
            {"retrieving", "retrieving_second", "receiving"},
        )

    def test_reprocess_restarts_completed_order(self):
        with self.Session() as db:
            unit = self._unit()
            db.add_all(
                [
                    unit,
                    ModalityRule(
                        modality="CT",
                        wait_minutes=15,
                        second_retrieve=True,
                        second_wait_minutes=90,
                    ),
                ]
            )
            db.flush()
            order = Order(
                unit_id=unit.id,
                filename="order.txt",
                acc="123",
                birth_date="20000101",
                modality="CT",
                study_uid="1.2.3",
                status="done",
                attempts=3,
                correlation_id="old-correlation",
                last_error="old error",
            )
            db.add(order)
            db.commit()

            _reset_order_for_reprocess(db, order)
            db.commit()

            self.assertEqual(order.status, "wait_retrieve")
            self.assertEqual(order.attempts, 0)
            self.assertNotEqual(order.correlation_id, "old-correlation")
            self.assertEqual(order.last_error, "")
            self.assertIsNotNone(order.retrieve_at)
            self.assertIsNotNone(order.second_retrieve_at)
            event = db.scalar(select(OrderEvent).where(OrderEvent.order_id == order.id))
            self.assertEqual(event.message, "Reprocessamento manual solicitado")


if __name__ == "__main__":
    unittest.main()
