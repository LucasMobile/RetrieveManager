import unittest
from datetime import datetime, timedelta

from sqlalchemy import select

from app.main import (
    _delete_order_record,
    _reset_order_for_reprocess,
)
from app.models import (
    AuditLog,
    ImageTransfer,
    ModalityRule,
    Order,
    OrderEvent,
    Unit,
)
from app.order_state import ACTIVE_ORDER_STATUSES
from app.pipeline import archive_completed_orders, cleanup_unmatched_orders
from tests.support import DatabaseTestCase, make_unit


class OrderActionsTest(DatabaseTestCase):
    @staticmethod
    def _unit() -> Unit:
        return make_unit(
            orders_api_url="https://integracao.example/v1/pedidos",
            orders_api_token="integration-token",
            pacs_port=2104,
            store_port=444,
            receive_dir="/receive",
            send_dir="/send",
            error_dir="/error",
            token="token",
        )

    def test_delete_archives_and_preserves_history_and_transfer(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = Order(
                unit_id=unit.id,
                source_id="order-id",
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

            archived = db.get(Order, order_id)
            self.assertIsNotNone(archived.archived_at)
            self.assertEqual(archived.archive_reason, "Pedido arquivado manualmente.")
            events = list(
                db.scalars(select(OrderEvent).where(OrderEvent.order_id == order_id))
            )
            self.assertEqual(
                [event.message for event in events],
                ["done", "Pedido arquivado"],
            )
            preserved = db.get(ImageTransfer, transfer_id)
            self.assertIsNotNone(preserved)
            self.assertEqual(preserved.order_id, order_id)

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
                source_id="order-id",
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

    def test_cleanup_archives_only_old_orders_not_found_by_cfind(self):
        now = datetime(2026, 9, 12, 12, 0, 0)
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            eligible = Order(
                unit_id=unit.id,
                acc="eligible",
                birth_date="20000101",
                status="watching",
                study_uid="",
                attempts=1,
                last_find_at=now - timedelta(minutes=5),
                created_at=now - timedelta(days=1, seconds=1),
            )
            protected = [
                Order(
                    unit_id=unit.id,
                    acc="recent",
                    birth_date="20000101",
                    status="watching",
                    study_uid="",
                    attempts=1,
                    last_find_at=now - timedelta(minutes=5),
                    created_at=now - timedelta(hours=23),
                ),
                Order(
                    unit_id=unit.id,
                    acc="never-searched",
                    birth_date="20000101",
                    status="watching",
                    study_uid="",
                    attempts=0,
                    last_find_at=None,
                    created_at=now - timedelta(days=2),
                ),
                Order(
                    unit_id=unit.id,
                    acc="found",
                    birth_date="20000101",
                    status="wait_retrieve",
                    study_uid="1.2.3",
                    attempts=1,
                    last_find_at=now - timedelta(days=1),
                    created_at=now - timedelta(days=2),
                ),
                Order(
                    unit_id=unit.id,
                    acc="technical-error",
                    birth_date="20000101",
                    status="error",
                    study_uid="",
                    attempts=1,
                    last_find_at=now - timedelta(days=1),
                    created_at=now - timedelta(days=2),
                ),
                Order(
                    unit_id=unit.id,
                    acc="technical-retry",
                    birth_date="20000101",
                    status="watching",
                    study_uid="",
                    attempts=0,
                    last_find_at=now - timedelta(days=1),
                    last_error="C-FIND falhou (exit 124)",
                    created_at=now - timedelta(days=2),
                ),
            ]
            db.add_all([eligible, *protected])
            db.flush()
            eligible_id = eligible.id
            db.add(OrderEvent(order_id=eligible_id, message="C-FIND sem resultado"))
            transfer = ImageTransfer(
                unit_id=unit.id,
                order_id=eligible_id,
                filename="preserved.dcm",
                correlation_id="cleanup-test",
                status="uploaded",
            )
            db.add(transfer)
            db.commit()
            transfer_id = transfer.id

            self.assertEqual(cleanup_unmatched_orders(db, now), 1)
            archived = db.get(Order, eligible_id)
            self.assertIsNotNone(archived.archived_at)
            self.assertIn("C-FIND", archived.archive_reason)
            self.assertEqual(
                {
                    order.acc
                    for order in db.scalars(
                        select(Order).where(Order.archived_at.is_(None))
                    )
                },
                {
                    "recent",
                    "never-searched",
                    "found",
                    "technical-error",
                    "technical-retry",
                },
            )
            self.assertEqual(db.get(ImageTransfer, transfer_id).order_id, eligible_id)
            self.assertEqual(
                [
                    event.message
                    for event in db.scalars(
                        select(OrderEvent).where(OrderEvent.order_id == eligible_id)
                    )
                ],
                ["C-FIND sem resultado", "Pedido arquivado"],
            )
            audit = db.scalar(
                select(AuditLog).where(AuditLog.resource_id == str(eligible_id))
            )
            self.assertEqual(audit.actor_username, "Sistema")
            self.assertIn("24 horas", audit.summary)

    def test_completed_orders_are_archived_after_two_weeks(self):
        now = datetime(2026, 9, 12, 12, 0, 0)
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            eligible = Order(
                unit_id=unit.id,
                acc="completed-old",
                birth_date="20000101",
                status="done",
                done_at=now - timedelta(days=14, seconds=1),
            )
            protected = [
                Order(
                    unit_id=unit.id,
                    acc="completed-recent",
                    birth_date="20000101",
                    status="done",
                    done_at=now - timedelta(days=13),
                ),
                Order(
                    unit_id=unit.id,
                    acc="prior-error",
                    birth_date="20000101",
                    status="done",
                    done_at=now - timedelta(days=30),
                    prior_status="error",
                ),
                Order(
                    unit_id=unit.id,
                    acc="missing-completion-date",
                    birth_date="20000101",
                    status="done",
                    done_at=None,
                ),
                Order(
                    unit_id=unit.id,
                    acc="still-processing",
                    birth_date="20000101",
                    status="receiving",
                    done_at=now - timedelta(days=30),
                ),
            ]
            db.add_all([eligible, *protected])
            db.commit()
            eligible_id = eligible.id

            self.assertEqual(archive_completed_orders(db, now), 1)
            archived = db.get(Order, eligible_id)
            self.assertEqual(archived.archived_at, now)
            self.assertIn("14 dias", archived.archive_reason)
            self.assertEqual(
                {
                    order.acc
                    for order in db.scalars(
                        select(Order).where(Order.archived_at.is_(None))
                    )
                },
                {
                    "completed-recent",
                    "prior-error",
                    "missing-completion-date",
                    "still-processing",
                },
            )
            event = db.scalar(
                select(OrderEvent).where(OrderEvent.order_id == eligible_id)
            )
            self.assertEqual(event.message, "Pedido arquivado")
            audit = db.scalar(
                select(AuditLog).where(AuditLog.resource_id == str(eligible_id))
            )
            self.assertIn("14 dias", audit.summary)


if __name__ == "__main__":
    unittest.main()
