import unittest
from datetime import date, datetime, timedelta
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.models import Base, Order, OrderEvent, Unit
from app.pipeline import (
    _find_one,
    _run_prior_move,
    claim_due_moves,
    prior_date_range,
    recover_stale_locks,
)


class PriorRetrieveTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def tearDown(self):
        self.engine.dispose()

    @staticmethod
    def _unit(max_parallel_moves=1):
        return Unit(
            name="unit",
            orders_api_url="https://integracao.example/v1/pedidos",
            orders_api_token="integration-token",
            pacs_aet="PACS",
            pacs_ip="127.0.0.1",
            pacs_port=2104,
            calling_aet="RETRIEVE",
            store_port=444,
            receive_dir="/receive",
            send_dir="/send",
            error_dir="/error",
            token="token",
            retrieve_prior_enabled=True,
            move_timeout_prior=1800,
            max_parallel_moves=max_parallel_moves,
        )

    @staticmethod
    def _order(unit_id, **values):
        defaults = {
            "unit_id": unit_id,
            "source_id": "source",
            "acc": "accession",
            "pat_id": "30211738",
            "birth_date": "19691027",
            "study_uid": "1.2.3.current",
            "modality": "MR",
            "body_part": "ABDOMEN",
            "status": "wait_retrieve",
            "retrieve_at": datetime.now() - timedelta(minutes=1),
            "prior_status": "queued",
            "prior_due_at": datetime.now() - timedelta(minutes=2),
            "prior_date_from": "20230911",
            "prior_date_to": "20260910",
        }
        defaults.update(values)
        return Order(**defaults)

    def test_three_calendar_year_range_and_leap_day(self):
        self.assertEqual(
            prior_date_range(date(2026, 9, 11)),
            ("20230911", "20260910"),
        )
        self.assertEqual(
            prior_date_range(date(2024, 2, 29)),
            ("20210228", "20240228"),
        )

    def test_find_saves_body_part_and_queues_prior_move(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = self._order(
                unit.id,
                status="watching",
                study_uid="",
                modality="",
                body_part="",
                prior_status="disabled",
                prior_due_at=None,
            )
            db.add(order)
            db.commit()
            found_at = datetime(2026, 9, 11, 10, 30)
            output = """
(0020,000d) UI [1.2.3.current] # StudyInstanceUID
(0008,0061) CS [MR] # ModalitiesInStudy
(0010,0010) PN [PACIENTE^TESTE] # PatientName
(0018,0015) CS [ABDOMEN] # BodyPartExamined
"""
            first_at = found_at + timedelta(minutes=15)

            with (
                patch("app.pipeline.c_find", return_value=(0, output)),
                patch(
                    "app.pipeline.schedule_from_now",
                    return_value=("MR", first_at, None),
                ),
            ):
                _find_one(db, unit, order, found_at)

            self.assertEqual(order.study_uid, "1.2.3.current")
            self.assertEqual(order.modality, "MR")
            self.assertEqual(order.body_part, "ABDOMEN")
            self.assertEqual(order.status, "wait_retrieve")
            self.assertEqual(order.prior_status, "queued")
            self.assertEqual(order.prior_date_from, "20230911")
            self.assertEqual(order.prior_date_to, "20260910")
            self.assertEqual(order.prior_due_at, found_at)

    def test_prior_is_claimed_before_current_and_holds_its_order(self):
        with self.Session() as db:
            unit = self._unit(max_parallel_moves=2)
            db.add(unit)
            db.flush()
            order = self._order(unit.id)
            db.add(order)
            db.commit()

            self.assertEqual(claim_due_moves(db, unit), [(order.id, "prior")])
            self.assertEqual(order.prior_status, "retrieving")
            self.assertEqual(order.status, "wait_retrieve")

            self.assertEqual(claim_due_moves(db, unit), [])

    def test_prior_failure_retries_without_changing_current_status(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = self._order(unit.id, prior_status="retrieving")
            db.add(order)
            db.commit()

            with patch("app.pipeline.c_move_prior", return_value=(2, "failed")):
                _run_prior_move(db, unit, order)

            self.assertEqual(order.status, "wait_retrieve")
            self.assertEqual(order.prior_status, "retry_wait")
            self.assertEqual(order.prior_attempts, 1)
            self.assertIsNotNone(order.prior_due_at)
            event = db.scalar(
                select(OrderEvent)
                .where(OrderEvent.order_id == order.id)
                .order_by(OrderEvent.id.desc())
            )
            self.assertIn("nova tentativa", event.message)

    def test_prior_lock_respects_configured_move_timeout(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = self._order(
                unit.id,
                prior_status="retrieving",
                prior_heartbeat_at=datetime.now() - timedelta(minutes=25),
            )
            db.add(order)
            db.commit()

            recover_stale_locks(db)
            self.assertEqual(order.prior_status, "retrieving")

            order.prior_heartbeat_at = datetime.now() - timedelta(minutes=33)
            db.commit()
            recover_stale_locks(db)

            self.assertEqual(order.prior_status, "queued")
            self.assertEqual(
                order.prior_last_error,
                "lock órfão do histórico recuperado",
            )


if __name__ == "__main__":
    unittest.main()
