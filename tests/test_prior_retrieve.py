import unittest
from datetime import date, datetime, timedelta
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.exc import DataError

from app.models import HistoricalStudy, ManualMoveRequest, Order, OrderEvent
from app.pipeline import (
    _find_one,
    _run_prior_move,
    claim_due_moves,
    find_pending,
    prior_date_range,
    recover_stale_locks,
    run_claimed_move,
)
from tests.support import DatabaseTestCase, make_unit


class PriorRetrieveTest(DatabaseTestCase):
    @staticmethod
    def _unit(max_parallel_moves=1):
        return make_unit(
            orders_api_url="https://integracao.example/v1/pedidos",
            orders_api_token="integration-token",
            pacs_port=2104,
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

    def test_find_uses_another_series_when_first_body_part_is_empty(self):
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
            output = """
(0020,000d) UI [1.2.3.current] # StudyInstanceUID
(0008,0061) CS [CT] # ModalitiesInStudy
(0010,0010) PN [PACIENTE^TESTE] # PatientName
(0018,0015) CS (no value available) # BodyPartExamined
"""
            series_output = """
Find Response: 1 (Pending)
(0018,0015) CS (no value available) # BodyPartExamined
Find Response: 2 (Pending)
(0018,0015) CS [ABDOMEN] # BodyPartExamined
"""

            with (
                patch("app.pipeline.c_find", return_value=(0, output)),
                patch(
                    "app.pipeline.c_find_series_body_part",
                    return_value=(0, series_output),
                ) as series_find,
                patch(
                    "app.pipeline.schedule_from_now",
                    return_value=("CT", datetime.now(), None),
                ),
            ):
                _find_one(db, unit, order, datetime.now())

            series_find.assert_called_once()
            self.assertEqual(order.body_part, "ABDOMEN")

    def test_find_bounds_pacs_metadata_to_postgres_columns(self):
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
            output = (
                "(0020,000d) UI [1.2.3.current] # StudyInstanceUID\n"
                "(0008,0061) CS [UNKNOWNMODALITY] # ModalitiesInStudy\n"
                f"(0010,0010) PN [{'P' * 300}] # PatientName\n"
                f"(0018,0015) CS [{'B' * 100}] # BodyPartExamined\n"
            )

            with (
                patch("app.pipeline.c_find", return_value=(0, output)),
                patch(
                    "app.pipeline.schedule_from_now",
                    return_value=("UNKNOWNMODALITY" * 3, datetime.now(), None),
                ),
            ):
                _find_one(db, unit, order, datetime.now())

            self.assertEqual(len(order.modality), 32)
            self.assertEqual(len(order.patient_name), 255)
            self.assertEqual(len(order.body_part), 64)

    def test_database_error_in_one_find_does_not_block_the_next_order(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            first = self._order(
                unit.id,
                acc="first",
                status="watching",
                study_uid="",
                prior_status="disabled",
            )
            second = self._order(
                unit.id,
                acc="second",
                status="watching",
                study_uid="",
                prior_status="disabled",
            )
            db.add_all([first, second])
            db.commit()
            failure = DataError(
                "INSERT",
                {},
                ValueError("value too long for type character varying(64)"),
                False,
            )

            with patch("app.pipeline._find_one", side_effect=[failure, None]) as find:
                find_pending(db, unit)

            self.assertEqual(find.call_count, 2)
            db.refresh(first)
            self.assertEqual(first.status, "error")
            self.assertIn("banco de dados", first.last_error)

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

            with patch("app.pipeline.c_find_prior", return_value=(2, "failed")):
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

    def test_empty_prior_find_finishes_without_attempting_move(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = self._order(unit.id, prior_status="retrieving")
            db.add(order)
            db.commit()

            with (
                patch("app.pipeline.c_find_prior", return_value=(0, "success")),
                patch("app.pipeline.c_move_prior_series") as move,
            ):
                _run_prior_move(db, unit, order)

            move.assert_not_called()
            self.assertEqual(order.prior_status, "done")
            self.assertEqual(order.prior_last_error, "")
            event = db.scalar(
                select(OrderEvent)
                .where(OrderEvent.order_id == order.id)
                .order_by(OrderEvent.id.desc())
            )
            self.assertIn("nenhum exame anterior", event.message)

    def test_prior_find_registers_study_and_moves_each_exact_series(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = self._order(unit.id, prior_status="retrieving")
            db.add(order)
            db.commit()
            output = """
Find Response: 1 (Pending)
(0008,0020) DA [20250110] # StudyDate
(0008,0050) SH [OLD-1] # AccessionNumber
(0008,0060) CS [MR] # Modality
(0018,0015) CS [ABDOMEN] # BodyPartExamined
(0020,000d) UI [1.2.old] # StudyInstanceUID
(0020,000e) UI [1.2.old.series.1] # SeriesInstanceUID
Find Response: 2 (Pending)
(0008,0020) DA [20250110] # StudyDate
(0008,0060) CS [MR] # Modality
(0020,000d) UI [1.2.old] # StudyInstanceUID
(0020,000e) UI [1.2.old.series.2] # SeriesInstanceUID
Received Final Find Response (Success)
"""

            with (
                patch("app.pipeline.c_find_prior", return_value=(0, output)),
                patch(
                    "app.pipeline.c_move_prior_series", return_value=(0, "success")
                ) as move,
            ):
                _run_prior_move(db, unit, order)

            self.assertEqual(move.call_count, 2)
            study = db.scalar(select(HistoricalStudy))
            self.assertIsNotNone(study)
            self.assertEqual(study.study_uid, "1.2.old")
            self.assertEqual(study.accession, "OLD-1")
            self.assertEqual(order.prior_status, "done")

    def test_prior_find_discards_current_study_outside_history_range(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = self._order(unit.id, prior_status="retrieving")
            db.add(order)
            db.commit()
            output = """
Find Response: 1 (Pending)
(0008,0020) DA [20260913] # StudyDate
(0008,0060) CS [MR] # Modality
(0020,000d) UI [1.2.3.current] # StudyInstanceUID
(0020,000e) UI [1.2.current.series] # SeriesInstanceUID
Received Final Find Response (Success)
"""

            with (
                patch("app.pipeline.c_find_prior", return_value=(0, output)),
                patch("app.pipeline.c_move_prior_series") as move,
            ):
                _run_prior_move(db, unit, order)

            move.assert_not_called()
            self.assertIsNone(db.scalar(select(HistoricalStudy)))
            self.assertEqual(order.prior_status, "done")

    def test_manual_current_move_keeps_normal_schedule_and_prior_queue(self):
        with self.Session() as db:
            unit = self._unit(max_parallel_moves=1)
            db.add(unit)
            db.flush()
            retrieve_at = datetime.now() + timedelta(minutes=15)
            order = self._order(
                unit.id,
                status="wait_retrieve",
                retrieve_at=retrieve_at,
                prior_status="queued",
                prior_due_at=datetime.now() - timedelta(minutes=1),
            )
            db.add(order)
            db.flush()
            move_request = ManualMoveRequest(
                order_id=order.id,
                unit_id=unit.id,
                requested_by_username="tester",
                correlation_id="manual-correlation",
                status="queued",
            )
            db.add(move_request)
            db.commit()

            self.assertEqual(
                claim_due_moves(db, unit), [(move_request.id, "manual")]
            )
            self.assertEqual(order.prior_status, "queued")
            with patch("app.pipeline.c_move", return_value=(0, "success")) as move:
                run_claimed_move(db, move_request.id, "manual")

            move.assert_called_once()
            db.refresh(move_request)
            self.assertEqual(move_request.status, "done")
            self.assertEqual(order.status, "wait_retrieve")
            self.assertEqual(order.retrieve_at, retrieve_at)
            self.assertEqual(order.prior_status, "queued")

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
