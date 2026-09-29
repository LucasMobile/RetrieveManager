import unittest
from datetime import date, datetime, timedelta
from unittest.mock import patch

from pydicom.dataset import Dataset
from sqlalchemy import func, select
from sqlalchemy.exc import DataError

from app.dicom_net import FindResult, MoveResult
from app.models import (
    HistoricalSeries,
    HistoricalStudy,
    ManualMoveRequest,
    Order,
    OrderEvent,
    Unit,
)
from app.pipeline.find import _find_one, find_pending, prior_date_range
from app.pipeline.move import (
    _run_move,
    _run_prior_move,
    claim_due_moves,
    fail_claimed_move,
    run_claimed_move,
)
from app.pipeline.orders import recover_stale_locks
from tests.support import DatabaseTestCase, make_unit


def response(**values) -> Dataset:
    ds = Dataset()
    for keyword, value in values.items():
        setattr(ds, keyword, "" if value is None else value)
    return ds


def find_result(*responses: dict) -> FindResult:
    """Successful C-FIND with the given pending responses."""
    return FindResult(True, tuple(response(**values) for values in responses), 0)


def study_find_output(*responses: dict) -> FindResult:
    """STUDY responses carrying the order's identity unless overridden."""
    return find_result(
        *(
            {
                "AccessionNumber": "accession",
                "PatientID": "30211738",
                "PatientBirthDate": "19691027",
                **values,
            }
            for values in responses
        )
    )


MOVE_OK = MoveResult(True, 0x0000, completed=1)


def move_failed(error: str = "falha de rede") -> MoveResult:
    return MoveResult(False, error=error)


class PriorRetrieveTest(DatabaseTestCase):
    def setUp(self):
        super().setUp()
        # No test may reach a real PACS: the complementary SERIES query
        # defaults to "no series"; tests that need series patch it again.
        for module in ("find", "move"):
            series = patch(
                f"app.pipeline.{module}.find_study_series",
                return_value=FindResult(True),
            )
            series.start()
            self.addCleanup(series.stop)

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
            output = study_find_output(
                {
                    "StudyInstanceUID": "1.2.3.current",
                    "ModalitiesInStudy": "MR",
                    "PatientName": "PACIENTE^TESTE",
                    "BodyPartExamined": "ABDOMEN",
                }
            )
            first_at = found_at + timedelta(minutes=15)

            with (
                patch("app.pipeline.find.find_study", return_value=output),
                patch(
                    "app.pipeline.find.schedule_from_now",
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

    def test_find_skips_sr_series_and_uses_next_clinical_modality(self):
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
            found_at = datetime(2026, 9, 14, 14, 0)
            study_output = study_find_output(
                {
                    "StudyInstanceUID": "1.2.3.current",
                    "ModalitiesInStudy": "SR",
                    "PatientName": "PACIENTE^TESTE",
                    "BodyPartExamined": "CHEST",
                }
            )
            series_output = find_result(
                {"Modality": "SR", "BodyPartExamined": "CHEST"},
                {"Modality": "MR", "BodyPartExamined": "ABDOMEN"},
                {"Modality": "PR"},
            )
            first_at = found_at + timedelta(minutes=15)

            with (
                patch("app.pipeline.find.find_study", return_value=study_output),
                patch(
                    "app.pipeline.find.find_study_series",
                    return_value=series_output,
                ),
                patch(
                    "app.pipeline.find.schedule_from_now",
                    return_value=("MR", first_at, None),
                ) as schedule,
            ):
                _find_one(db, unit, order, found_at)

            schedule.assert_called_once_with(db, "MR")
            self.assertEqual(order.modality, "MR")
            self.assertEqual(order.body_part, "ABDOMEN")
            self.assertEqual(order.status, "wait_retrieve")

    def test_find_keeps_watching_when_only_discarded_series_exist(self):
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
            )
            db.add(order)
            db.commit()
            study_output = study_find_output(
                {"StudyInstanceUID": "1.2.3.current", "ModalitiesInStudy": "SR"}
            )
            series_output = find_result(
                {"Modality": "SR"},
                {"Modality": "PR"},
            )

            with (
                patch("app.pipeline.find.find_study", return_value=study_output),
                patch(
                    "app.pipeline.find.find_study_series",
                    return_value=series_output,
                ),
                patch("app.pipeline.find.schedule_from_now") as schedule,
            ):
                _find_one(db, unit, order, datetime.now())

            schedule.assert_not_called()
            self.assertEqual(order.status, "watching")
            self.assertEqual(order.study_uid, "")
            self.assertEqual(order.modality, "")
            self.assertIn("modalidade clínica válida", order.events[-1].message)

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
            output = study_find_output(
                {
                    "StudyInstanceUID": "1.2.3.current",
                    "ModalitiesInStudy": "CT",
                    "PatientName": "PACIENTE^TESTE",
                    "BodyPartExamined": None,
                }
            )
            series_output = find_result(
                {"BodyPartExamined": None},
                {"BodyPartExamined": "ABDOMEN"},
            )

            with (
                patch("app.pipeline.find.find_study", return_value=output),
                patch(
                    "app.pipeline.find.find_study_series",
                    return_value=series_output,
                ) as series_find,
                patch(
                    "app.pipeline.find.schedule_from_now",
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
            output = study_find_output(
                {
                    "StudyInstanceUID": "1.2.3.current",
                    "ModalitiesInStudy": "MR",
                    "PatientName": "P" * 300,
                    "BodyPartExamined": "B" * 100,
                }
            )

            with (
                patch("app.pipeline.find.find_study", return_value=output),
                patch(
                    "app.pipeline.find.schedule_from_now",
                    return_value=("MR", datetime.now(), None),
                ),
            ):
                _find_one(db, unit, order, datetime.now())

            self.assertEqual(order.modality, "MR")
            self.assertEqual(len(order.patient_name), 255)
            self.assertEqual(len(order.body_part), 64)

    def _watching_order(self, db, **unit_values):
        unit = self._unit()
        # Several subtests share one database; unit names and ports are unique.
        sequence = db.scalar(select(func.count()).select_from(Unit)) + 1
        unit.name = f"unit-{sequence}"
        unit.store_port = 10000 + sequence
        for key, value in unit_values.items():
            setattr(unit, key, value)
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
        return unit, order

    def _run_find(self, db, unit, order, output):
        with (
            patch("app.pipeline.find.find_study", return_value=output),
            patch(
                "app.pipeline.find.find_study_series", return_value=FindResult(True)
            ) as series,
            patch(
                "app.pipeline.find.schedule_from_now",
                return_value=("MR", datetime.now(), None),
            ) as schedule,
        ):
            _find_one(db, unit, order, datetime.now())
        return series, schedule

    def test_find_blocks_order_when_accession_returns_two_studies(self):
        with self.Session() as db:
            unit, order = self._watching_order(db)
            output = study_find_output(
                {"StudyInstanceUID": "1.2.study.a", "ModalitiesInStudy": "MR"},
                {"StudyInstanceUID": "1.2.study.b", "ModalitiesInStudy": "MR"},
            )
            series, schedule = self._run_find(db, unit, order, output)

            series.assert_not_called()
            schedule.assert_not_called()
            self.assertEqual(order.status, "error")
            self.assertEqual(order.study_uid, "")
            self.assertIn("2 estudos diferentes", order.last_error)
            self.assertEqual(order.events[-1].level, "error")

    def test_find_accepts_repeated_responses_for_the_same_study(self):
        with self.Session() as db:
            unit, order = self._watching_order(db)
            response = {"StudyInstanceUID": "1.2.3.current", "ModalitiesInStudy": "MR"}
            self._run_find(db, unit, order, study_find_output(response, response))
            self.assertEqual(order.status, "wait_retrieve")
            self.assertEqual(order.study_uid, "1.2.3.current")

    def test_find_blocks_study_whose_returned_identity_differs_from_order(self):
        cases = {
            "PatientID": {"PatientID": "99999999"},
            "PatientBirthDate": {"PatientBirthDate": "19700101"},
            "AccessionNumber": {"AccessionNumber": "other-accession"},
        }
        for field, override in cases.items():
            with self.subTest(field=field), self.Session() as db:
                unit, order = self._watching_order(db)
                output = study_find_output(
                    {
                        "StudyInstanceUID": "1.2.3.current",
                        "ModalitiesInStudy": "MR",
                        **override,
                    }
                )
                _series, schedule = self._run_find(db, unit, order, output)

                schedule.assert_not_called()
                self.assertEqual(order.status, "error")
                self.assertIn(field, order.last_error)
                self.assertNotIn("99999999", order.last_error)

    def test_find_rejects_missing_patient_id_in_pacs_response(self):
        with self.Session() as db:
            unit, order = self._watching_order(db)
            output = study_find_output(
                {
                    "StudyInstanceUID": "1.2.3.current",
                    "ModalitiesInStudy": "MR",
                    "PatientID": None,
                }
            )
            self._run_find(db, unit, order, output)
            self.assertEqual(order.status, "error")
            self.assertIn("PatientID", order.last_error)

    def test_find_patient_id_suffix_requires_unit_opt_in(self):
        suffixed = {
            "StudyInstanceUID": "1.2.3.current",
            "ModalitiesInStudy": "MR",
            "PatientID": "30211738-1",
        }
        with self.Session() as db:
            unit, order = self._watching_order(db)
            self._run_find(db, unit, order, study_find_output(suffixed))
            self.assertEqual(order.status, "error")
        with self.Session() as db:
            unit, order = self._watching_order(db, pacs_patient_id_wildcard=True)
            self._run_find(db, unit, order, study_find_output(suffixed))
            self.assertEqual(order.status, "wait_retrieve")
            self.assertEqual(order.study_uid, "1.2.3.current")

    def test_prior_find_is_exact_and_ignores_series_from_other_patients(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = self._order(unit.id, prior_status="retrieving")
            db.add(order)
            db.commit()
            output = find_result(
                {
                    "PatientID": "30211738",
                    "PatientBirthDate": "19691027",
                    "StudyDate": "20250110",
                    "StudyInstanceUID": "1.2.own",
                    "SeriesInstanceUID": "1.2.own.series",
                },
                {
                    "PatientID": "302117389",
                    "PatientBirthDate": "19691027",
                    "StudyDate": "20250110",
                    "StudyInstanceUID": "1.2.prefix",
                    "SeriesInstanceUID": "1.2.prefix.series",
                },
                {
                    "PatientID": "30211738",
                    "PatientBirthDate": "19700101",
                    "StudyDate": "20250110",
                    "StudyInstanceUID": "1.2.birth",
                    "SeriesInstanceUID": "1.2.birth.series",
                },
                {
                    "StudyDate": "20250110",
                    "StudyInstanceUID": "1.2.anonymous",
                    "SeriesInstanceUID": "1.2.anonymous.series",
                },
            )
            with (
                patch(
                    "app.pipeline.move.find_prior_series", return_value=output
                ) as find,
                patch("app.pipeline.move.move_series", return_value=MOVE_OK) as move,
            ):
                _run_prior_move(db, unit, order)

            self.assertFalse(find.call_args.kwargs["patient_id_wildcard"])
            move.assert_called_once()
            self.assertEqual(move.call_args.args[2], "1.2.own.series")
            self.assertEqual(
                list(db.scalars(select(HistoricalStudy.study_uid))), ["1.2.own"]
            )
            self.assertEqual(order.prior_status, "done")
            self.assertIn(
                "3 série(s) com identificação divergente", order.events[-1].message
            )

    def test_prior_wildcard_option_is_forwarded_and_accepts_suffixed_id(self):
        with self.Session() as db:
            unit = self._unit()
            unit.pacs_patient_id_wildcard = True
            db.add(unit)
            db.flush()
            order = self._order(unit.id, prior_status="retrieving")
            db.add(order)
            db.commit()
            output = find_result(
                {
                    "PatientID": "30211738-1",
                    "PatientBirthDate": "19691027",
                    "StudyDate": "20250110",
                    "StudyInstanceUID": "1.2.suffixed",
                    "SeriesInstanceUID": "1.2.suffixed.series",
                },
                {
                    "PatientID": "3021173",
                    "PatientBirthDate": "19691027",
                    "StudyDate": "20250110",
                    "StudyInstanceUID": "1.2.shorter",
                    "SeriesInstanceUID": "1.2.shorter.series",
                },
            )
            with (
                patch(
                    "app.pipeline.move.find_prior_series", return_value=output
                ) as find,
                patch("app.pipeline.move.move_series", return_value=MOVE_OK) as move,
            ):
                _run_prior_move(db, unit, order)

            self.assertTrue(find.call_args.kwargs["patient_id_wildcard"])
            move.assert_called_once()
            self.assertEqual(move.call_args.args[2], "1.2.suffixed.series")

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

            with patch(
                "app.pipeline.find._find_one", side_effect=[failure, None]
            ) as find:
                find_pending(db, unit)

            self.assertEqual(find.call_count, 2)
            db.refresh(first)
            self.assertEqual(first.status, "error")
            self.assertIn("banco de dados", first.last_error)

    def test_find_command_failure_is_retried_and_not_classified_as_not_found(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = self._order(
                unit.id, status="watching", study_uid="", prior_status="disabled"
            )
            db.add(order)
            db.commit()

            with (
                patch("app.pipeline.find.FIND_TIMEOUT_SECONDS", 7),
                patch(
                    "app.pipeline.find.find_study",
                    return_value=FindResult(False, error="tempo esgotado"),
                ) as find,
            ):
                find_pending(db, unit)

            self.assertEqual(find.call_args.kwargs["timeout"], 7)

            self.assertEqual(order.status, "watching")
            self.assertEqual(order.attempts, 0)
            self.assertIn("tempo esgotado", order.last_error)
            event = db.scalar(
                select(OrderEvent)
                .where(OrderEvent.order_id == order.id)
                .order_by(OrderEvent.id.desc())
            )
            self.assertIn("será repetida", event.message)

    def test_current_move_failure_uses_bounded_retry(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = self._order(
                unit.id,
                status="retrieving",
                prior_status="disabled",
                attempts=0,
            )
            db.add(order)
            db.commit()

            with patch("app.pipeline.move.move_study", return_value=move_failed()):
                _run_move(db, unit, order, second=False)

            self.assertEqual(order.status, "wait_retrieve")
            self.assertEqual(order.attempts, 1)
            self.assertGreater(order.retrieve_at, datetime.now())

    def test_unexpected_claimed_move_failure_is_persisted_immediately(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = self._order(
                unit.id,
                status="retrieving",
                prior_status="disabled",
                attempts=1,
            )
            db.add(order)
            db.commit()

            fail_claimed_move(db, order.id, "first", RuntimeError("boom"))

            self.assertEqual(order.status, "wait_retrieve")
            self.assertIn("Falha interna", order.last_error)

    def test_current_and_prior_can_progress_in_parallel_when_unit_has_capacity(self):
        with self.Session() as db:
            unit = self._unit(max_parallel_moves=2)
            db.add(unit)
            db.flush()
            order = self._order(unit.id)
            db.add(order)
            db.commit()

            self.assertEqual(
                claim_due_moves(db, unit),
                [(order.id, "first"), (order.id, "prior")],
            )
            self.assertEqual(order.prior_status, "retrieving")
            self.assertEqual(order.status, "retrieving")

            self.assertEqual(claim_due_moves(db, unit), [])

    def test_prior_failure_retries_without_changing_current_status(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = self._order(unit.id, prior_status="retrieving")
            db.add(order)
            db.commit()

            with patch(
                "app.pipeline.move.find_prior_series",
                return_value=FindResult(False, error="falha"),
            ):
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
                patch(
                    "app.pipeline.move.find_prior_series", return_value=FindResult(True)
                ),
                patch("app.pipeline.move.move_series") as move,
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
            output = find_result(
                {
                    "PatientID": "30211738",
                    "PatientBirthDate": "19691027",
                    "StudyDate": "20250110",
                    "AccessionNumber": "OLD-1",
                    "Modality": "MR",
                    "BodyPartExamined": "ABDOMEN",
                    "StudyInstanceUID": "1.2.old",
                    "SeriesInstanceUID": "1.2.old.series.1",
                },
                {
                    "PatientID": "30211738",
                    "PatientBirthDate": "19691027",
                    "StudyDate": "20250110",
                    "Modality": "MR",
                    "StudyInstanceUID": "1.2.old",
                    "SeriesInstanceUID": "1.2.old.series.2",
                },
            )

            with (
                patch("app.pipeline.move.find_prior_series", return_value=output),
                patch("app.pipeline.move.move_series", return_value=MOVE_OK) as move,
            ):
                _run_prior_move(db, unit, order)

            self.assertEqual(move.call_count, 2)
            study = db.scalar(select(HistoricalStudy))
            self.assertIsNotNone(study)
            self.assertEqual(study.study_uid, "1.2.old")
            self.assertEqual(study.accession, "OLD-1")
            self.assertEqual(order.prior_status, "done")
            completed_series = list(
                db.scalars(
                    select(HistoricalSeries).where(HistoricalSeries.status == "done")
                )
            )
            self.assertEqual(len(completed_series), 2)

    def test_prior_retry_skips_series_already_completed(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = self._order(unit.id, prior_status="retrieving")
            db.add(order)
            db.commit()
            output = find_result(
                {
                    "PatientID": "30211738",
                    "PatientBirthDate": "19691027",
                    "StudyDate": "20250110",
                    "Modality": "MR",
                    "StudyInstanceUID": "1.2.old",
                    "SeriesInstanceUID": "1.2.old.series.1",
                },
                {
                    "PatientID": "30211738",
                    "PatientBirthDate": "19691027",
                    "StudyDate": "20250110",
                    "Modality": "MR",
                    "StudyInstanceUID": "1.2.old",
                    "SeriesInstanceUID": "1.2.old.series.2",
                },
            )
            with (
                patch("app.pipeline.move.find_prior_series", return_value=output),
                patch(
                    "app.pipeline.move.move_series",
                    side_effect=[MOVE_OK, move_failed()],
                ),
            ):
                _run_prior_move(db, unit, order)
            self.assertEqual(order.prior_status, "retry_wait")

            order.prior_status = "retrieving"
            db.commit()
            with (
                patch("app.pipeline.move.find_prior_series", return_value=output),
                patch("app.pipeline.move.move_series", return_value=MOVE_OK) as move,
            ):
                _run_prior_move(db, unit, order)

            move.assert_called_once()
            self.assertEqual(move.call_args.args[2], "1.2.old.series.2")
            self.assertEqual(order.prior_status, "done")

    def test_prior_find_discards_current_study_outside_history_range(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            order = self._order(unit.id, prior_status="retrieving")
            db.add(order)
            db.commit()
            output = find_result(
                {
                    "PatientID": "30211738",
                    "PatientBirthDate": "19691027",
                    "StudyDate": "20260913",
                    "Modality": "MR",
                    "StudyInstanceUID": "1.2.3.current",
                    "SeriesInstanceUID": "1.2.current.series",
                },
            )

            with (
                patch("app.pipeline.move.find_prior_series", return_value=output),
                patch("app.pipeline.move.move_series") as move,
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

            self.assertEqual(claim_due_moves(db, unit), [(move_request.id, "manual")])
            self.assertEqual(order.prior_status, "queued")
            with patch("app.pipeline.move.move_study", return_value=MOVE_OK) as move:
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
