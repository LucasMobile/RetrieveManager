import logging
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import select

from app.compaction import CompactResult
from app.dicom_rules import RuleMatch
from app.models import (
    AuditLog,
    DicomRule,
    DicomRuleApplication,
    HistoricalImageLink,
    HistoricalStudy,
    ImageTransfer,
    ModalityRule,
    Order,
    OrderEvent,
    UnitPriorModality,
)
from app.pipeline.records import _record_compact_result
from app.pipeline.send import CircuitState, SendResult, _record_send_results
from tests.support import DatabaseTestCase, make_unit


class TransferTrackingTest(DatabaseTestCase):
    def _add_retrieve_rules(self, db) -> None:
        db.add_all(
            [
                ModalityRule(modality="CT", wait_minutes=10, monitor_enabled=True),
                ModalityRule(modality="*", wait_minutes=10, monitor_enabled=False),
            ]
        )
        db.flush()

    def test_success_is_persisted_before_local_file_cleanup(self):
        with tempfile.TemporaryDirectory() as send_dir, self.Session() as db:
            unit = make_unit(name="unit", send_dir=send_dir)
            db.add(unit)
            db.flush()
            transfer = ImageTransfer(
                unit_id=unit.id,
                filename="image.dcm",
                correlation_id="corr-1",
                status="compressed",
            )
            db.add(transfer)
            db.commit()
            path = Path(send_dir) / transfer.filename
            path.write_bytes(b"DICOM")

            _record_send_results(
                db,
                unit,
                [SendResult(transfer.id, "corr-1", True, http_status=200)],
                CircuitState(),
            )

            self.assertEqual(transfer.status, "uploaded")
            self.assertFalse(path.exists())

    def test_upload_failure_is_persisted_with_backoff(self):
        with self.Session() as db:
            unit = make_unit(
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
            )
            db.add(unit)
            db.flush()
            order = Order(
                unit_id=unit.id,
                source_id="order-id",
                acc="accession",
                birth_date="20000101",
                correlation_id="corr-1",
            )
            db.add(order)
            db.flush()
            transfer = ImageTransfer(
                unit_id=unit.id,
                order_id=order.id,
                filename="image.dcm",
                correlation_id=order.correlation_id,
                status="compressed",
            )
            db.add(transfer)
            db.commit()

            logging.disable(logging.CRITICAL)
            try:
                _record_send_results(
                    db,
                    unit,
                    [
                        SendResult(
                            transfer.id,
                            order.correlation_id,
                            False,
                            http_status=503,
                            error_type="CloudHttpError",
                            duration_ms=15.0,
                        )
                    ],
                    CircuitState(),
                )
            finally:
                logging.disable(logging.NOTSET)

            db.refresh(transfer)
            self.assertEqual(transfer.status, "upload_error")
            self.assertEqual(transfer.attempts, 1)
            self.assertEqual(transfer.last_http_status, 503)
            self.assertIsNotNone(transfer.next_attempt_at)

    def test_retrieved_image_reuses_transfer_with_new_correlation(self):
        with self.Session() as db:
            unit = make_unit(
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
            )
            db.add(unit)
            db.flush()
            order = Order(
                unit_id=unit.id,
                source_id="order-id",
                acc="accession",
                birth_date="20000101",
                correlation_id="new-correlation",
                study_uid="1.2.3",
            )
            db.add(order)
            transfer = ImageTransfer(
                unit_id=unit.id,
                order_id=order.id,
                filename="image.dcm",
                correlation_id="old-correlation",
                study_uid="1.2.3",
                status="uploaded",
                attempts=2,
            )
            db.add(transfer)
            db.commit()

            _record_compact_result(
                db,
                unit,
                CompactResult("image", "image.dcm", "1.2.3", "compressed"),
            )
            db.commit()

            db.refresh(transfer)
            # Released to the sender only once the file is in send_dir.
            self.assertEqual(transfer.status, "publishing")
            self.assertEqual(transfer.correlation_id, "new-correlation")
            self.assertEqual(transfer.attempts, 0)

    def test_historical_image_is_grouped_by_study_uid(self):
        with self.Session() as db:
            unit = make_unit(
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
            )
            db.add(unit)
            db.flush()
            observed_at = datetime.now()
            order = Order(
                unit_id=unit.id,
                source_id="order-id",
                acc="current",
                pat_id="30211738",
                birth_date="19691027",
                study_uid="1.2.current",
                modality="MR",
                body_part="ABDOMEN",
                prior_status="retrieving",
                prior_date_from="20230911",
                prior_date_to="20260910",
                prior_started_at=observed_at - timedelta(seconds=5),
            )
            db.add(order)
            db.commit()

            _record_compact_result(
                db,
                unit,
                CompactResult(
                    "historical-file",
                    "historical-file.dcm",
                    "1.2.historical",
                    "compressed",
                    patient_id="30211738",
                    birth_date="19691027",
                    study_date="20250110",
                    accession="old-accession",
                    modality="MR",
                    body_part="ABDOMEN",
                    description="RM ABDOMEN",
                    observed_at=observed_at,
                ),
            )
            db.commit()

            study = db.scalar(select(HistoricalStudy))
            self.assertIsNotNone(study)
            self.assertEqual(study.order_id, order.id)
            self.assertEqual(study.study_uid, "1.2.historical")
            self.assertEqual(study.accession, "old-accession")
            link = db.scalar(select(HistoricalImageLink))
            self.assertIsNotNone(link)
            self.assertEqual(link.historical_study_id, study.id)
            self.assertEqual(len(list(db.scalars(select(Order)))), 1)

    def test_historical_patient_id_suffix_is_linked_only_with_unit_opt_in(self):
        for wildcard in (False, True):
            with self.subTest(wildcard=wildcard), self.Session() as db:
                unit = make_unit(
                    name=f"unit-{wildcard}",
                    store_port=444 + int(wildcard),
                    retrieve_prior_enabled=True,
                    pacs_patient_id_wildcard=wildcard,
                )
                db.add(unit)
                db.flush()
                observed_at = datetime.now()
                order = Order(
                    unit_id=unit.id,
                    source_id=f"order-{wildcard}",
                    acc=f"current-{wildcard}",
                    pat_id="30211738",
                    birth_date="19691027",
                    study_uid=f"1.2.current.{int(wildcard)}",
                    modality="MR",
                    body_part="ABDOMEN",
                    prior_status="retrieving",
                    prior_date_from="20230911",
                    prior_date_to="20260910",
                    prior_started_at=observed_at - timedelta(seconds=5),
                )
                db.add(order)
                db.commit()

                _record_compact_result(
                    db,
                    unit,
                    CompactResult(
                        f"historical-{wildcard}",
                        f"historical-{wildcard}.dcm",
                        f"1.2.historical.{int(wildcard)}",
                        "compressed",
                        patient_id="30211738-A",
                        birth_date="19691027",
                        study_date="20250110",
                        accession=f"old-accession-{wildcard}",
                        modality="MR",
                        body_part="ABDOMEN",
                        observed_at=observed_at,
                    ),
                )
                db.commit()

                study = db.scalar(
                    select(HistoricalStudy).where(HistoricalStudy.order_id == order.id)
                )
                self.assertEqual(study is not None, wildcard)

    def test_discovered_historical_study_is_linked_without_metadata_guessing(self):
        with self.Session() as db:
            unit = make_unit(name="unit", retrieve_prior_enabled=True)
            db.add(unit)
            db.flush()
            observed_at = datetime.now()
            order = Order(
                unit_id=unit.id,
                source_id="order-id",
                acc="current",
                pat_id="3685917",
                birth_date="20040116",
                study_uid="1.2.current",
                modality="CT",
                prior_status="retrieving",
                prior_date_from="20230913",
                prior_date_to="20260912",
                prior_started_at=observed_at - timedelta(seconds=5),
            )
            db.add(order)
            db.flush()
            study = HistoricalStudy(
                order_id=order.id,
                unit_id=unit.id,
                study_uid="1.2.historical",
                study_date="20250110",
                modality="CT",
            )
            db.add(study)
            db.commit()

            _record_compact_result(
                db,
                unit,
                CompactResult(
                    "historical-file",
                    "historical-file.dcm",
                    "1.2.historical",
                    "compressed",
                    observed_at=observed_at,
                ),
            )
            db.commit()

            link = db.scalar(select(HistoricalImageLink))
            self.assertIsNotNone(link)
            self.assertEqual(link.historical_study_id, study.id)

    def test_unsolicited_study_creates_one_monitored_order(self):
        with self.Session() as db:
            unit = make_unit(name="unit")
            db.add(unit)
            db.flush()
            self._add_retrieve_rules(db)
            observed_at = datetime.now()

            for index in range(2):
                _record_compact_result(
                    db,
                    unit,
                    CompactResult(
                        f"image-{index}",
                        f"image-{index}.dcm",
                        "1.2.unsolicited",
                        "compressed",
                        patient_id="123456",
                        birth_date="19800102",
                        study_date="20260915",
                        accession="ACC-DIRECT",
                        modality="CT",
                        body_part="CHEST",
                        observed_at=observed_at,
                    ),
                )
                db.commit()

            orders = list(db.scalars(select(Order)))
            self.assertEqual(len(orders), 1)
            order = orders[0]
            self.assertEqual(order.status, "monitoring")
            self.assertIsNone(order.retrieve_at)
            self.assertIsNone(order.done_at)
            self.assertAlmostEqual(
                (order.monitor_until - observed_at).total_seconds() / 3600,
                6,
                delta=0.01,
            )
            self.assertIsNotNone(order.monitor_next_at)
            self.assertEqual(order.study_uid, "1.2.unsolicited")
            self.assertEqual(order.acc, "ACC-DIRECT")
            self.assertEqual(order.pat_id, "123456")
            self.assertEqual(order.birth_date, "19800102")
            self.assertEqual(order.exam_date, "20260915")
            self.assertEqual(order.modality, "CT")
            self.assertEqual(order.body_part, "CHEST")
            self.assertEqual(order.api_read_status, "confirmed")
            self.assertTrue(order.source_id.startswith("storescp:"))
            self.assertEqual(
                len(
                    list(
                        db.scalars(
                            select(ImageTransfer).where(
                                ImageTransfer.order_id == order.id
                            )
                        )
                    )
                ),
                2,
            )
            self.assertEqual(
                len(
                    list(
                        db.scalars(select(AuditLog).where(AuditLog.action == "create"))
                    )
                ),
                1,
            )
            self.assertEqual(len(list(db.scalars(select(OrderEvent)))), 1)

    def test_unsolicited_study_respects_rule_without_monitoring(self):
        with self.Session() as db:
            unit = make_unit(name="unit")
            db.add(unit)
            db.flush()
            self._add_retrieve_rules(db)

            _record_compact_result(
                db,
                unit,
                CompactResult(
                    "image",
                    "image.dcm",
                    "1.2.no-second",
                    "compressed",
                    patient_id="123456",
                    birth_date="19800102",
                    study_date="20260915",
                    accession="ACC-NO-SECOND",
                    modality="DX",
                    observed_at=datetime.now(),
                ),
            )
            db.commit()

            order = db.scalar(select(Order))
            self.assertEqual(order.status, "done")
            self.assertIsNone(order.monitor_until)
            self.assertIsNotNone(order.done_at)

    def test_later_series_fills_missing_body_part_on_received_order(self):
        with self.Session() as db:
            unit = make_unit(name="unit")
            db.add(unit)
            db.flush()
            self._add_retrieve_rules(db)
            common = {
                "study_uid": "1.2.body-part",
                "status": "compressed",
                "patient_id": "123456",
                "birth_date": "19800102",
                "study_date": "20260915",
                "accession": "ACC-BODY-PART",
                "modality": "CT",
                "observed_at": datetime.now(),
            }

            _record_compact_result(
                db,
                unit,
                CompactResult("series-one", "series-one.dcm", body_part="", **common),
            )
            db.commit()
            order = db.scalar(select(Order))
            self.assertEqual(order.body_part, "")

            _record_compact_result(
                db,
                unit,
                CompactResult(
                    "series-two",
                    "series-two.dcm",
                    body_part="ABDOMEN",
                    **common,
                ),
            )
            db.commit()

            db.refresh(order)
            self.assertEqual(order.body_part, "ABDOMEN")

    def test_received_study_outside_prior_list_skips_prior_retrieve(self):
        with self.Session() as db:
            unit = make_unit(name="unit", retrieve_prior_enabled=True)
            db.add(unit)
            db.flush()
            self._add_retrieve_rules(db)
            db.add(UnitPriorModality(unit_id=unit.id, code="MR"))
            order = Order(
                unit_id=unit.id,
                source_id="api-order",
                acc="ACC-CT",
                pat_id="123456",
                birth_date="19800102",
                status="watching",
                api_read_status="confirmed",
            )
            db.add(order)
            db.commit()

            _record_compact_result(
                db,
                unit,
                CompactResult(
                    "image",
                    "image.dcm",
                    "1.2.ct",
                    "compressed",
                    patient_id="123456",
                    birth_date="19800102",
                    study_date="20260915",
                    accession="ACC-CT",
                    modality="CT",
                    observed_at=datetime.now(),
                ),
            )
            db.commit()

            db.refresh(order)
            self.assertEqual(order.status, "monitoring")
            self.assertEqual(order.prior_status, "disabled")
            self.assertIn(
                "Histórico não solicitado: CT não está nas modalidades de exames "
                "anteriores da unidade (MR)",
                [event.message for event in order.events],
            )

    def test_received_study_satisfies_watching_order_by_accession(self):
        with self.Session() as db:
            unit = make_unit(name="unit", retrieve_prior_enabled=True)
            db.add(unit)
            db.flush()
            self._add_retrieve_rules(db)
            order = Order(
                unit_id=unit.id,
                source_id="api-order",
                acc="ACC-QUEUED",
                pat_id="123456",
                birth_date="19800102",
                status="watching",
                api_read_status="confirmed",
            )
            db.add(order)
            db.commit()

            _record_compact_result(
                db,
                unit,
                CompactResult(
                    "image",
                    "image.dcm",
                    "1.2.queued",
                    "compressed",
                    patient_id="123456",
                    birth_date="19800102",
                    study_date="20260915",
                    accession="ACC-QUEUED",
                    modality="CT",
                    observed_at=datetime.now(),
                ),
            )
            db.commit()

            self.assertEqual(len(list(db.scalars(select(Order)))), 1)
            db.refresh(order)
            self.assertEqual(order.study_uid, "1.2.queued")
            self.assertEqual(order.status, "monitoring")
            self.assertIsNotNone(order.monitor_until)
            self.assertEqual(order.prior_status, "queued")
            self.assertIsNotNone(order.prior_due_at)

    def test_second_study_with_same_accession_complements_the_order(self):
        with self.Session() as db:
            unit = make_unit(name="unit")
            db.add(unit)
            db.flush()
            self._add_retrieve_rules(db)
            order = Order(
                unit_id=unit.id,
                source_id="api-order",
                acc="ACC-SPLIT",
                pat_id="123456",
                birth_date="19800102",
                status="done",
                study_uid="1.2.first",
                modality="CT",
                api_read_status="confirmed",
            )
            db.add(order)
            db.commit()

            for index in range(2):
                _record_compact_result(
                    db,
                    unit,
                    CompactResult(
                        f"recon-{index}",
                        f"recon-{index}.dcm",
                        "1.2.reconstruction",
                        "compressed",
                        patient_id="123456",
                        birth_date="19800102",
                        study_date="20260915",
                        accession="ACC-SPLIT",
                        modality="CT",
                        observed_at=datetime.now(),
                    ),
                )
                db.commit()

            self.assertEqual(len(list(db.scalars(select(Order)))), 1)
            db.refresh(order)
            self.assertEqual(order.study_uid, "1.2.first")
            self.assertEqual(order.status, "done")
            transfers = list(db.scalars(select(ImageTransfer)))
            self.assertEqual({t.status for t in transfers}, {"publishing"})
            self.assertEqual({t.order_id for t in transfers}, {order.id})
            complement_events = [
                event.message
                for event in order.events
                if event.message.startswith("Estudo complementar")
            ]
            self.assertEqual(len(complement_events), 1)
            self.assertIn("1.2.reconstruction", complement_events[0])

    def test_archived_order_is_reused_and_restored_by_study_uid(self):
        with self.Session() as db:
            unit = make_unit(name="unit")
            db.add(unit)
            db.flush()
            self._add_retrieve_rules(db)
            order = Order(
                unit_id=unit.id,
                source_id="old-order",
                acc="ACC-ARCHIVED",
                pat_id="123456",
                birth_date="19800102",
                status="done",
                study_uid="1.2.archived",
                modality="CT",
                archived_at=datetime.now(),
                archive_reason="retenção",
            )
            db.add(order)
            db.commit()
            order_id = order.id

            _record_compact_result(
                db,
                unit,
                CompactResult(
                    "image",
                    "image.dcm",
                    "1.2.archived",
                    "compressed",
                    patient_id="123456",
                    birth_date="19800102",
                    study_date="20260915",
                    accession="ACC-ARCHIVED",
                    modality="CT",
                    observed_at=datetime.now(),
                ),
            )
            db.commit()

            self.assertEqual(len(list(db.scalars(select(Order)))), 1)
            restored = db.get(Order, order_id)
            self.assertIsNone(restored.archived_at)
            self.assertEqual(restored.archive_reason, "")
            self.assertEqual(restored.status, "monitoring")
            self.assertIsNotNone(restored.monitor_until)
            audit = db.scalar(select(AuditLog).where(AuditLog.action == "restore"))
            self.assertIsNotNone(audit)

    def test_invalid_unsolicited_metadata_is_quarantined_without_order(self):
        with (
            tempfile.TemporaryDirectory() as send_dir,
            tempfile.TemporaryDirectory() as error_dir,
            self.Session() as db,
        ):
            unit = make_unit(name="unit", send_dir=send_dir, error_dir=error_dir)
            db.add(unit)
            db.flush()
            path = Path(send_dir) / "invalid.dcm"
            path.write_bytes(b"DICOM")

            _record_compact_result(
                db,
                unit,
                CompactResult(
                    "invalid",
                    "invalid.dcm",
                    "1.2.invalid",
                    "compressed",
                    patient_id="",
                    birth_date="19800102",
                    study_date="20260915",
                    accession="ACC-INVALID",
                    modality="CT",
                    observed_at=datetime.now(),
                    temp_output=str(path),
                ),
            )
            db.commit()

            self.assertIsNone(db.scalar(select(Order)))
            transfer = db.scalar(select(ImageTransfer))
            self.assertEqual(transfer.status, "metadata_error")
            self.assertIn("PatientID", transfer.last_error)
            self.assertFalse(path.exists())
            self.assertTrue((Path(error_dir) / "invalid.dcm").exists())

    def test_applied_dicom_rule_is_audited_on_transfer(self):
        with self.Session() as db:
            unit = make_unit(
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
            )
            db.add(unit)
            db.flush()
            dicom_rule = DicomRule(
                name="Descartar SLRX",
                enabled=True,
                priority=10,
                combinator="and",
                action="delete",
            )
            db.add(dicom_rule)
            db.flush()

            _record_compact_result(
                db,
                unit,
                CompactResult(
                    "image",
                    "",
                    "1.2.3",
                    "discarded_rule",
                    rule_matches=(RuleMatch(dicom_rule.id, dicom_rule.name, "delete"),),
                ),
            )
            db.commit()

            application = db.scalar(select(DicomRuleApplication))
            self.assertIsNotNone(application)
            self.assertEqual(application.rule_id, dicom_rule.id)
            self.assertEqual(application.rule_name, "Descartar SLRX")
            self.assertEqual(application.action, "delete")


if __name__ == "__main__":
    unittest.main()
