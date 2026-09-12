import logging
import unittest
from datetime import datetime, timedelta

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.dicom_rules import RuleMatch
from app.models import (
    Base,
    DicomRule,
    DicomRuleApplication,
    HistoricalImageLink,
    HistoricalStudy,
    ImageTransfer,
    Order,
    Unit,
)
from app.pipeline import (
    CircuitState,
    CompactResult,
    SendResult,
    _record_compact_result,
    _record_send_results,
)


class TransferTrackingTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def tearDown(self):
        self.engine.dispose()

    def test_upload_failure_is_persisted_with_backoff(self):
        with self.Session() as db:
            unit = Unit(
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
            unit = Unit(
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
            self.assertEqual(transfer.status, "compressed")
            self.assertEqual(transfer.correlation_id, "new-correlation")
            self.assertEqual(transfer.attempts, 0)

    def test_historical_image_is_grouped_by_study_uid(self):
        with self.Session() as db:
            unit = Unit(
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
                    patient_id="30211738-A",
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

    def test_applied_dicom_rule_is_audited_on_transfer(self):
        with self.Session() as db:
            unit = Unit(
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
                    rule_matches=(
                        RuleMatch(dicom_rule.id, dicom_rule.name, "delete"),
                    ),
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
