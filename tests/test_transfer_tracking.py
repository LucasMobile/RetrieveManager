import logging
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import Base, ImageTransfer, Order, Unit
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
                input_dir="/in",
                sent_dir="/sent",
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
                filename="order.txt",
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
                input_dir="/in",
                sent_dir="/sent",
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
                filename="order.txt",
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


if __name__ == "__main__":
    unittest.main()
