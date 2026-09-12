import unittest
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import Base, Unit
from app.worker import _tick


class UnitCloudSettingsTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def tearDown(self):
        self.engine.dispose()

    def test_worker_uses_each_units_cloud_destination_and_zero_settle(self):
        with self.Session() as db:
            db.add_all(
                [
                    Unit(
                        name="Unidade A",
                        enabled=True,
                        orders_api_url="https://example.test/orders",
                        orders_api_token="integration-token",
                        pacs_aet="PACS",
                        pacs_ip="127.0.0.1",
                        pacs_port=2104,
                        calling_aet="RETRIEVE",
                        store_port=444,
                        receive_dir="/receive",
                        send_dir="/send",
                        error_dir="/error",
                        token="unit-token",
                        cloud_url="https://cloud.example/send",
                        file_settle_seconds=0,
                    ),
                    Unit(
                        name="Unidade B",
                        enabled=True,
                        orders_api_url="https://example.test/orders-b",
                        orders_api_token="integration-token-b",
                        pacs_aet="PACSB",
                        pacs_ip="127.0.0.2",
                        pacs_port=2105,
                        calling_aet="RETRIEVEB",
                        store_port=445,
                        receive_dir="/receive-b",
                        send_dir="/send-b",
                        error_dir="/error-b",
                        token="unit-token-b",
                        cloud_url="https://cloud-b.example/send",
                        file_settle_seconds=9,
                    ),
                ]
            )
            db.commit()

        supervisor = MagicMock()
        pool = MagicMock()
        with (
            patch("app.worker.SessionLocal", self.Session),
            patch("app.worker.recover_stale_locks"),
            patch("app.worker.ingest_unit"),
            patch("app.worker.find_pending"),
            patch("app.worker.claim_due_moves", return_value=[]),
            patch("app.worker.compact_unit"),
            patch("app.worker.send_unit") as send_unit,
        ):
            _tick(supervisor, pool)

        calls = {
            (call.args[1].name, call.args[2], call.args[3])
            for call in send_unit.call_args_list
        }
        self.assertEqual(
            calls,
            {
                ("Unidade A", "https://cloud.example/send", 0),
                ("Unidade B", "https://cloud-b.example/send", 9),
            },
        )


if __name__ == "__main__":
    unittest.main()
