import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from app.worker import _schedule_ack_job, _tick
from tests.support import DatabaseTestCase, make_unit


class UnitCloudSettingsTest(DatabaseTestCase):
    def test_only_one_ack_job_runs_for_each_unit(self):
        pool = MagicMock()
        first = Future()
        second = Future()
        pool.submit.side_effect = [first, second]
        jobs = {}

        _schedule_ack_job(pool, jobs, 7)
        _schedule_ack_job(pool, jobs, 7)
        self.assertEqual(pool.submit.call_count, 1)

        first.set_result(None)
        _schedule_ack_job(pool, jobs, 7)
        self.assertEqual(pool.submit.call_count, 2)

    def test_worker_uses_each_units_cloud_destination_and_zero_settle(self):
        with self.Session() as db:
            db.add_all(
                [
                    make_unit(
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
                    make_unit(
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

    def test_failure_in_one_unit_does_not_block_the_next_unit(self):
        with self.Session() as db:
            db.add_all(
                [
                    make_unit(name="Unidade com falha", enabled=True),
                    make_unit(name="Unidade saudável", enabled=True),
                ]
            )
            db.commit()

        def fail_first_unit(_db, unit):
            if unit.name == "Unidade com falha":
                raise RuntimeError("falha isolada")

        supervisor = MagicMock()
        pool = MagicMock()
        with (
            patch("app.worker.SessionLocal", self.Session),
            patch("app.worker.recover_stale_locks"),
            patch("app.worker.cleanup_unmatched_orders"),
            patch("app.worker.archive_completed_orders"),
            patch("app.worker.ingest_unit", side_effect=fail_first_unit),
            patch("app.worker.find_pending") as find_pending,
            patch("app.worker.claim_due_moves", return_value=[]),
            patch("app.worker.compact_unit"),
            patch("app.worker.send_unit") as send_unit,
        ):
            _tick(supervisor, pool)

        self.assertEqual(find_pending.call_count, 2)
        self.assertEqual(send_unit.call_count, 2)


if __name__ == "__main__":
    unittest.main()
