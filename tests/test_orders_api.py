import logging
import unittest
from unittest.mock import AsyncMock, patch

from sqlalchemy import select

from app.models import AuditLog, Order, Unit
from app.orders_api import AckResult, InvalidApiOrder, OrdersApiError, parse_api_order
from app.pipeline import _last_orders_api_poll, ingest_unit
from tests.support import DatabaseTestCase, make_unit


class OrdersApiParsingTest(unittest.TestCase):
    def test_converts_pleres_dates_to_dicom_format(self):
        parsed = parse_api_order(
            {
                "_id": "69ea265a704bca3b5bbe737a",
                "patientId": "3624313",
                "accessionNumber": "7202601300013848",
                "examDate": "04/23/2026 10:49:23",
                "patientBirthdate": "11/04/1955 00:00:00",
            }
        )
        self.assertEqual(parsed.source_id, "69ea265a704bca3b5bbe737a")
        self.assertEqual(parsed.patient_birthdate, "19551104")
        self.assertEqual(parsed.exam_date, "20260423")

    def test_rejects_missing_fields_and_unexpected_dates(self):
        valid = {
            "patientId": "1",
            "accessionNumber": "2",
            "examDate": "04/23/2026 10:49:23",
            "patientBirthdate": "11/04/1955 00:00:00",
        }
        for key in valid:
            with self.subTest(key=key), self.assertRaises(InvalidApiOrder):
                parse_api_order({**valid, key: ""})
        with self.assertRaises(InvalidApiOrder):
            parse_api_order({**valid, "examDate": "2026-04-23"})

    def test_rejects_identifiers_that_do_not_fit_database_contract(self):
        payload = {
            "patientId": "1" * 65,
            "accessionNumber": "2",
            "examDate": "04/23/2026 10:49:23",
            "patientBirthdate": "11/04/1955 00:00:00",
        }
        with self.assertRaisesRegex(InvalidApiOrder, "patientId excede"):
            parse_api_order(payload)


class OrdersApiIngestionTest(DatabaseTestCase):
    def setUp(self):
        super().setUp()
        _last_orders_api_poll.clear()

    def tearDown(self):
        _last_orders_api_poll.clear()
        super().tearDown()

    @staticmethod
    def _unit() -> Unit:
        return make_unit(
            name="AXIAL",
            orders_api_url="https://integracao.example/v1/pedidos",
            orders_api_token="secret-token",
            orders_api_station_id="26",
            pacs_port=2104,
            store_port=444,
            receive_dir="/receive",
            send_dir="/send",
            error_dir="/error",
            token="unit-token",
        )

    @staticmethod
    def _payload(accession="7202601300013848", **changes):
        item = {
            "mirthReaded": False,
            "_id": "69ea265a704bca3b5bbe737a",
            "idPosto": "26",
            "patientId": "3624313",
            "accessionNumber": accession,
            "examDate": "04/23/2026 10:49:23",
            "patientBirthdate": "11/04/1955 00:00:00",
        }
        item.update(changes)
        return item

    def test_filters_deduplicates_commits_and_acknowledges(self):
        payload = [
            self._payload(),
            self._payload(_id="duplicate"),
            self._payload("already-read", mirthReaded=True),
            self._payload("other-station", idPosto="48"),
            self._payload("invalid", patientId=""),
        ]
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.commit()
            with (
                patch("app.pipeline.fetch_orders", new=AsyncMock(return_value=payload)),
                patch(
                    "app.pipeline.acknowledge_orders",
                    new=AsyncMock(return_value=[AckResult("7202601300013848", True)]),
                ) as acknowledge,
            ):
                self.assertEqual(ingest_unit(db, unit), 1)

            orders = list(db.scalars(select(Order)))
            self.assertEqual(len(orders), 1)
            self.assertEqual(orders[0].birth_date, "19551104")
            self.assertEqual(orders[0].exam_date, "20260423")
            self.assertEqual(orders[0].api_read_status, "confirmed")
            audit = db.scalar(select(AuditLog))
            self.assertIsNotNone(audit)
            self.assertEqual(audit.actor_username, "Sistema")
            self.assertEqual(audit.resource_type, "order")
            self.assertEqual(audit.resource_id, str(orders[0].id))
            acknowledge.assert_awaited_once_with(
                unit.orders_api_url,
                unit.orders_api_token,
                ["7202601300013848"],
            )

    def test_failed_ack_is_retried_without_duplicate_order(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.commit()
            with (
                patch(
                    "app.pipeline.fetch_orders",
                    new=AsyncMock(return_value=[self._payload()]),
                ),
                patch(
                    "app.pipeline.acknowledge_orders",
                    new=AsyncMock(
                        return_value=[AckResult("7202601300013848", False, "HTTP 503")]
                    ),
                ),
            ):
                self.assertEqual(ingest_unit(db, unit), 1)

            order = db.scalar(select(Order))
            self.assertEqual(order.api_read_status, "pending")
            self.assertEqual(order.api_read_attempts, 1)

            _last_orders_api_poll.clear()
            with (
                patch("app.pipeline.fetch_orders", new=AsyncMock(return_value=[])),
                patch(
                    "app.pipeline.acknowledge_orders",
                    new=AsyncMock(return_value=[AckResult("7202601300013848", True)]),
                ),
            ):
                self.assertEqual(ingest_unit(db, unit), 0)

            self.assertEqual(db.scalar(select(Order)).api_read_status, "confirmed")
            self.assertEqual(db.scalar(select(Order)).api_read_attempts, 2)
            self.assertEqual(len(list(db.scalars(select(Order)))), 1)

    def test_get_failure_logs_the_safe_error_detail(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.commit()
            with (
                patch(
                    "app.pipeline.fetch_orders",
                    new=AsyncMock(side_effect=OrdersApiError("HTTP 401")),
                ),
                patch(
                    "app.pipeline.acknowledge_orders",
                    new=AsyncMock(return_value=[]),
                ),
                patch("app.pipeline.log_event") as logged,
            ):
                self.assertEqual(ingest_unit(db, unit), 0)

            failure = next(
                call
                for call in logged.call_args_list
                if call.args[:3]
                == (logging.getLogger("worker"), logging.ERROR, "orders.api.get")
            )
            self.assertEqual(failure.kwargs["error_detail"], "HTTP 401")


if __name__ == "__main__":
    unittest.main()
