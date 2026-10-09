import asyncio
import logging
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch

from sqlalchemy import event, select

from app.models import AuditLog, Order, Unit
from app.orders_api import (
    AckResult,
    InvalidApiOrder,
    OrdersApiError,
    acknowledge_orders,
    iter_acknowledgements,
    parse_api_order,
)
from app.pipeline.orders import (
    _last_orders_api_poll,
    acknowledge_pending_orders,
    ingest_unit,
)
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

    def test_filters_deduplicates_and_leaves_ack_pending_for_background_job(self):
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
            transaction_during_get = []

            async def fetch(_url, _token):
                transaction_during_get.append(db.in_transaction())
                return payload

            with patch(
                "app.pipeline.orders.fetch_orders",
                new=fetch,
            ):
                self.assertEqual(ingest_unit(db, unit), 1)

            self.assertEqual(transaction_during_get, [False])
            orders = list(db.scalars(select(Order)))
            self.assertEqual(len(orders), 1)
            self.assertEqual(orders[0].birth_date, "19551104")
            self.assertEqual(orders[0].exam_date, "20260423")
            self.assertEqual(orders[0].api_read_status, "pending")
            self.assertEqual(orders[0].api_read_attempts, 0)
            audit = db.scalar(select(AuditLog))
            self.assertIsNotNone(audit)
            self.assertEqual(audit.actor_username, "Sistema")
            self.assertEqual(audit.resource_type, "order")
            self.assertEqual(audit.resource_id, str(orders[0].id))

    def test_known_orders_are_read_in_one_query(self):
        def run(count):
            _last_orders_api_poll.clear()
            payload = [self._payload(f"ACC-{index}") for index in range(count)]
            statements = []

            def track(_conn, _cursor, statement, *_args):
                statements.append(statement)

            event.listen(self.engine, "before_cursor_execute", track)
            try:
                with patch(
                    "app.pipeline.orders.fetch_orders",
                    new=AsyncMock(return_value=payload),
                ):
                    ingest_unit(db, unit)
            finally:
                event.remove(self.engine, "before_cursor_execute", track)
            return [s for s in statements if s.lstrip().startswith("SELECT")]

        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.commit()
            # Every order of the second GET is already recorded (no ACK yet).
            run(30)
            self.assertEqual(len(run(30)), 1)
            orders = list(db.scalars(select(Order)))
            self.assertEqual(len(orders), 30)
            self.assertEqual({order.api_read_status for order in orders}, {"pending"})

    def test_failed_background_ack_is_retried_without_duplicate_order(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.commit()
            with patch(
                "app.pipeline.orders.fetch_orders",
                new=AsyncMock(return_value=[self._payload()]),
            ):
                self.assertEqual(ingest_unit(db, unit), 1)

            async def failed_ack(*_args, **_kwargs):
                yield AckResult("7202601300013848", False, "HTTP 503")

            with patch("app.pipeline.orders.iter_acknowledgements", new=failed_ack):
                acknowledge_pending_orders(db, unit)

            order = db.scalar(select(Order))
            self.assertEqual(order.api_read_status, "pending")
            self.assertEqual(order.api_read_attempts, 1)

            _last_orders_api_poll.clear()
            with patch(
                "app.pipeline.orders.fetch_orders",
                new=AsyncMock(return_value=[]),
            ):
                self.assertEqual(ingest_unit(db, unit), 0)

            async def successful_ack(*_args, **_kwargs):
                yield AckResult("7202601300013848", True)

            with patch("app.pipeline.orders.iter_acknowledgements", new=successful_ack):
                acknowledge_pending_orders(db, unit)

            self.assertEqual(db.scalar(select(Order)).api_read_status, "confirmed")
            self.assertEqual(db.scalar(select(Order)).api_read_attempts, 2)
            self.assertEqual(len(list(db.scalars(select(Order)))), 1)

    def test_get_snapshot_older_than_confirmation_does_not_reack(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.commit()
            with patch(
                "app.pipeline.orders.fetch_orders",
                new=AsyncMock(return_value=[self._payload()]),
            ):
                self.assertEqual(ingest_unit(db, unit), 1)

            def ack_during_get(*_args, **_kwargs):
                # The ACK job's PUT confirms while this GET is still in flight.
                order = db.scalar(select(Order))
                order.api_read_status = "confirmed"
                order.api_read_at = datetime.now()
                db.commit()
                return [self._payload()]

            _last_orders_api_poll.clear()
            with patch(
                "app.pipeline.orders.fetch_orders",
                new=AsyncMock(side_effect=ack_during_get),
            ):
                ingest_unit(db, unit)
            self.assertEqual(db.scalar(select(Order)).api_read_status, "confirmed")

            # A later GET that still reports it unread re-acknowledges.
            _last_orders_api_poll.clear()
            with patch(
                "app.pipeline.orders.fetch_orders",
                new=AsyncMock(return_value=[self._payload()]),
            ):
                ingest_unit(db, unit)
            self.assertEqual(db.scalar(select(Order)).api_read_status, "pending")

    def test_get_failure_logs_the_safe_error_detail(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.commit()
            with (
                patch(
                    "app.pipeline.orders.fetch_orders",
                    new=AsyncMock(side_effect=OrdersApiError("HTTP 401")),
                ),
                patch("app.pipeline.orders.log_event") as logged,
            ):
                self.assertEqual(ingest_unit(db, unit), 0)

            failure = next(
                call
                for call in logged.call_args_list
                if call.args[:3]
                == (logging.getLogger("worker"), logging.ERROR, "orders.api.get")
            )
            self.assertEqual(failure.kwargs["error_detail"], "HTTP 401")

    def test_put_uses_company_of_each_unit_even_with_shared_integration_token(self):
        with self.Session() as db:
            first = make_unit(
                name="Sao Cristovao",
                orders_api_station_id="25",
                orders_api_company_id="1582",
                token="dicom-25",
            )
            second = make_unit(
                name="CDB",
                orders_api_station_id="48",
                orders_api_company_id="4232",
                token="dicom-48",
            )
            db.add_all([first, second])
            db.commit()
            payload = [
                self._payload("ACC-25", idPosto=25),
                self._payload("ACC-48", idPosto=48),
            ]
            with patch(
                "app.pipeline.orders.fetch_orders", new=AsyncMock(return_value=payload)
            ):
                self.assertEqual(ingest_unit(db, first), 1)
                self.assertEqual(ingest_unit(db, second), 1)
            with patch(
                "app.orders_api._request", new=AsyncMock(return_value=(200, "{}"))
            ) as request:
                acknowledge_pending_orders(db, first)
                self.assertEqual(
                    db.scalar(
                        select(Order).where(Order.unit_id == second.id)
                    ).api_read_status,
                    "pending",
                )
                acknowledge_pending_orders(db, second)
            self.assertEqual(request.await_count, 2)
            for call, unit, accession in zip(
                request.await_args_list,
                (first, second),
                ("ACC-25", "ACC-48"),
                strict=True,
            ):
                self.assertEqual(
                    call.args[1:4],
                    ("PUT", unit.orders_api_url + "/", unit.orders_api_token),
                )
                self.assertEqual(call.kwargs["params"], {"accessionNumber": accession})
                self.assertEqual(
                    call.kwargs["json"],
                    {"mirthReaded": True, "empresa_id": unit.orders_api_company_id},
                )
            self.assertTrue(
                all(
                    order.api_read_status == "confirmed"
                    for order in db.scalars(select(Order))
                )
            )

    def test_missing_company_keeps_ingestion_and_blocks_put_until_configured(self):
        with self.Session() as db:
            unit = self._unit()
            unit.orders_api_company_id = ""
            db.add(unit)
            db.commit()
            with patch(
                "app.pipeline.orders.fetch_orders",
                new=AsyncMock(return_value=[self._payload()]),
            ):
                self.assertEqual(ingest_unit(db, unit), 1)
            with patch(
                "app.orders_api._request", new=AsyncMock(return_value=(200, "{}"))
            ) as request:
                acknowledge_pending_orders(db, unit)
                request.assert_not_awaited()
                order = db.scalar(select(Order))
                self.assertEqual(order.api_read_status, "pending")
                self.assertEqual(order.api_read_attempts, 0)
                self.assertIn("Empresa ID", order.api_read_last_error)
                unit.orders_api_company_id = "4232"
                db.commit()
                acknowledge_pending_orders(db, unit)
                self.assertEqual(request.await_count, 1)
                self.assertEqual(order.api_read_status, "confirmed")
                self.assertEqual(order.api_read_last_error, "")

    def test_ingestion_summary_explains_why_returned_orders_do_not_enter_queue(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            archived = Order(
                unit_id=unit.id,
                acc="ARCHIVED",
                birth_date="20000101",
                archived_at=datetime(2026, 9, 16),
                status="done",
            )
            db.add(archived)
            db.commit()
            payload = [
                self._payload("ARCHIVED"),
                self._payload("NEW", examDate="09/17/2026 08:00:00"),
                self._payload("NEW"),
                self._payload("READ", mirthReaded=True),
                self._payload("OTHER", idPosto=48),
                self._payload("INVALID", patientId=""),
            ]
            with (
                patch(
                    "app.pipeline.orders.fetch_orders",
                    new=AsyncMock(return_value=payload),
                ),
                patch("app.pipeline.orders.log_event") as logged,
            ):
                self.assertEqual(ingest_unit(db, unit), 1)
            summary = next(
                call.kwargs
                for call in logged.call_args_list
                if call.args[2] == "orders.api.ingest.summary"
            )
            for field in (
                "created",
                "existing_archived",
                "duplicate_payload",
                "already_read",
                "other_station",
                "invalid",
            ):
                self.assertEqual(summary[field + "_count"], 1)
            self.assertEqual(summary["record_count"], 6)
            self.assertIsNotNone(archived.archived_at)
            self.assertEqual(
                db.scalar(select(Order).where(Order.acc == "NEW")).exam_date, "20260917"
            )
            rejection = next(
                call.kwargs
                for call in logged.call_args_list
                if call.kwargs.get("error_type") == "InvalidApiOrder"
            )
            self.assertEqual(
                rejection["error_detail"], "campo obrigatório ausente: patientId"
            )

    def test_completed_ack_is_committed_before_later_result_crashes(self):
        with self.Session() as db:
            unit = self._unit()
            db.add(unit)
            db.flush()
            first = Order(
                unit_id=unit.id,
                acc="ACK-ONE",
                birth_date="20000101",
                api_read_status="pending",
            )
            second = Order(
                unit_id=unit.id,
                acc="ACK-TWO",
                birth_date="20000101",
                api_read_status="pending",
            )
            db.add_all([first, second])
            db.commit()

            async def partial_ack(*_args, **_kwargs):
                yield AckResult("ACK-ONE", True)
                raise RuntimeError("falha depois do primeiro resultado")

            with (
                patch("app.pipeline.orders.iter_acknowledgements", new=partial_ack),
                self.assertRaisesRegex(RuntimeError, "depois do primeiro"),
            ):
                acknowledge_pending_orders(db, unit)

            db.expire_all()
            self.assertEqual(db.get(Order, first.id).api_read_status, "confirmed")
            self.assertEqual(db.get(Order, second.id).api_read_status, "pending")


class OrdersApiAcknowledgementTest(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_company_never_opens_http_session(self):
        for company_id in ("", "0", "-1", "invalid"):
            with (
                self.subTest(company_id=company_id),
                patch("app.orders_api.aiohttp.ClientSession") as session,
            ):
                with self.assertRaisesRegex(OrdersApiError, "Empresa ID"):
                    async for _result in iter_acknowledgements(
                        "https://example.test/orders",
                        "token",
                        ["ACC"],
                        company_id=company_id,
                    ):
                        self.fail("Invalid company must not be acknowledged")
                session.assert_not_called()

    async def test_parallel_units_keep_their_company_and_token(self):
        async def request(_session, method, _url, token, **kwargs):
            await asyncio.sleep(0)
            expected = {"token-25": "1582", "token-48": "4232"}[token]
            self.assertEqual(method, "PUT")
            self.assertEqual(
                kwargs["json"], {"mirthReaded": True, "empresa_id": expected}
            )
            self.assertEqual(kwargs["params"]["accessionNumber"], "ACC-" + expected)
            return 200, "{}"

        with patch("app.orders_api._request", new=request):
            results = await asyncio.gather(
                acknowledge_orders(
                    "https://example.test/orders",
                    "token-25",
                    ["ACC-1582"],
                    company_id="1582",
                ),
                acknowledge_orders(
                    "https://example.test/orders",
                    "token-48",
                    ["ACC-4232"],
                    company_id="4232",
                ),
            )
        self.assertTrue(all(result.success for batch in results for result in batch))

    async def test_acknowledgements_run_with_bounded_concurrency(self):
        active = 0
        peak = 0

        async def request(*_args, **_kwargs):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.01)
            active -= 1
            return 200, ""

        with patch("app.orders_api._request", new=request):
            results = [
                result
                async for result in iter_acknowledgements(
                    "https://example.test/orders",
                    "token",
                    [f"ACC-{index}" for index in range(10)],
                    concurrency=3,
                    company_id="4232",
                )
            ]

        self.assertEqual(len(results), 10)
        self.assertTrue(all(result.success for result in results))
        self.assertEqual(peak, 3)


if __name__ == "__main__":
    unittest.main()
