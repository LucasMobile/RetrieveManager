import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.main import _dashboard_units
from app.models import Base, Order, Unit


class DashboardTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()
        self.engine.dispose()

    def _unit(self, name: str, *, enabled: bool) -> Unit:
        path = self.temp_dir.name
        return Unit(
            name=name,
            enabled=enabled,
            orders_api_url="https://integracao.example/v1/pedidos",
            orders_api_token="integration-token",
            pacs_aet="PACS",
            pacs_ip="127.0.0.1",
            pacs_port=2104,
            calling_aet="RETRIEVE",
            store_port=444,
            receive_dir=path,
            send_dir=path,
            error_dir=path,
            token="token",
        )

    @patch("app.main.port_listening", return_value=True)
    def test_summary_aggregates_units_and_orders(self, _port_listening):
        with self.Session() as db:
            active = self._unit("A", enabled=True)
            db.add_all([active, self._unit("B", enabled=False)])
            db.flush()
            db.add_all(
                [
                    Order(
                        unit_id=active.id,
                        source_id="watching",
                        acc="1",
                        birth_date="20000101",
                        status="watching",
                    ),
                    Order(
                        unit_id=active.id,
                        source_id="queue",
                        acc="2",
                        birth_date="20000101",
                        status="wait_retrieve",
                    ),
                    Order(
                        unit_id=active.id,
                        source_id="error",
                        acc="3",
                        birth_date="20000101",
                        status="error",
                        updated_at=datetime.now() - timedelta(hours=1),
                    ),
                    Order(
                        unit_id=active.id,
                        source_id="prior-queue",
                        acc="4",
                        birth_date="20000101",
                        status="wait_retrieve",
                        prior_status="queued",
                    ),
                    Order(
                        unit_id=active.id,
                        source_id="prior-running",
                        acc="5",
                        birth_date="20000101",
                        status="wait_retrieve",
                        prior_status="retrieving",
                    ),
                    Order(
                        unit_id=active.id,
                        source_id="prior-error",
                        acc="6",
                        birth_date="20000101",
                        status="done",
                        prior_status="error",
                        updated_at=datetime.now() - timedelta(hours=1),
                    ),
                ]
            )
            db.commit()

            units, pager, summary = _dashboard_units(db, 1)

            self.assertEqual(len(units), 2)
            self.assertEqual(pager["total"], 2)
            self.assertEqual(summary["enabled"], 1)
            self.assertEqual(summary["watching"], 1)
            # Cada pedido é contado uma única vez por categoria, mesmo quando
            # o status atual e o histórico apontam para a mesma fila.
            self.assertEqual(summary["queue"], 3)
            self.assertEqual(summary["running"], 1)
            self.assertEqual(summary["error"], 2)
            active_view = next(unit for unit in units if unit.name == "A")
            self.assertEqual(active_view.counts["queue"], 3)
            self.assertEqual(active_view.counts["running"], 1)
            self.assertEqual(active_view.counts["error"], 2)


if __name__ == "__main__":
    unittest.main()
