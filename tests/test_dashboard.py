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
            input_dir=path,
            sent_dir=path,
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
                        filename="watching.txt",
                        acc="1",
                        birth_date="20000101",
                        status="watching",
                    ),
                    Order(
                        unit_id=active.id,
                        filename="queue.txt",
                        acc="2",
                        birth_date="20000101",
                        status="wait_retrieve",
                    ),
                    Order(
                        unit_id=active.id,
                        filename="error.txt",
                        acc="3",
                        birth_date="20000101",
                        status="error",
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
            self.assertEqual(summary["queue"], 1)
            self.assertEqual(summary["running"], 0)
            self.assertEqual(summary["error"], 1)


if __name__ == "__main__":
    unittest.main()
