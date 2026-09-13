import unittest
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import Base, Unit


class DatabaseTestCase(unittest.TestCase):
    """Fast database test base; PostgreSQL integration tests remain separate."""

    def setUp(self) -> None:
        super().setUp()
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def tearDown(self) -> None:
        self.engine.dispose()
        super().tearDown()


def make_unit(**overrides: Any) -> Unit:
    values: dict[str, Any] = {
        "name": "unit",
        "enabled": True,
        "orders_api_url": "https://orders.example/api",
        "orders_api_token": "orders-token",
        "pacs_aet": "PACS",
        "pacs_ip": "127.0.0.1",
        "pacs_port": 104,
        "calling_aet": "RETRIEVE",
        "store_port": 11112,
        "receive_dir": "/data/receive",
        "send_dir": "/data/send",
        "error_dir": "/data/error",
        "cloud_url": "https://cloud.example/upload",
    }
    values.update(overrides)
    return Unit(**values)
