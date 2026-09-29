import os
import unittest
from typing import Any
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

from app.models import Base, Unit

# With TEST_POSTGRES_URL set (database name containing "test"), every database
# test runs in its own throwaway PostgreSQL schema instead of in-memory SQLite.
TEST_POSTGRES_URL = os.getenv("TEST_POSTGRES_URL", "").strip()


def _postgres_test_url() -> str:
    if not TEST_POSTGRES_URL:
        return ""
    parsed = make_url(TEST_POSTGRES_URL)
    if not parsed.drivername.startswith("postgresql"):
        raise RuntimeError("TEST_POSTGRES_URL não aponta para PostgreSQL")
    if "test" not in (parsed.database or "").lower():
        raise RuntimeError("o nome do banco PostgreSQL deve conter 'test'")
    return TEST_POSTGRES_URL


class DatabaseTestCase(unittest.TestCase):
    """Fast SQLite by default; PostgreSQL when TEST_POSTGRES_URL is set."""

    def setUp(self) -> None:
        super().setUp()
        url = _postgres_test_url()
        if url:
            self.schema = f"retrieve_test_{uuid4().hex}"
            self.admin_engine = create_engine(url)
            with self.admin_engine.begin() as connection:
                connection.execute(text(f'CREATE SCHEMA "{self.schema}"'))
            self.engine = create_engine(
                url, connect_args={"options": f"-csearch_path={self.schema}"}
            )
        else:
            self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def tearDown(self) -> None:
        self.engine.dispose()
        if hasattr(self, "admin_engine"):
            with self.admin_engine.begin() as connection:
                connection.execute(text(f'DROP SCHEMA "{self.schema}" CASCADE'))
            self.admin_engine.dispose()
        super().tearDown()


def make_unit(**overrides: Any) -> Unit:
    values: dict[str, Any] = {
        "name": "unit",
        "enabled": True,
        "orders_api_url": "https://orders.example/api",
        "orders_api_token": "orders-token",
        "orders_api_company_id": "1582",
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
