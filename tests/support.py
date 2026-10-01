import unittest
from typing import Any
from uuid import uuid4

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker

from app.config import DATABASE_URL
from app.models import Base, Unit

# Database tests use the same POSTGRES_* settings as the app, each test in its
# own throwaway schema.


def _postgres_test_url() -> URL:
    if "test" not in (DATABASE_URL.database or "").lower():
        raise RuntimeError(
            "Os testes criam e apagam schemas: use um banco descartável cujo "
            "POSTGRES_DB contenha 'test' (ex.: retrieve_test)"
        )
    return DATABASE_URL


def postgres_test_engine(test: unittest.TestCase) -> Engine:
    """Engine bound to a fresh schema with every table; dropped after the test."""
    url = _postgres_test_url()
    schema = f"retrieve_test_{uuid4().hex}"
    admin = create_engine(url)
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    Base.metadata.create_all(engine)

    def drop() -> None:
        engine.dispose()
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()

    test.addCleanup(drop)
    return engine


class DatabaseTestCase(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.engine = postgres_test_engine(self)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)


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
