import os
import unittest
from datetime import datetime
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from app import db as db_module
from app.models import Base, Order, UnitCompressionSettings, UnitCompressRule
from tests.support import make_unit


class PostgreSQLRetrieveIntegrationTest(unittest.TestCase):
    """Opt-in checks for the production database's locking/schema contract."""

    @classmethod
    def setUpClass(cls):
        database_url = os.getenv("TEST_POSTGRES_URL", "").strip()
        if not database_url:
            raise unittest.SkipTest("TEST_POSTGRES_URL não configurada")
        parsed = make_url(database_url)
        if not parsed.drivername.startswith("postgresql"):
            raise unittest.SkipTest("TEST_POSTGRES_URL não aponta para PostgreSQL")
        if "test" not in (parsed.database or "").lower():
            raise unittest.SkipTest("o nome do banco PostgreSQL deve conter 'test'")

        cls.schema = f"retrieve_test_{uuid4().hex}"
        cls.admin_engine = create_engine(database_url, pool_pre_ping=True)
        with cls.admin_engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{cls.schema}"'))
        cls.engine = create_engine(
            database_url,
            connect_args={"options": f"-csearch_path={cls.schema}"},
            pool_pre_ping=True,
        )
        Base.metadata.create_all(cls.engine)
        cls.Session = sessionmaker(bind=cls.engine, expire_on_commit=False)

    @classmethod
    def tearDownClass(cls):
        cls.engine.dispose()
        with cls.admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{cls.schema}" CASCADE'))
        cls.admin_engine.dispose()

    def test_additive_history_checkpoint_table_exists(self):
        table_names = inspect(self.engine).get_table_names()
        self.assertIn("historical_series", table_names)
        self.assertIn("unit_compression_settings", table_names)
        self.assertIn("unit_compress_rules", table_names)
        self.assertIn("unit_drop_modalities", table_names)
        order_indexes = {
            index["name"] for index in inspect(self.engine).get_indexes("orders")
        }
        self.assertIn("ix_orders_unit_study_uid", order_indexes)

    def test_company_id_migration_preserves_existing_units_and_is_repeatable(self):
        with self.Session() as db:
            unit = make_unit(name="Legacy company migration")
            db.add(unit)
            db.commit()
            unit_id = unit.id
        with self.engine.begin() as connection:
            connection.execute(
                text("ALTER TABLE units DROP COLUMN orders_api_company_id")
            )
        with (
            patch.object(db_module, "engine", self.engine),
            patch.object(db_module, "DATABASE_URL", "postgresql://test"),
        ):
            db_module._migrate_schema()
            db_module._migrate_schema()
        with self.engine.connect() as connection:
            row = connection.execute(
                text("SELECT name, orders_api_company_id FROM units WHERE id = :id"),
                {"id": unit_id},
            ).one()
            self.assertEqual(tuple(row), ("Legacy company migration", ""))

    def test_compression_modality_is_unique_inside_each_unit(self):
        with self.Session() as db:
            first = make_unit(name="PostgreSQL compression one")
            second = make_unit(name="PostgreSQL compression two", store_port=11113)
            db.add_all([first, second])
            db.flush()
            db.add_all(
                [
                    UnitCompressionSettings(unit_id=first.id),
                    UnitCompressionSettings(unit_id=second.id),
                    UnitCompressRule(
                        unit_id=first.id, modality="CT", jpeg_flag="lossy"
                    ),
                    UnitCompressRule(
                        unit_id=second.id, modality="CT", jpeg_flag="lossless"
                    ),
                ]
            )
            db.commit()

            self.assertEqual(
                len(
                    list(
                        db.scalars(
                            select(UnitCompressRule).where(
                                UnitCompressRule.modality == "CT"
                            )
                        )
                    )
                ),
                2,
            )
            db.add(
                UnitCompressRule(unit_id=first.id, modality="CT", jpeg_flag="lossless")
            )
            with self.assertRaises(IntegrityError):
                db.commit()
            db.rollback()

    def test_phase_migrations_upgrade_an_older_schema_repeatably(self):
        with self.Session() as db:
            unit = make_unit(name="Legacy phases migration", store_port=11140)
            db.add(unit)
            db.flush()
            db.add(UnitCompressRule(unit_id=unit.id, modality="CR", jpeg_flag="+eb"))
            db.add(UnitCompressionSettings(unit_id=unit.id, default_jpeg_flag="+e1"))
            db.commit()
            unit_id = unit.id
        with self.engine.begin() as connection:
            connection.execute(text("ALTER TABLE image_transfers DROP COLUMN sha256"))
            connection.execute(
                text(
                    "ALTER TABLE units ADD COLUMN file_settle_seconds "
                    "INTEGER NOT NULL DEFAULT 3"
                )
            )
        with (
            patch.object(db_module, "engine", self.engine),
            patch.object(db_module, "DATABASE_URL", "postgresql://test"),
        ):
            for _ in range(2):
                db_module._migrate_schema()
                db_module._migrate_compression_profiles()

        inspector = inspect(self.engine)
        transfer_columns = {c["name"] for c in inspector.get_columns("image_transfers")}
        unit_columns = {c["name"] for c in inspector.get_columns("units")}
        self.assertIn("sha256", transfer_columns)
        self.assertNotIn("file_settle_seconds", unit_columns)
        with self.engine.connect() as connection:
            self.assertEqual(
                connection.scalar(
                    text(
                        "SELECT jpeg_flag FROM unit_compress_rules WHERE unit_id = :id"
                    ),
                    {"id": unit_id},
                ),
                "lossy",
            )
            self.assertEqual(
                connection.scalar(
                    text(
                        "SELECT default_jpeg_flag FROM unit_compression_settings "
                        "WHERE unit_id = :id"
                    ),
                    {"id": unit_id},
                ),
                "lossless",
            )

    def test_skip_locked_prevents_two_workers_from_claiming_same_order(self):
        with self.Session() as db:
            unit = make_unit(name="PostgreSQL lock test")
            db.add(unit)
            db.flush()
            order = Order(
                unit_id=unit.id,
                acc="lock-test",
                birth_date="20000101",
                study_uid="1.2.3",
                status="wait_retrieve",
                retrieve_at=datetime.now(),
            )
            db.add(order)
            db.commit()
            order_id = order.id

        first = self.Session()
        second = self.Session()
        try:
            locked = first.scalar(
                select(Order)
                .where(Order.id == order_id)
                .with_for_update(skip_locked=True)
            )
            skipped = second.scalar(
                select(Order)
                .where(Order.id == order_id)
                .with_for_update(skip_locked=True)
            )
            self.assertIsNotNone(locked)
            self.assertIsNone(skipped)
        finally:
            first.rollback()
            second.rollback()
            first.close()
            second.close()


if __name__ == "__main__":
    unittest.main()
