import unittest
from datetime import datetime
from uuid import uuid4

from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from app.db import (
    _SCHEMA_MAINTENANCE_LOCK,
    AUTOVACUUM_SETTINGS,
    BIGINT_COLUMNS,
    OBSOLETE_POSTGRESQL_INDEXES,
    POSTGRESQL_INDEXES,
    ensure_postgresql_schema,
)
from app.models import Base, Order, UnitCompressionSettings, UnitCompressRule
from tests.support import _postgres_test_url, make_unit


class PostgreSQLRetrieveIntegrationTest(unittest.TestCase):
    """Checks for the database's locking and schema contract."""

    @classmethod
    def setUpClass(cls):
        database_url = _postgres_test_url()

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


class SchemaMaintenanceTest(unittest.TestCase):
    """ensure_postgresql_schema brings an existing database to the current set."""

    def setUp(self):
        database_url = _postgres_test_url()
        self.schema = f"retrieve_test_{uuid4().hex}"
        self.admin_engine = create_engine(database_url)
        with self.admin_engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{self.schema}"'))
        # public stays on the path for pg_trgm when it is installed there.
        self.engine = create_engine(
            database_url,
            connect_args={"options": f"-csearch_path={self.schema},public"},
        )
        Base.metadata.create_all(self.engine)
        self.addCleanup(self._drop)

    def _drop(self):
        self.engine.dispose()
        with self.admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{self.schema}" CASCADE'))
        self.admin_engine.dispose()

    def indexes(self) -> dict[str, bool]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT c.relname, i.indisvalid FROM pg_index i "
                    "JOIN pg_class c ON c.oid = i.indexrelid "
                    "WHERE c.relnamespace = current_schema()::regnamespace"
                )
            )
            return {name: valid for name, valid in rows}

    def test_existing_database_converges_and_reruns_are_harmless(self):
        with self.engine.begin() as connection:
            # Indexes an earlier version created.
            connection.execute(
                text(
                    "CREATE INDEX ix_dicom_instance_unit_sop "
                    "ON dicom_instances (unit_id, sop_uid)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX ix_dicom_instance_unit_path "
                    "ON dicom_instances (unit_id, source_path)"
                )
            )
            connection.execute(
                text("CREATE INDEX ix_orders_correlation_id ON orders (correlation_id)")
            )
            connection.execute(
                text("CREATE INDEX ix_orders_active_id ON orders (archived_at, id)")
            )
            # Integer ids, as created before they became bigint.
            for table, columns in BIGINT_COLUMNS.items():
                changes = ", ".join(f"ALTER COLUMN {c} TYPE integer" for c in columns)
                connection.execute(text(f"ALTER TABLE {table} {changes}"))
                connection.execute(text(f"ALTER SEQUENCE {table}_id_seq AS integer"))
        ensure_postgresql_schema(self.engine)

        # A build interrupted halfway leaves an INVALID index behind.
        with self.engine.begin() as connection:
            superuser = connection.scalar(
                text("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
            )
            if superuser:
                connection.execute(
                    text(
                        "UPDATE pg_index SET indisvalid = false "
                        "WHERE indexrelid = 'ix_historical_study_unit_uid'::regclass"
                    )
                )
        ensure_postgresql_schema(self.engine)
        ensure_postgresql_schema(self.engine)

        indexes = self.indexes()
        for name, _definition in POSTGRESQL_INDEXES:
            self.assertIs(indexes.get(name), True, name)
        for name in OBSOLETE_POSTGRESQL_INDEXES:
            self.assertNotIn(name, indexes)
        # Kept: integrity, and the history filter by status.
        self.assertIn("uq_dicom_instance_content", indexes)
        self.assertIn("ix_orders_status", indexes)

        with self.engine.connect() as connection:
            for table, columns in BIGINT_COLUMNS.items():
                types = dict(
                    connection.execute(
                        text(
                            "SELECT column_name, data_type "
                            "FROM information_schema.columns "
                            "WHERE table_schema = current_schema() "
                            "AND table_name = :t"
                        ),
                        {"t": table},
                    ).all()
                )
                for column in columns:
                    self.assertEqual(types[column], "bigint", f"{table}.{column}")
                # The sequence goes past the integer limit.
                connection.execute(text(f"SELECT setval('{table}_id_seq', 3000000000)"))
                self.assertEqual(
                    connection.scalar(text(f"SELECT nextval('{table}_id_seq')")),
                    3000000001,
                )
            for table, settings in AUTOVACUUM_SETTINGS.items():
                options = connection.scalar(
                    text("SELECT reloptions FROM pg_class WHERE oid = to_regclass(:t)"),
                    {"t": table},
                )
                for key, value in settings.items():
                    self.assertIn(f"{key}={value}", options, table)
            # The maintenance lock was released.
            self.assertTrue(
                connection.scalar(
                    text("SELECT pg_try_advisory_lock(:key)"),
                    {"key": _SCHEMA_MAINTENANCE_LOCK},
                )
            )
            connection.execute(
                text("SELECT pg_advisory_unlock(:key)"),
                {"key": _SCHEMA_MAINTENANCE_LOCK},
            )


if __name__ == "__main__":
    unittest.main()
