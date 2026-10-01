import unittest
from datetime import datetime
from uuid import uuid4

from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

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


if __name__ == "__main__":
    unittest.main()
