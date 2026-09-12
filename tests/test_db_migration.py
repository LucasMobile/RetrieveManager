import tempfile
import unittest
from pathlib import Path

from sqlalchemy import create_engine, inspect, text

from app import db as db_module


class DatabaseMigrationTest(unittest.TestCase):
    def test_removes_legacy_move_destination_column(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.db"
            engine = create_engine(f"sqlite:///{path.as_posix()}")
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "CREATE TABLE units ("
                        "id INTEGER PRIMARY KEY, "
                        "name VARCHAR(120) NOT NULL, "
                        "dest_aet VARCHAR(64) NOT NULL"
                        ")"
                    )
                )
                connection.execute(
                    text(
                        "INSERT INTO units (name, dest_aet) "
                        "VALUES ('Unidade antiga', 'RETRIEVE')"
                    )
                )

            original_engine = db_module.engine
            original_url = db_module.DATABASE_URL
            try:
                db_module.engine = engine
                db_module.DATABASE_URL = f"sqlite:///{path.as_posix()}"
                db_module._migrate_schema()
            finally:
                db_module.engine = original_engine
                db_module.DATABASE_URL = original_url

            columns = {
                column["name"] for column in inspect(engine).get_columns("units")
            }
            self.assertNotIn("dest_aet", columns)
            self.assertIn("orders_api_url", columns)
            self.assertIn("orders_api_token", columns)
            self.assertIn("orders_api_station_id", columns)
            self.assertIn("retrieve_prior_enabled", columns)
            self.assertIn("move_timeout_prior", columns)
            with engine.connect() as connection:
                self.assertEqual(
                    connection.scalar(text("SELECT name FROM units")),
                    "Unidade antiga",
                )
            engine.dispose()


if __name__ == "__main__":
    unittest.main()
