import logging
import unittest

from sqlalchemy import create_engine

from app.db import watch_pool_saturation
from tests.support import _postgres_test_url


class PoolSaturationTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(_postgres_test_url(), pool_size=2, max_overflow=0)
        self.addCleanup(self.engine.dispose)
        watch_pool_saturation(self.engine, 2)

    def test_last_free_connection_is_logged_once_per_interval(self):
        logger = logging.getLogger("db")
        with self.assertNoLogs(logger, "WARNING"):
            first = self.engine.connect()
        self.addCleanup(first.close)

        with self.assertLogs(logger, "WARNING") as logs:
            second = self.engine.connect()
        second.close()
        (record,) = logs.records
        self.assertEqual(record.action, "db.pool.saturated")
        self.assertEqual((record.checked_out, record.capacity), (2, 2))

        # Saturated again right away: counted, not logged.
        with self.assertNoLogs(logger, "WARNING"):
            self.engine.connect().close()


if __name__ == "__main__":
    unittest.main()
