import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from app.models import Order
from app.pipeline.find import find_pending
from app.worker import _find_job, stop_event
from tests.support import DatabaseTestCase, make_unit


class FindBackoffTest(DatabaseTestCase):
    """Orders the PACS has not answered yet are searched less often as they age."""

    def searched(self, orders, *, interval=30):
        with self.Session() as db:
            unit = make_unit(name=f"unit-{interval}", find_interval_seconds=interval)
            db.add(unit)
            db.flush()
            for acc, created_ago, searched_ago in orders:
                now = datetime.now()
                db.add(
                    Order(
                        unit_id=unit.id,
                        acc=acc,
                        birth_date="20000101",
                        status="watching",
                        created_at=now - created_ago,
                        last_find_at=None
                        if searched_ago is None
                        else now - searched_ago,
                    )
                )
            db.commit()
            found = []
            with patch(
                "app.pipeline.find._find_one",
                side_effect=lambda _db, _unit, order, _now: found.append(order.acc),
            ):
                find_pending(db, unit, max_orders=len(orders))
        return set(found)

    def test_waits_grow_with_the_age_of_the_order(self):
        s = timedelta(seconds=1)
        found = self.searched(
            [
                ("fresh-due", 5 * 60 * s, 40 * s),
                ("half-hour-early", 30 * 60 * s, 40 * s),
                ("half-hour-due", 30 * 60 * s, 70 * s),
                ("old-early", 3 * 3600 * s, 70 * s),
                ("old-due", 3 * 3600 * s, 130 * s),
                ("old-never-searched", 3 * 3600 * s, None),
            ]
        )
        self.assertEqual(
            found, {"fresh-due", "half-hour-due", "old-due", "old-never-searched"}
        )

    def test_long_waits_are_capped(self):
        minute = timedelta(minutes=1)
        # 4 x 2 min would be 8 min; the cap keeps it at 5 min.
        found = self.searched(
            [("old-capped", 3 * 60 * minute, 6 * minute)], interval=120
        )
        self.assertEqual(found, {"old-capped"})
        # An interval above the cap is never shortened.
        found = self.searched(
            [
                ("old-slow-unit-early", 3 * 60 * minute, 9 * minute),
                ("old-slow-unit-due", 3 * 60 * minute, 11 * minute),
            ],
            interval=600,
        )
        self.assertEqual(found, {"old-slow-unit-due"})


class FindJobTest(unittest.TestCase):
    def setUp(self):
        stop_event.clear()

    def test_slot_keeps_searching_while_orders_are_due(self):
        with patch("app.worker._run_unit_stage", side_effect=[1, 1, 0]) as run:
            _find_job(7)
        self.assertEqual(run.call_count, 3)

    def test_slot_returns_after_its_batch(self):
        with (
            patch("app.worker.FIND_BATCH_SIZE", 4),
            patch("app.worker._run_unit_stage", return_value=1) as run,
        ):
            _find_job(7)
        self.assertEqual(run.call_count, 4)

    def test_slot_stops_on_shutdown(self):
        stop_event.set()
        self.addCleanup(stop_event.clear)
        with patch("app.worker._run_unit_stage", return_value=1) as run:
            _find_job(7)
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
