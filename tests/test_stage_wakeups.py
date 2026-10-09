import logging
import threading
import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from sqlalchemy import text

from app.db import INSTANCES_RECEIVED_CHANNEL
from app.notify import NotificationListener
from app.wakeup import COMPACT, SEND, Wakeups
from app.worker import _dispatch_wakeups, _received_listener_loop, stop_event
from tests.support import DatabaseTestCase


class WakeupsTest(unittest.TestCase):
    def test_requests_are_taken_once(self):
        wakeups = Wakeups()
        self.assertFalse(wakeups.wait(0))
        wakeups.request(COMPACT, 1)
        wakeups.request(COMPACT, 1)
        wakeups.request(SEND, 2)
        self.assertTrue(wakeups.wait(0))
        self.assertEqual(wakeups.take(), {COMPACT: {1}, SEND: {2}})
        self.assertFalse(wakeups.wait(0))
        self.assertEqual(wakeups.take(), {})

    def test_deferred_request_returns_when_the_job_ends(self):
        wakeups = Wakeups()
        job = Future()
        wakeups.request_after(SEND, 3, job)
        wakeups.request_after(SEND, 3, job)  # kept once
        self.assertEqual(wakeups.take(), {})

        job.set_result(None)
        self.assertEqual(wakeups.take(), {SEND: {3}})
        self.assertEqual(wakeups.take(), {})


class DispatchTest(unittest.TestCase):
    def setUp(self):
        stop_event.clear()
        self.wakeups = Wakeups()
        patcher = patch("app.worker.wakeups", self.wakeups)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_requested_stage_starts_without_waiting_for_the_tick(self):
        compact_pool, send_pool = MagicMock(), MagicMock()
        send_pool.submit.return_value = Future()

        _dispatch_wakeups({SEND: {5}}, compact_pool, {}, send_pool, {})

        compact_pool.submit.assert_not_called()
        self.assertEqual(send_pool.submit.call_args.args[1], 5)

    def test_running_job_is_started_again_when_it_ends(self):
        running = Future()
        compact_jobs = {5: running}
        compact_pool = MagicMock()
        compact_pool.submit.return_value = Future()

        _dispatch_wakeups({COMPACT: {5}}, compact_pool, compact_jobs, MagicMock(), {})
        compact_pool.submit.assert_not_called()

        # It may have read the queue before the new instances were committed.
        running.set_result(False)
        requests = self.wakeups.take()
        self.assertEqual(requests, {COMPACT: {5}})
        _dispatch_wakeups(requests, compact_pool, compact_jobs, MagicMock(), {})
        compact_pool.submit.assert_called_once()


class ReceivedListenerTest(DatabaseTestCase):
    def test_receiver_notify_requests_the_compaction_of_its_unit(self):
        wakeups = Wakeups()
        listener = NotificationListener(
            INSTANCES_RECEIVED_CHANNEL,
            logger=logging.getLogger("test"),
            action="test.listen",
        )
        # Connect and LISTEN before the notification is sent.
        self.assertIsNone(listener.wait(threading.Event(), 0.1))
        stop_event.clear()
        self.addCleanup(stop_event.clear)
        with patch("app.worker.wakeups", wakeups):
            thread = threading.Thread(
                target=_received_listener_loop, args=(listener,), daemon=True
            )
            thread.start()
            with self.engine.begin() as connection:
                connection.execute(
                    text("SELECT pg_notify(:channel, '42')"),
                    {"channel": INSTANCES_RECEIVED_CHANNEL},
                )
            woke = wakeups.wait(5)
            stop_event.set()
            thread.join(5)

        self.assertTrue(woke)
        self.assertEqual(wakeups.take(), {COMPACT: {42}})
        self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
