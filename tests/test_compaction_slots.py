import os
import tempfile
import threading
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from app.config import _env_compact_workers, available_cpus, compact_workers_for
from app.pipeline.compact import FairSlots


class CompactWorkersDefaultTest(unittest.TestCase):
    def test_two_cpus_stay_free_with_at_least_one_process(self):
        self.assertEqual(
            [compact_workers_for(cpus) for cpus in (1, 2, 3, 4, 6, 8)],
            [1, 1, 1, 2, 4, 6],
        )

    def test_container_cpu_quota_limits_the_count(self):
        with tempfile.TemporaryDirectory() as root:
            cpu_max = Path(root) / "cpu.max"
            with patch("app.config.os.process_cpu_count", return_value=16):
                self.assertEqual(available_cpus(Path(root)), 16)  # no cgroup file
                cpu_max.write_text("max 100000\n")
                self.assertEqual(available_cpus(Path(root)), 16)
                cpu_max.write_text("200000 100000\n")
                self.assertEqual(available_cpus(Path(root)), 2)
                cpu_max.write_text("50000 100000\n")
                self.assertEqual(available_cpus(Path(root)), 1)

    def test_env_value_wins_and_empty_or_auto_use_the_cpus(self):
        with patch("app.config.available_cpus", return_value=6):
            for raw, expected in (("", 4), ("auto", 4), ("0", 4), ("8", 8)):
                with (
                    self.subTest(raw=raw),
                    patch.dict(os.environ, {"COMPACT_GLOBAL_WORKERS": raw}),
                ):
                    self.assertEqual(_env_compact_workers(), expected)
            with patch.dict(os.environ, clear=True):
                self.assertEqual(_env_compact_workers(), 4)


class FairSlotsTest(unittest.TestCase):
    def setUp(self):
        self.slots = FairSlots(4)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)

    def held(self) -> dict[int, int]:
        return {unit: count for unit, count in self.slots._held.items() if count}

    def take_in_thread(self, unit_id: int) -> tuple[threading.Event, threading.Event]:
        """Ask for a slot in another thread; set ``got`` once it holds one and
        release it when ``done`` is set."""
        got, done = threading.Event(), threading.Event()

        def run():
            with self.slots.slot(unit_id):
                got.set()
                done.wait(5)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(done.set)
        return got, done

    def test_a_unit_alone_uses_every_slot(self):
        for _ in range(4):
            self.stack.enter_context(self.slots.slot(1))
        self.assertEqual(self.held(), {1: 4})

    def test_a_second_unit_gets_the_next_free_slot_and_the_split_converges(self):
        for _ in range(3):
            self.stack.enter_context(self.slots.slot(1))
        last = ExitStack()
        last.enter_context(self.slots.slot(1))
        self.assertEqual(self.held(), {1: 4})

        got_b, _done_b = self.take_in_thread(2)
        self.assertFalse(got_b.wait(0.2))  # every slot is taken

        # Unit 1 asks for another one while unit 2 waits: above its share.
        got_a, _done_a = self.take_in_thread(1)
        last.close()
        self.assertTrue(got_b.wait(2))
        self.assertFalse(got_a.wait(0.2))
        self.assertEqual(self.held(), {1: 3, 2: 1})
        self.stack.close()  # lets the waiting thread finish

    def test_share_is_rounded_up_and_frees_when_a_unit_finishes(self):
        got_b, done_b = self.take_in_thread(2)
        self.assertTrue(got_b.wait(2))
        for _ in range(2):
            self.stack.enter_context(self.slots.slot(1))
        # Two units: two slots each; a third request of unit 1 waits.
        got_a, _done_a = self.take_in_thread(1)
        self.assertFalse(got_a.wait(0.2))

        done_b.set()  # unit 2 is done: unit 1 alone again
        self.assertTrue(got_a.wait(2))
        self.assertEqual(self.held(), {1: 3})


if __name__ == "__main__":
    unittest.main()
