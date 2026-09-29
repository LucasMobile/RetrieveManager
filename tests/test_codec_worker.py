import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.codec_worker import CodecCrash, CodecPool, CodecTimeout
from app.compaction import CompactResult
from app.pipeline.compact import _compact_one


class CodecPoolTest(unittest.TestCase):
    def pool(self, function):
        pool = CodecPool(f"tests.codec_targets:{function}", size=1)
        self.addCleanup(pool.close)
        return pool

    def test_worker_is_isolated_and_reused(self):
        pool = self.pool("echo")
        first_pid, value = pool.run({"a": 1}, timeout=30)
        second_pid, _ = pool.run("again", timeout=30)
        self.assertEqual(value, {"a": 1})
        self.assertNotEqual(first_pid, os.getpid())
        self.assertEqual(first_pid, second_pid)

    def test_crash_is_reported_and_next_job_gets_a_fresh_worker(self):
        pool = self.pool("crash")
        first = pool.run("ok", timeout=30)
        with self.assertRaises(CodecCrash):
            pool.run("crash", timeout=30)
        self.assertNotEqual(pool.run("ok", timeout=30), first)

    def test_timeout_kills_the_worker(self):
        pool = self.pool("slow")
        first = pool.run(0, timeout=30)
        with self.assertRaises(CodecTimeout):
            pool.run(10, timeout=0.5)
        self.assertNotEqual(pool.run(0, timeout=30), first)

    def test_job_exception_does_not_hang(self):
        with self.assertRaisesRegex(RuntimeError, "ValueError"):
            self.pool("raises").run("job", timeout=30)


class CompactCrashTest(unittest.TestCase):
    def compact(self, side_effect):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        directory = Path(temporary.name)
        (directory / "send").mkdir()
        source = directory / "CT.1"
        source.write_bytes(b"original")
        with patch(
            "app.pipeline.compact._codec_pool.run", side_effect=side_effect
        ) as run:
            result = _compact_one(
                str(source),
                str(directory / "send" / "CT.1.dcm"),
                str(directory / "error" / "CT.1"),
                str(directory / "work"),
                "token",
                set(),
                {"*": "lossless"},
                (),
            )
        return result, run, source, directory

    def test_one_crash_is_retried_on_a_fresh_worker(self):
        calls = []

        def succeed_second_time(job, timeout):
            calls.append(job)
            if len(calls) == 1:
                raise CodecCrash
            Path(job.temp_output).write_bytes(b"encoded")
            return CompactResult("CT.1", "CT.1.dcm", "1.2", "compressed")

        result, run, source, directory = self.compact(succeed_second_time)
        self.assertEqual(result.status, "compressed")
        self.assertEqual(run.call_count, 2)
        # Published later, after the transfer row is committed.
        self.assertEqual(source.read_bytes(), b"original")

    def test_repeated_crash_falls_back_to_sending_as_received(self):
        def crash_unless_copy(job, timeout):
            if not job.force_copy:
                raise CodecCrash
            Path(job.temp_output).write_bytes(b"original")
            return CompactResult(
                "CT.1", "CT.1.dcm", "1.2", "compressed", codec_method="copy"
            )

        result, run, source, _directory = self.compact(crash_unless_copy)
        self.assertEqual(result.status, "compressed")
        self.assertEqual(result.codec_reason, "fallback:CodecCrash")
        self.assertEqual(run.call_count, 3)
        self.assertTrue(source.exists())  # published after the commit

    def test_crash_also_in_copy_mode_quarantines_the_untouched_source(self):
        result, run, source, directory = self.compact(CodecCrash)
        self.assertEqual(
            (result.status, result.error_type), ("compression_error", "CodecCrash")
        )
        self.assertEqual(run.call_count, 4)
        self.assertEqual((directory / "error" / "CT.1").read_bytes(), b"original")
        self.assertFalse(source.exists())


if __name__ == "__main__":
    unittest.main()
