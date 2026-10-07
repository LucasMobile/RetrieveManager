import json
import logging
import unittest
from datetime import timedelta, timezone
from unittest.mock import patch

from app.observability import (
    JsonFormatter,
    configure_logging,
    log_context,
    log_event,
)


class ObservabilityTest(unittest.TestCase):
    def test_json_log_has_required_context(self):
        record = logging.LogRecord(
            "worker",
            logging.INFO,
            __file__,
            1,
            "cloud.upload",
            (),
            None,
        )
        record.action = "cloud.upload"
        record.resource = "transfer:7"
        record.status = "success"
        record.duration_ms = 12.4
        with log_context("corr-123", user_id=9):
            payload = json.loads(JsonFormatter().format(record))

        self.assertEqual(payload["correlation_id"], "corr-123")
        self.assertEqual(payload["user_id"], 9)
        self.assertEqual(payload["action"], "cloud.upload")
        self.assertIn("timestamp", payload)
        self.assertIn("error_type", payload)

    def test_json_log_uses_configured_timezone(self):
        record = logging.LogRecord(
            "worker", logging.INFO, __file__, 1, "test", (), None
        )
        record.created = 1789328029.174766
        with patch("app.observability.LOG_TIMEZONE", timezone(timedelta(hours=-3))):
            payload = json.loads(JsonFormatter().format(record))

        self.assertTrue(payload["timestamp"].endswith("-03:00"))

    def test_reserved_log_fields_are_safely_prefixed(self):
        records = []

        class RecordingHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("test.reserved_fields")
        logger.handlers = [RecordingHandler()]
        logger.setLevel(logging.INFO)
        logger.propagate = False

        log_event(
            logger,
            logging.INFO,
            "test.event",
            resource="test",
            status="success",
            created=True,
        )

        self.assertEqual(len(records), 1)
        self.assertTrue(records[0].event_created)

    def test_error_detail_is_derived_from_the_error_unless_given(self):
        records = []

        class RecordingHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("test.error_detail")
        logger.handlers = [RecordingHandler()]
        logger.setLevel(logging.INFO)
        logger.propagate = False
        error = RuntimeError("line one\nline two")

        log_event(logger, logging.ERROR, "a", status="failure", error=error)
        log_event(
            logger,
            logging.ERROR,
            "b",
            status="failure",
            error=error,
            error_detail="HTTP 401",
        )
        log_event(
            logger, logging.ERROR, "c", status="failure", error=error, error_detail=None
        )
        log_event(logger, logging.INFO, "d", status="success")

        derived, explicit, omitted, success = records
        self.assertEqual(derived.error_detail, "line one line two")
        self.assertEqual(explicit.error_detail, "HTTP 401")
        self.assertIsNone(omitted.error_detail)
        self.assertFalse(hasattr(success, "error_detail"))

    def test_exception_stack_has_locations_without_patient_or_secret_text(self):
        try:
            raise RuntimeError("patient=PRIVATE; token=SECRET")
        except RuntimeError:
            import sys

            record = logging.LogRecord(
                "worker",
                logging.ERROR,
                __file__,
                1,
                "upload.failed",
                (),
                sys.exc_info(),
            )
        rendered = JsonFormatter().format(record)
        payload = json.loads(rendered)
        self.assertTrue(payload["error_stack"])
        self.assertEqual(payload["error_stack"][-1]["file"], "test_observability.py")
        self.assertNotIn("PRIVATE", rendered)
        self.assertNotIn("SECRET", rendered)

    def test_pynetdicom_logs_only_warnings_in_every_service(self):
        # Two INFO lines per image of a C-MOVE filled the Docker log in minutes.
        root = logging.getLogger()
        handlers, level = root.handlers[:], root.level
        pynetdicom = logging.getLogger("pynetdicom")
        previous = pynetdicom.level
        self.addCleanup(pynetdicom.setLevel, previous)
        self.addCleanup(root.setLevel, level)
        self.addCleanup(setattr, root, "handlers", handlers)
        pynetdicom.setLevel(logging.NOTSET)

        configure_logging()

        association = logging.getLogger("pynetdicom.association")
        self.assertFalse(association.isEnabledFor(logging.INFO))
        self.assertTrue(association.isEnabledFor(logging.WARNING))


if __name__ == "__main__":
    unittest.main()
