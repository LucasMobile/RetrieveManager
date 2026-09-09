import json
import logging
import unittest

from app.observability import JsonFormatter, log_context, log_event


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


if __name__ == "__main__":
    unittest.main()
