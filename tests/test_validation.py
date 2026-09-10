import unittest

from app.config import BASE_DIR, store_bind_port
from app.validation import (
    validate_cloud_url,
    validate_jpeg_flag,
    validate_unit_form,
)


class ValidationTest(unittest.TestCase):
    def test_external_store_port_maps_to_unprivileged_listener(self):
        self.assertEqual(store_bind_port(444), 10444)
        self.assertEqual(store_bind_port(445), 445)

    def test_valid_unit_form_is_normalized(self):
        root = str(BASE_DIR)
        form = {
            "name": "Hospital A",
            "enabled": "1",
            "pacs_aet": "PACS",
            "pacs_ip": "127.0.0.1",
            "pacs_port": "2104",
            "calling_aet": "RETRIEVE",
            "dest_aet": "RETRIEVE",
            "store_port": "444",
            "input_dir": root,
            "sent_dir": root,
            "receive_dir": root,
            "send_dir": root,
            "error_dir": root,
            "token": "unit-token",
            "move_timeout_first": "600",
            "move_timeout_second": "900",
            "max_parallel_moves": "2",
            "find_interval_seconds": "30",
            "compact_workers": "4",
            "send_workers": "8",
        }
        result = validate_unit_form(form)
        self.assertEqual(result["pacs_port"], 2104)
        self.assertEqual(result["name"], "Hospital A")

    def test_rejects_unsafe_values(self):
        with self.assertRaises(ValueError):
            validate_jpeg_flag("--arbitrary-option")
        with self.assertRaises(ValueError):
            validate_cloud_url("http://169.254.169.254/latest/meta-data")


if __name__ == "__main__":
    unittest.main()
