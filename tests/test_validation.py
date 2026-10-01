import unittest

from app.config import BASE_DIR, store_bind_port
from app.validation import (
    validate_cloud_url,
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
            "store_port": "444",
            "orders_api_url": "https://integracao.example/v1/pedidos",
            "orders_api_token": "integration-token",
            "orders_api_station_id": "48",
            "orders_api_company_id": " 4232 ",
            "retrieve_prior_enabled": "1",
            "move_timeout_prior": "1800",
            "receive_dir": root,
            "send_dir": root,
            "error_dir": root,
            "token": "unit-token",
            "cloud_url": "https://idr.mobilemed.com.br/api/router/send-image",
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
        self.assertEqual(result["orders_api_station_id"], "48")
        self.assertEqual(result["orders_api_company_id"], "4232")
        self.assertTrue(result["retrieve_prior_enabled"])
        self.assertEqual(result["move_timeout_prior"], 1800)
        self.assertEqual(
            result["cloud_url"], "https://idr.mobilemed.com.br/api/router/send-image"
        )

        editable_form = {**form, "token": "", "orders_api_token": ""}
        result = validate_unit_form(editable_form)
        self.assertEqual(result["token"], "")
        with self.assertRaisesRegex(ValueError, "Token da unidade é obrigatório"):
            validate_unit_form(editable_form, creating=True)

        for creating in (False, True):
            for invalid in ("", "0", "-1", "4.2", "company", "1" * 65):
                with (
                    self.subTest(creating=creating, company_id=invalid),
                    self.assertRaisesRegex(ValueError, "Empresa ID"),
                ):
                    validate_unit_form(
                        {**form, "orders_api_company_id": invalid},
                        creating=creating,
                    )

    def test_rejects_unsafe_values(self):
        with self.assertRaises(ValueError):
            validate_cloud_url("http://169.254.169.254/latest/meta-data")


if __name__ == "__main__":
    unittest.main()
