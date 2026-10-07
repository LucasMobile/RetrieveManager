import threading
import unittest

from sqlalchemy import select

from app.config import BASE_DIR, DEFAULT_CLOUD_URL
from app.models import Unit
from app.routes.units import _store_route_problems, _unit_from_form
from app.validation import validate_unit_form
from tests.web_support import AdminWebTestCase


def unit_form(name, folder, senders, **overrides):
    root = BASE_DIR / "data" / "store-routing-test" / folder
    data = {
        "name": name,
        "enabled": "1",
        "pacs_aet": "PACS",
        "pacs_ip": "127.0.0.1",
        "pacs_port": "2104",
        "calling_aet": "MOBILEMED",
        "store_port": "445",
        "store_allowed_aets": senders,
        "orders_api_url": "https://example.test/orders",
        "orders_api_token": "integration-token",
        "orders_api_company_id": "1582",
        "retrieve_prior_enabled": "0",
        "move_timeout_prior": "1800",
        "receive_dir": str(root / "receive"),
        "send_dir": str(root / "send"),
        "error_dir": str(root / "error"),
        "token": "unit-token",
        "cloud_url": DEFAULT_CLOUD_URL,
        "move_timeout_first": "600",
        "move_timeout_update": "900",
        "max_parallel_moves": "1",
        "find_interval_seconds": "30",
        "compact_workers": "8",
        "send_workers": "16",
    }
    data.update(overrides)
    return data


class StoreRoutingFormTest(AdminWebTestCase):
    def create(self, data):
        """Submit the create form; returns the page shown after the redirect."""
        data = {**data, "csrf_token": self.token("/units/new")}
        return self.client.post("/units/new", data=data, follow_redirects=True)

    def units(self):
        with self.Session() as db:
            return {unit.name: unit for unit in db.scalars(select(Unit))}

    def test_units_share_a_port_with_disjoint_senders(self):
        self.create(unit_form("Empresa1", "a", "serverPacs1"))
        self.create(unit_form("Empresa2", "b", "serverPacs2"))

        units = self.units()
        self.assertEqual(set(units), {"Empresa1", "Empresa2"})
        page = self.client.get(f"/units/{units['Empresa1'].id}")
        self.assertIn("compartilhados com: Empresa2", page.text)
        self.assertNotIn("Recebimento bloqueado", page.text)

    def test_overlapping_sender_is_refused_with_the_other_unit_named(self):
        self.create(unit_form("Empresa1", "a", "serverPacs1"))
        page = self.create(unit_form("Empresa2", "b", "SERVERPACS1,serverPacs2"))

        self.assertEqual(set(self.units()), {"Empresa1"})
        self.assertIn("SERVERPACS1", page.text)
        self.assertIn("Empresa1", page.text)

    def test_shared_port_without_sender_list_is_refused_on_either_side(self):
        self.create(unit_form("Empresa1", "a", ""))
        self.create(unit_form("Empresa2", "b", "serverPacs2"))
        self.create(unit_form("Empresa3", "c", "serverPacs3", store_port="446"))
        self.create(unit_form("Empresa4", "d", "", store_port="446"))

        self.assertEqual(set(self.units()), {"Empresa1", "Empresa3"})

    def test_folders_of_another_unit_are_refused(self):
        self.create(unit_form("Empresa1", "a", "", store_port="444"))
        other = unit_form("Empresa2", "b", "")
        other["error_dir"] = unit_form("x", "a", "")["receive_dir"]
        page = self.create(other)

        self.assertEqual(set(self.units()), {"Empresa1"})
        self.assertIn("Empresa1", page.text)

    def test_editing_rechecks_against_the_other_units(self):
        self.create(unit_form("Empresa1", "a", "serverPacs1"))
        self.create(unit_form("Empresa2", "b", "serverPacs2"))
        unit_id = self.units()["Empresa2"].id
        data = unit_form("Empresa2", "b", "serverPacs1")
        data["csrf_token"] = self.token(f"/units/{unit_id}")

        self.client.post(f"/units/{unit_id}", data=data, follow_redirects=False)

        self.assertEqual(self.units()["Empresa2"].store_allowed_aets, "serverPacs2")

    def test_concurrent_saves_cannot_both_pass_the_check(self):
        first = validate_unit_form(
            unit_form("Empresa1", "a", "serverPacs1"), creating=True
        )
        second = validate_unit_form(
            unit_form("Empresa2", "b", "SERVERPACS1"), creating=True
        )
        result: dict[str, list[str]] = {}

        def save_second():
            with self.Session() as db:
                result["problems"] = _store_route_problems(db, second, None)

        with self.Session() as db:
            self.assertEqual(_store_route_problems(db, first, None), [])
            db.add(_unit_from_form(first, None))
            db.flush()
            thread = threading.Thread(target=save_second)
            thread.start()
            thread.join(timeout=0.5)
            # Waits on the advisory lock until the first save commits.
            self.assertTrue(thread.is_alive())
            db.commit()
        thread.join(timeout=10)

        self.assertFalse(thread.is_alive())
        self.assertTrue(result["problems"])
        self.assertIn("Empresa1", result["problems"][0])


if __name__ == "__main__":
    unittest.main()
