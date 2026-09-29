import os
import re
import subprocess
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from starlette.requests import Request
from starlette.testclient import TestClient

from app.config import BASE_DIR, DEFAULT_CLOUD_URL
from app.db import get_db
from app.dicom_net import EchoResult
from app.main import app
from app.middleware import _same_origin
from app.models import (
    AuditLog,
    Base,
    DicomInstance,
    DicomRule,
    DicomStudy,
    ImageTransfer,
    ManualMoveRequest,
    Order,
    Settings,
    Unit,
    UnitCompressRule,
    UnitDropModality,
    User,
)
from app.rate_limit import (
    global_rate_limiter,
    login_failure_rate_limiter,
    reset_rate_limiters,
)
from app.security import hash_password, verify_password
from app.validation import validate_cloud_url


class SecurityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.password = "test-password-123"
        cls.password_hash = hash_password(cls.password)

    def setUp(self):
        reset_rate_limiters()
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(self.engine)
        with self.Session() as db:
            db.add(
                User(
                    username="tester",
                    password_hash=self.password_hash,
                    role="admin",
                )
            )
            db.add(Settings(id=1))
            db.commit()

        def database():
            with self.Session() as db:
                yield db

        self.previous_overrides = app.dependency_overrides.copy()
        app.dependency_overrides[get_db] = database
        # Do not run lifespan: it initializes the deployment database.
        self.client = TestClient(
            app,
            base_url="https://testserver",
            raise_server_exceptions=False,
        )

    def tearDown(self):
        self.client.close()
        reset_rate_limiters()
        app.dependency_overrides.clear()
        app.dependency_overrides.update(self.previous_overrides)
        self.engine.dispose()

    def token(self, path="/login"):
        response = self.client.get(path)
        self.assertEqual(response.status_code, 200)
        return re.search(r'name="csrf_token" value="([^"]+)"', response.text)[1]

    def login_credentials(self, username: str, password: str):
        token = self.token()
        response = self.client.post(
            "/login",
            data={
                "username": username,
                "password": password,
                "csrf_token": token,
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        return token

    def login(self):
        return self.login_credentials("tester", self.password)

    def test_failed_login_is_blocked_after_five_attempts_for_the_ip(self):
        token = self.token()
        self.assertEqual(login_failure_rate_limiter.limit, 5)
        for expected_remaining in range(4, -1, -1):
            response = self.client.post(
                "/login",
                data={
                    "username": "tester",
                    "password": "senha-incorreta",
                    "csrf_token": token,
                },
            )
            self.assertEqual(response.status_code, 401)
            self.assertEqual(response.headers["x-ratelimit-limit"], "5")
            self.assertEqual(
                response.headers["x-ratelimit-remaining"], str(expected_remaining)
            )
            self.assertEqual(response.headers["x-ratelimit-scope"], "failed-login")

        blocked = self.client.post(
            "/login",
            data={
                "username": "tester",
                "password": self.password,
                "csrf_token": token,
            },
        )
        self.assertEqual(blocked.status_code, 429)
        self.assertEqual(blocked.headers["x-ratelimit-remaining"], "0")
        self.assertIn("retry-after", blocked.headers)

    def test_successful_login_does_not_erase_the_ip_failure_window(self):
        token = self.token()
        for _ in range(2):
            self.client.post(
                "/login",
                data={
                    "username": "tester",
                    "password": "senha-incorreta",
                    "csrf_token": token,
                },
            )
        response = self.client.post(
            "/login",
            data={
                "username": "tester",
                "password": self.password,
                "csrf_token": token,
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["x-ratelimit-remaining"], "3")

    def test_global_rate_limit_returns_429_and_standard_headers(self):
        previous_limit = global_rate_limiter.limit
        global_rate_limiter.limit = 2
        try:
            first = self.client.get("/health/live")
            second = self.client.get("/health/live")
            blocked = self.client.get("/health/live")
        finally:
            global_rate_limiter.limit = previous_limit
            global_rate_limiter.reset()

        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.headers["x-ratelimit-limit"], "2")
        self.assertEqual(first.headers["x-ratelimit-remaining"], "1")
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.headers["x-ratelimit-remaining"], "0")
        self.assertEqual(blocked.status_code, 429)
        self.assertEqual(blocked.headers["x-ratelimit-scope"], "global")
        self.assertEqual(blocked.headers["retry-after"], "60")

    def unit_form_data(self, **overrides):
        root = str(BASE_DIR)
        data = {
            "name": "Unidade de teste",
            "enabled": "1",
            "pacs_aet": "PACS",
            "pacs_ip": "127.0.0.1",
            "pacs_port": "2104",
            "calling_aet": "RETRIEVE",
            "store_port": "444",
            "orders_api_url": "https://example.test/orders",
            "orders_api_token": "integration-token",
            "orders_api_station_id": "",
            "orders_api_company_id": "1582",
            "retrieve_prior_enabled": "0",
            "move_timeout_prior": "1800",
            "receive_dir": root,
            "send_dir": root,
            "error_dir": root,
            "token": "unit-token",
            "cloud_url": DEFAULT_CLOUD_URL,
            "move_timeout_first": "600",
            "move_timeout_second": "900",
            "max_parallel_moves": "1",
            "find_interval_seconds": "30",
            "compact_workers": "8",
            "send_workers": "16",
        }
        data.update(overrides)
        return data

    def test_all_post_routes_require_csrf(self):
        self.login()
        for route in app.routes:
            if "POST" not in getattr(route, "methods", set()):
                continue
            path = re.sub(r"\{[^}]+\}", "1", route.path)
            with self.subTest(path=path):
                self.assertEqual(self.client.post(path).status_code, 403)

    def test_token_rotation_session_binding_and_header_transport(self):
        old = self.login()
        token = self.token("/settings")
        self.assertNotEqual(old, token)
        for invalid in (old, "invalid", "á"):
            self.assertEqual(
                self.client.post(
                    "/logout",
                    data={"csrf_token": invalid},
                ).status_code,
                403,
            )
        other = TestClient(app, base_url="https://testserver")
        try:
            other.get("/login")
            self.assertEqual(
                other.post("/logout", data={"csrf_token": token}).status_code, 403
            )
        finally:
            other.close()
        response = self.client.post(
            "/logout",
            headers={"X-CSRF-Token": token},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(
            self.client.post("/logout", data={"csrf_token": token}).status_code, 403
        )

    def test_pacs_wildcard_and_store_allowlist_default_off_and_round_trip(self):
        self.login()
        token = self.token("/units/new")
        data = self.unit_form_data(csrf_token=token)
        self.client.post("/units/new", data=data)
        with self.Session() as db:
            unit = db.scalar(select(Unit))
            unit_id = unit.id
            self.assertFalse(unit.pacs_patient_id_wildcard)
            self.assertEqual(unit.store_allowed_aets, "")
            self.assertEqual(unit.store_allowed_ips, "")

        self.client.post(
            f"/units/{unit_id}",
            data={
                **data,
                "pacs_patient_id_wildcard": "1",
                "store_allowed_aets": " srvPACS , SRVPACS;MODALIDADE\n",
                "store_allowed_ips": "192.168.3.103, 10.10.0.0/24",
            },
        )
        with self.Session() as db:
            unit = db.get(Unit, unit_id)
            self.assertTrue(unit.pacs_patient_id_wildcard)
            self.assertEqual(unit.store_allowed_aets, "srvPACS,MODALIDADE")
            self.assertEqual(unit.store_allowed_ips, "192.168.3.103,10.10.0.0/24")
        edit_page = self.client.get(f"/units/{unit_id}").text
        self.assertIn('value="192.168.3.103,10.10.0.0/24"', edit_page)
        self.assertRegex(
            edit_page, r'name="pacs_patient_id_wildcard"[^>]*value="1"[^>]*checked'
        )
        self.assertIn('value="srvPACS,MODALIDADE"', edit_page)

        # Unchecked checkbox is omitted by the browser: the option turns off.
        self.client.post(
            f"/units/{unit_id}",
            data={**data, "store_allowed_aets": "PACS*"},
        )
        self.client.post(
            f"/units/{unit_id}",
            data={**data, "store_allowed_ips": "192.168.3.300"},
        )
        with self.Session() as db:
            unit = db.get(Unit, unit_id)
            self.assertTrue(unit.pacs_patient_id_wildcard)
            self.assertEqual(unit.store_allowed_aets, "srvPACS,MODALIDADE")
            self.assertEqual(unit.store_allowed_ips, "192.168.3.103,10.10.0.0/24")
        self.client.post(f"/units/{unit_id}", data=data)
        with self.Session() as db:
            unit = db.get(Unit, unit_id)
            self.assertFalse(unit.pacs_patient_id_wildcard)
            self.assertEqual(unit.store_allowed_aets, "")
            self.assertEqual(unit.store_allowed_ips, "")

    def test_company_id_required_on_create_and_edit_and_saved_in_form(self):
        self.login()
        token = self.token("/units/new")
        data = self.unit_form_data(csrf_token=token, orders_api_company_id="")
        self.client.post("/units/new", data=data)
        with self.Session() as db:
            self.assertIsNone(db.scalar(select(Unit)))
        self.client.post("/units/new", data={**data, "orders_api_company_id": "1582"})
        with self.Session() as db:
            unit = db.scalar(select(Unit))
            unit_id = unit.id
            self.assertEqual(unit.orders_api_company_id, "1582")
        edit_page = self.client.get(f"/units/{unit_id}")
        company_input = re.search(
            r'<input[^>]+name="orders_api_company_id"[^>]*>', edit_page.text
        )[0]
        self.assertIn('value="1582"', company_input)
        self.assertIn("required", company_input)
        for invalid in ("", "0", "-1", "invalid"):
            self.client.post(
                f"/units/{unit_id}", data={**data, "orders_api_company_id": invalid}
            )
            with self.Session() as db:
                self.assertEqual(db.get(Unit, unit_id).orders_api_company_id, "1582")
        self.client.post(
            f"/units/{unit_id}", data={**data, "orders_api_company_id": "4232"}
        )
        with self.Session() as db:
            self.assertEqual(db.get(Unit, unit_id).orders_api_company_id, "4232")

    def test_cloud_settings_are_saved_per_unit_and_not_mutated_on_error(self):
        self.login()
        token = self.token("/units/new")
        data = self.unit_form_data(csrf_token=token)
        self.assertEqual(
            self.client.post(
                "/units/new",
                data=data,
                follow_redirects=False,
            ).status_code,
            303,
        )
        with self.Session() as db:
            unit = db.scalar(select(Unit))
            unit_id = unit.id
            self.assertEqual(unit.cloud_url, DEFAULT_CLOUD_URL)

        edit_page = self.client.get(f"/units/{unit_id}")
        self.assertEqual(edit_page.status_code, 200)
        self.assertEqual(edit_page.text.count('placeholder="••••••••••••"'), 2)
        self.assertNotIn('value="integration-token"', edit_page.text)
        self.assertNotIn('value="unit-token"', edit_page.text)
        self.assertNotIn("Deixe em branco para manter o token atual.", edit_page.text)

        token = self.token(f"/units/{unit_id}")
        update_data = self.unit_form_data(
            csrf_token=token,
            orders_api_token="",
            token="",
        )
        self.assertEqual(
            self.client.post(
                f"/units/{unit_id}", data=update_data, follow_redirects=False
            ).status_code,
            303,
        )
        with self.Session() as db:
            unit = db.get(Unit, unit_id)
            self.assertEqual(unit.orders_api_token, "integration-token")
            self.assertEqual(unit.token, "unit-token")
        self.assertEqual(
            self.client.get("/orders", params={"q": "x" * 201}).status_code, 422
        )
        self.assertEqual(
            self.client.get("/orders", params={"status": "invalid"}).status_code, 422
        )
        self.assertEqual(
            self.client.post(
                f"/units/{unit_id}",
                data={
                    **update_data,
                    "cloud_url": DEFAULT_CLOUD_URL + "x" * 500,
                },
                follow_redirects=False,
            ).status_code,
            303,
        )
        with self.Session() as db:
            self.assertEqual(db.get(Unit, unit_id).cloud_url, DEFAULT_CLOUD_URL)
        with self.assertRaises(ValueError):
            validate_cloud_url(DEFAULT_CLOUD_URL + "x" * 500)

    def test_compression_settings_are_saved_per_unit(self):
        self.login()
        token = self.token("/units/new")
        data = self.unit_form_data(
            csrf_token=token,
            compress_lossless="MR",
            compress_lossy="CT,US",
            drop_modalities="US,SR",
        )
        response = self.client.post("/units/new", data=data, follow_redirects=False)
        self.assertEqual(response.status_code, 303)

        with self.Session() as db:
            unit = db.scalar(select(Unit))
            rules = {
                row.modality: row.jpeg_flag
                for row in db.scalars(
                    select(UnitCompressRule).where(UnitCompressRule.unit_id == unit.id)
                )
            }
            drops = {
                row.code
                for row in db.scalars(
                    select(UnitDropModality).where(UnitDropModality.unit_id == unit.id)
                )
            }
            self.assertEqual(rules, {"CT": "lossy", "MR": "lossless"})
            self.assertEqual(drops, {"SR", "US"})
            unit_id = unit.id

        edit_page = self.client.get(f"/units/{unit_id}")
        self.assertEqual(edit_page.status_code, 200)
        self.assertIn('name="compress_lossy" value="CT"', edit_page.text)
        self.assertNotIn("/rules/compress", edit_page.text)

        token = self.token(f"/units/{unit_id}")
        invalid = self.unit_form_data(
            csrf_token=token,
            orders_api_token="",
            token="",
            compress_lossless="CT",
            compress_lossy="CT",
        )
        response = self.client.post(
            f"/units/{unit_id}", data=invalid, follow_redirects=False
        )
        self.assertEqual(response.status_code, 303)
        with self.Session() as db:
            rules = {
                row.modality: row.jpeg_flag
                for row in db.scalars(
                    select(UnitCompressRule).where(UnitCompressRule.unit_id == unit_id)
                )
            }
            self.assertEqual(rules, {"CT": "lossy", "MR": "lossless"})

    def test_dicom_rule_page_create_and_static_rule_routes(self):
        self.login()
        with self.Session() as db:
            unit = Unit(
                name="Unidade de teste",
                orders_api_url="https://example.test/orders",
                orders_api_token="integration-token",
                pacs_aet="PACS",
                pacs_ip="127.0.0.1",
                pacs_port=2104,
                calling_aet="RETRIEVE",
                store_port=444,
                receive_dir="/receive",
                send_dir="/send",
                error_dir="/error",
                token="token",
            )
            db.add(unit)
            db.commit()
            unit_id = unit.id

        token = self.token("/rules")
        response = self.client.post(
            "/rules",
            data={
                "csrf_token": token,
                "name": "Descartar SLRX",
                "enabled": "1",
                "priority": "10",
                "unit_ids": [str(unit_id)],
                "combinator": "and",
                "condition_tag": ["0020,0010"],
                "condition_operator": ["starts_with_digits"],
                "condition_value": ["SLRX"],
                "action": "delete",
                "action_tag": "",
                "action_value": "",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        with self.Session() as db:
            created = db.scalar(select(DicomRule))
            self.assertIsNotNone(created)
            created_id = created.id
            self.assertEqual(created.conditions[0].tag, "0020,0010")
            self.assertEqual(created.unit_links[0].unit_id, unit_id)

        edit_page = self.client.get(f"/rules?edit={created_id}")
        self.assertEqual(edit_page.status_code, 200)
        response = self.client.post(
            f"/rules/dicom/{created_id}",
            data={
                "csrf_token": token,
                "name": "Normalizar instituição",
                "enabled": "1",
                "priority": "20",
                "unit_ids": [str(unit_id)],
                "combinator": "or",
                "condition_tag": ["0008,0060", "0008,0080"],
                "condition_operator": ["equals", "not_exists"],
                "condition_value": ["CT", ""],
                "action": "replace",
                "action_tag": "0008,0080",
                "action_value": "Mobilemed",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        with self.Session() as db:
            updated = db.get(DicomRule, created_id)
            self.assertEqual(updated.name, "Normalizar instituição")
            self.assertEqual(updated.combinator, "or")
            self.assertEqual(updated.action, "replace")
            self.assertEqual(len(updated.conditions), 2)

        tag_info = self.client.get("/rules/tag-info", params={"tag": "0008,0060"})
        self.assertEqual(tag_info.status_code, 200)
        self.assertEqual(tag_info.json()["tag"], "0008,0060")
        self.assertEqual(self.client.get("/rules/retrieve").status_code, 200)

    def test_regular_user_is_limited_to_read_only_operation_pages(self):
        regular_password = "regular-password-123"
        with self.Session() as db:
            db.add(
                User(
                    username="observer",
                    password_hash=hash_password(regular_password),
                    role="user",
                )
            )
            db.commit()

        self.login_credentials("observer", regular_password)
        dashboard = self.client.get("/")
        self.assertEqual(dashboard.status_code, 200)
        self.assertNotIn('href="/units"', dashboard.text)
        self.assertNotIn('href="/users"', dashboard.text)
        self.assertNotIn("Nova unidade", dashboard.text)
        self.assertIn("data-account-menu", dashboard.text)
        self.assertIn("data-theme-toggle", dashboard.text)
        self.assertIn('href="/account/password"', dashboard.text)
        self.assertIn("Trocar senha", dashboard.text)
        orders = self.client.get("/orders")
        self.assertEqual(orders.status_code, 200)
        self.assertNotIn('action="/orders/', orders.text)
        self.assertEqual(self.client.get("/account/password").status_code, 200)

        for path in (
            "/units",
            "/units/new",
            "/rules",
            "/rules/retrieve",
            "/rules/compress",
            "/users",
            "/logs",
            "/orders/history",
            "/settings",
        ):
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path).status_code, 403)

        token = self.token("/")
        for path in (
            "/units/1/toggle",
            "/rules/drop",
            "/orders/1/retry",
            "/orders/1/cancel",
            "/orders/1/delete",
        ):
            with self.subTest(path=path):
                self.assertEqual(
                    self.client.post(path, data={"csrf_token": token}).status_code,
                    403,
                )

    def test_audit_logs_record_changes_and_are_filterable(self):
        self.login()
        token = self.token("/units/new")
        response = self.client.post(
            "/units/new",
            data=self.unit_form_data(csrf_token=token),
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)

        with self.Session() as db:
            entry = db.scalar(select(AuditLog))
            self.assertIsNotNone(entry)
            self.assertEqual(entry.actor_username, "tester")
            self.assertEqual(entry.action, "create")
            self.assertEqual(entry.resource_type, "unit")
            self.assertEqual(entry.resource_name, "Unidade de teste")

        logs = self.client.get("/logs", params={"resource": "unit", "action": "create"})
        self.assertEqual(logs.status_code, 200)
        self.assertIn("Unidade de teste", logs.text)
        self.assertIn("Unidade adicionada ao sistema.", logs.text)
        self.assertIn('name="page_size" value="30" data-custom-select-value', logs.text)
        self.assertIn('data-value="30" aria-selected="true"', logs.text)
        self.assertEqual(
            self.client.get("/logs", params={"page_size": 10}).status_code,
            200,
        )
        self.assertEqual(
            self.client.get("/logs", params={"resource": "invalid"}).status_code,
            422,
        )
        self.assertEqual(
            self.client.get("/logs", params={"page_size": 25}).status_code,
            422,
        )

    def test_audit_logs_use_cursor_backed_pages(self):
        self.login()
        with self.Session() as db:
            db.add_all(
                AuditLog(
                    actor_username="tester",
                    actor_role="admin",
                    action="create",
                    resource_type="unit",
                    resource_id=str(index),
                    resource_name=f"Log {index:02d}",
                    summary="Evento de teste",
                )
                for index in range(42)
            )
            db.commit()

        first = self.client.get("/logs")
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.text.count('class="log-date"'), 30)
        cursor = re.search(r"before=(\d+)&amp;page=2", first.text)[1]

        second = self.client.get("/logs", params={"before": cursor, "page": 2})
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.text.count('class="log-date"'), 12)
        self.assertIn("Página 2 de 2", second.text)

        ten_per_page = self.client.get("/logs", params={"page_size": 10})
        self.assertEqual(ten_per_page.text.count('class="log-date"'), 10)
        self.assertRegex(ten_per_page.text, r"before=\d+&amp;page=3")

    def test_admin_can_create_update_and_delete_users_with_self_protection(self):
        self.login()
        users_page = self.client.get("/users")
        self.assertEqual(users_page.status_code, 200)
        self.assertIn("tester", users_page.text)
        new_user_page = self.client.get("/users/new")
        self.assertIn('option value="user" selected', new_user_page.text)
        token = re.search(r'name="csrf_token" value="([^"]+)"', users_page.text)[1]
        initial_password = "initial-password-123"
        response = self.client.post(
            "/users/new",
            data={
                "csrf_token": token,
                "username": "observer",
                "password": initial_password,
                "password_confirmation": initial_password,
                "role": "user",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        with self.Session() as db:
            managed_user = db.scalar(select(User).where(User.username == "observer"))
            self.assertIsNotNone(managed_user)
            managed_user_id = managed_user.id
            self.assertEqual(managed_user.role, "user")
            self.assertTrue(
                verify_password(initial_password, managed_user.password_hash)
            )

        updated_password = "updated-password-123"
        response = self.client.post(
            f"/users/{managed_user_id}",
            data={
                "csrf_token": token,
                "role": "admin",
                "new_password": updated_password,
                "password_confirmation": updated_password,
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        with self.Session() as db:
            managed_user = db.get(User, managed_user_id)
            self.assertEqual(managed_user.role, "admin")
            self.assertTrue(
                verify_password(updated_password, managed_user.password_hash)
            )
            current_user_id = db.scalar(
                select(User.id).where(User.username == "tester")
            )

        self.client.post(
            f"/users/{current_user_id}",
            data={
                "csrf_token": token,
                "role": "user",
                "new_password": "",
                "password_confirmation": "",
            },
        )
        self.client.post(f"/users/{current_user_id}/delete", data={"csrf_token": token})
        bypass_password = "bypass-password-123"
        self.client.post(
            f"/users/{current_user_id}",
            data={
                "csrf_token": token,
                "role": "admin",
                "new_password": bypass_password,
                "password_confirmation": bypass_password,
            },
        )
        with self.Session() as db:
            current = db.get(User, current_user_id)
            self.assertIsNotNone(current)
            self.assertEqual(current.role, "admin")
            self.assertFalse(verify_password(bypass_password, current.password_hash))

        self.assertEqual(
            self.client.post(
                f"/users/{managed_user_id}/delete",
                data={"csrf_token": token},
                follow_redirects=False,
            ).status_code,
            303,
        )
        with self.Session() as db:
            self.assertIsNone(db.get(User, managed_user_id))

    def test_regular_user_can_change_own_password(self):
        old_password = "regular-password-123"
        new_password = "regular-password-456"
        with self.Session() as db:
            db.add(
                User(
                    username="observer",
                    password_hash=hash_password(old_password),
                    role="user",
                )
            )
            db.commit()
        self.login_credentials("observer", old_password)
        token = self.token("/account/password")
        response = self.client.post(
            "/account/password",
            data={
                "csrf_token": token,
                "current": old_password,
                "new_password": new_password,
                "password_confirmation": new_password,
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        with self.Session() as db:
            observer = db.scalar(select(User).where(User.username == "observer"))
            self.assertTrue(verify_password(new_password, observer.password_hash))

    def _second_browser(self, username: str, password: str) -> TestClient:
        browser = TestClient(
            app, base_url="https://testserver", raise_server_exceptions=False
        )
        page = browser.get("/login")
        token = re.search(r'name="csrf_token" value="([^"]+)"', page.text)[1]
        response = browser.post(
            "/login",
            data={"username": username, "password": password, "csrf_token": token},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.addCleanup(browser.close)
        return browser

    def test_own_password_change_revokes_other_sessions_but_keeps_current(self):
        self.login()
        other_browser = self._second_browser("tester", self.password)
        self.assertEqual(other_browser.get("/").status_code, 200)

        new_password = "changed-password-456"
        token = self.token("/account/password")
        self.client.post(
            "/account/password",
            data={
                "csrf_token": token,
                "current": self.password,
                "new_password": new_password,
                "password_confirmation": new_password,
            },
        )

        self.assertEqual(self.client.get("/").status_code, 200)
        revoked = other_browser.get("/", follow_redirects=False)
        self.assertEqual(revoked.status_code, 303)
        self.assertEqual(revoked.headers["location"], "/login")
        self._second_browser("tester", new_password)

    def test_admin_password_reset_revokes_the_managed_user_sessions(self):
        observer_password = "observer-password-123"
        with self.Session() as db:
            observer = User(
                username="observer",
                password_hash=hash_password(observer_password),
                role="user",
            )
            db.add(observer)
            db.commit()
            observer_id = observer.id
        observer_browser = self._second_browser("observer", observer_password)
        self.assertEqual(observer_browser.get("/").status_code, 200)

        self.login()
        token = self.token(f"/users/{observer_id}")
        reset_password = "reset-password-789"
        self.client.post(
            f"/users/{observer_id}",
            data={
                "csrf_token": token,
                "role": "user",
                "new_password": reset_password,
                "password_confirmation": reset_password,
            },
        )
        self.assertEqual(
            observer_browser.get("/", follow_redirects=False).status_code, 303
        )

    def test_unknown_username_still_spends_a_bcrypt_comparison(self):
        token = self.token()
        with patch("app.security.bcrypt.checkpw", return_value=False) as checkpw:
            response = self.client.post(
                "/login",
                data={
                    "username": "does-not-exist",
                    "password": "whatever-password",
                    "csrf_token": token,
                },
            )
        self.assertEqual(response.status_code, 401)
        checkpw.assert_called_once()

    def test_headers_on_success_rejection_and_unexpected_errors(self):
        responses = [self.client.get("/health/live"), self.client.post("/logout")]

        def failed_database():
            raise RuntimeError("synthetic failure")

        app.dependency_overrides[get_db] = failed_database
        for accept in ("text/html", "application/json"):
            response = self.client.get("/health", headers={"Accept": accept})
            self.assertEqual(response.status_code, 500)
            responses.append(response)
        for response in responses:
            self.assertEqual(response.headers["x-frame-options"], "DENY")
            self.assertEqual(response.headers["x-content-type-options"], "nosniff")
            self.assertIn(
                "base-uri 'none'", response.headers["content-security-policy"]
            )
            self.assertEqual(response.headers["cache-control"], "no-store")

        static_response = self.client.get("/static/app.css")
        self.assertEqual(static_response.status_code, 200)
        self.assertEqual(
            static_response.headers["cache-control"],
            "public, max-age=31536000, immutable",
        )

    def test_echo_header_keeps_multipart_form_readable(self):
        self.login()
        token = self.token("/units/new")
        fields = {
            "calling_aet": "RETRIEVE",
            "pacs_aet": "PACS",
            "pacs_ip": "127.0.0.1",
            "pacs_port": "2104",
        }
        with patch("app.routes.units.echo", return_value=EchoResult(True, 0)) as echo:
            response = self.client.post(
                "/units/test-echo",
                files={key: (None, value) for key, value in fields.items()},
                headers={"X-CSRF-Token": token},
            )
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()["ok"])
            echo.assert_called_once()

    def test_search_payload_is_escaped_and_not_sql(self):
        self.login()
        payload = "\"><img src=x onerror=alert(1)>' OR 1=1 --"
        response = self.client.get("/orders", params={"q": payload})
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("<img src=x", response.text)
        self.assertIn("&lt;img", response.text)

    def test_orders_use_cursor_backed_pages_and_separate_archived_history(
        self,
    ):
        self.login()
        with self.Session() as db:
            unit = Unit(
                name="Cursor",
                orders_api_url="https://example.test/orders",
                orders_api_token="token",
                pacs_aet="PACS",
                pacs_ip="127.0.0.1",
                pacs_port=2104,
                calling_aet="RETRIEVE",
                store_port=444,
                receive_dir=str(BASE_DIR),
                send_dir=str(BASE_DIR),
                error_dir=str(BASE_DIR),
                token="unit-token",
            )
            db.add(unit)
            db.flush()
            db.add_all(
                Order(
                    unit_id=unit.id,
                    acc=f"active-{index:02d}",
                    birth_date="20000101",
                )
                for index in range(190)
            )
            db.add(
                Order(
                    unit_id=unit.id,
                    acc="archived-only",
                    birth_date="20000101",
                    archived_at=datetime.now(),
                    archive_reason="Teste",
                )
            )
            db.commit()

        first = self.client.get("/orders")
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.text.count('class="order-date"'), 30)
        self.assertNotIn("archived-only", first.text)
        self.assertIn('aria-label="Primeira página"', first.text)
        self.assertIn('aria-label="Última página"', first.text)
        self.assertIn("Página 1 de 7", first.text)
        self.assertEqual(
            self.client.get("/orders", params={"page": 2}).status_code,
            422,
        )

        for target_page in range(2, 8):
            link = re.search(
                rf'href="([^"]+)" aria-label="Ir para a página {target_page}"',
                first.text,
            )
            self.assertIsNotNone(link, f"Link da página {target_page} ausente")
            href = link[1].replace("&amp;", "&")
            response = self.client.get(f"/orders{href}")
            self.assertEqual(response.status_code, 200)
            self.assertIn(f"Página {target_page} de 7", response.text)
            expected_count = 10 if target_page == 7 else 30
            self.assertEqual(response.text.count('class="order-date"'), expected_count)
            if target_page == 2:
                detail_link = re.search(
                    r'href="(/orders/\d+\?return_to=[^"]+)"[^>]+'
                    r'aria-label="Visualizar pedido"',
                    response.text,
                )
                self.assertIsNotNone(detail_link)
                detail = self.client.get(detail_link[1].replace("&amp;", "&"))
                self.assertEqual(detail.status_code, 200)
                self.assertRegex(
                    detail.text,
                    r'href="/orders\?page_size=30&amp;before=\d+&amp;page=2" '
                    r'aria-label="Voltar"',
                )

        ten_per_page = self.client.get("/orders", params={"page_size": 10})
        self.assertEqual(ten_per_page.text.count('class="order-date"'), 10)
        self.assertIn(
            'name="page_size" value="10" data-custom-select-value',
            ten_per_page.text,
        )
        self.assertIn('data-value="10" aria-selected="true"', ten_per_page.text)
        self.assertRegex(ten_per_page.text, r"before=\d+&amp;page=3")

        history = self.client.get("/orders/history")
        self.assertEqual(history.status_code, 200)
        self.assertIn("archived-only", history.text)
        self.assertNotIn("active-41", history.text)

    def test_orders_filter_accepts_all_units_with_a_status(self):
        self.login()
        response = self.client.get(
            "/orders",
            params={"unit_id": "", "status": "watching", "page_size": 30},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn('name="unit_id" value=""', response.text)
        self.assertIn('name="status" value="watching"', response.text)
        self.assertIn('data-value="watching" aria-selected="true"', response.text)

    def test_error_page_uses_logo_before_code_and_title(self):
        self.login()
        response = self.client.get(
            "/orders",
            params={"page": "invalid"},
            headers={"Accept": "text/html"},
        )

        self.assertEqual(response.status_code, 422)
        logo = response.text.index("/static/logo-white.png?v=1")
        code = response.text.index("ERRO 422")
        title = response.text.index("Dados inválidos")
        self.assertLess(logo, code)
        self.assertLess(code, title)

    def test_order_page_can_queue_manual_current_retrieve(self):
        self.login()
        with self.Session() as db:
            unit = Unit(
                name="Manual move",
                orders_api_url="https://example.test/orders",
                orders_api_token="token",
                pacs_aet="PACS",
                pacs_ip="127.0.0.1",
                pacs_port=2104,
                calling_aet="RETRIEVE",
                store_port=444,
                receive_dir=str(BASE_DIR),
                send_dir=str(BASE_DIR),
                error_dir=str(BASE_DIR),
                token="unit-token",
            )
            db.add(unit)
            db.flush()
            order = Order(
                unit_id=unit.id,
                acc="manual-current",
                birth_date="20000101",
                study_uid="1.2.current",
                modality="CT",
                status="wait_retrieve",
                prior_status="disabled",
            )
            db.add(order)
            db.commit()
            order_id = order.id

        page = self.client.get(f"/orders/{order_id}")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Retrieve agora", page.text)
        self.assertIn("Atualizar", page.text)
        token = re.search(r'name="csrf_token" value="([^"]+)"', page.text)[1]
        response = self.client.post(
            f"/orders/{order_id}/retrieve-now",
            data={"csrf_token": token},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        with self.Session() as db:
            move_request = db.scalar(select(ManualMoveRequest))
            self.assertIsNotNone(move_request)
            self.assertEqual(move_request.order_id, order_id)
            self.assertEqual(move_request.status, "queued")

    def test_instances_page_lists_only_pending_problems(self):
        self.login()
        with self.Session() as db:
            unit = Unit(
                name="Instances unit",
                orders_api_url="https://example.test/orders",
                orders_api_token="token",
                pacs_aet="PACS",
                pacs_ip="127.0.0.1",
                pacs_port=2104,
                calling_aet="RETRIEVE",
                store_port=444,
                receive_dir=str(BASE_DIR),
                send_dir=str(BASE_DIR),
                error_dir=str(BASE_DIR),
                token="unit-token",
            )
            db.add(unit)
            db.flush()
            order = Order(
                unit_id=unit.id,
                acc="ACC-SECRET",
                birth_date="20000101",
                study_uid="1.2.inst",
                modality="CT",
                status="done",
                prior_status="disabled",
            )
            db.add(order)
            db.flush()
            transfer = ImageTransfer(
                unit_id=unit.id,
                order_id=order.id,
                filename="CT.conflict.dcm",
                correlation_id="corr",
                status="uploaded",
            )
            db.add(transfer)
            study = DicomStudy(unit_id=unit.id, study_uid="1.2.inst")
            db.add(study)
            db.flush()
            for index, state in enumerate(("conflict", "missing", "compacted")):
                db.add(
                    DicomInstance(
                        unit_id=unit.id,
                        study_id=study.id,
                        sop_uid=f"1.2.inst.{index}",
                        modality="CT",
                        source_path=str(BASE_DIR / f"CT.{state}"),
                        source_sha256=str(index) * 64,
                        calling_aet="SRVPACS",
                        peer_ip="192.168.3.103",
                        state=state,
                        transfer_id=transfer.id if state == "conflict" else None,
                    )
                )
            db.commit()
            unit_id, order_id = unit.id, order.id

        page = self.client.get("/instances")
        self.assertEqual(page.status_code, 200)
        self.assertIn("CT.conflict", page.text)
        self.assertIn("CT.missing", page.text)
        self.assertNotIn("CT.compacted", page.text)
        self.assertIn(f'href="/orders/{order_id}"', page.text)
        self.assertIn("192.168.3.103", page.text)
        self.assertNotIn("ACC-SECRET", page.text)

        filtered = self.client.get(
            "/instances", params={"state": "missing", "unit_id": unit_id}
        )
        self.assertIn("CT.missing", filtered.text)
        self.assertNotIn("CT.conflict", filtered.text)
        self.assertEqual(
            self.client.get("/instances", params={"state": "compacted"}).status_code,
            422,
        )

    def test_send_failures_can_be_resent_from_order_and_unit(self):
        self.login()
        with self.Session() as db:
            unit = Unit(
                name="Resend unit",
                orders_api_url="https://example.test/orders",
                orders_api_token="token",
                pacs_aet="PACS",
                pacs_ip="127.0.0.1",
                pacs_port=2104,
                calling_aet="RETRIEVE",
                store_port=444,
                receive_dir=str(BASE_DIR),
                send_dir=str(BASE_DIR),
                error_dir=str(BASE_DIR),
                token="unit-token",
            )
            db.add(unit)
            db.flush()
            order = Order(
                unit_id=unit.id,
                acc="resend",
                birth_date="20000101",
                study_uid="1.2.resend",
                modality="CT",
                status="done",
                prior_status="disabled",
            )
            db.add(order)
            db.flush()
            for index, order_id in enumerate((order.id, order.id, None)):
                db.add(
                    ImageTransfer(
                        unit_id=unit.id,
                        order_id=order_id,
                        filename=f"{index}.dcm",
                        correlation_id=f"corr-{index}",
                        status="send_error",
                        attempts=7,
                        last_error="CloudHttpError",
                    )
                )
            db.commit()
            unit_id, order_id = unit.id, order.id

        page = self.client.get(f"/orders/{order_id}")
        self.assertIn("Reenviar 2 imagem(ns) com falha de envio", page.text)
        token = re.search(r'name="csrf_token" value="([^"]+)"', page.text)[1]
        response = self.client.post(
            f"/orders/{order_id}/resend-failed",
            data={"csrf_token": token},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        with self.Session() as db:
            rows = {
                row.filename: (row.status, row.attempts)
                for row in db.scalars(select(ImageTransfer))
            }
        self.assertEqual(
            rows,
            {
                "0.dcm": ("compressed", 0),
                "1.dcm": ("compressed", 0),
                "2.dcm": ("send_error", 7),
            },
        )
        self.assertNotIn(
            "com falha de envio", self.client.get(f"/orders/{order_id}").text
        )

        token = self.token(f"/units/{unit_id}")
        response = self.client.post(
            f"/units/{unit_id}/resend-failed",
            data={"csrf_token": token},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        with self.Session() as db:
            self.assertEqual(
                set(db.scalars(select(ImageTransfer.status))), {"compressed"}
            )
            audit = db.scalars(
                select(AuditLog).where(AuditLog.action == "resend")
            ).all()
            self.assertEqual(len(audit), 2)

    def test_archiving_unit_preserves_unit_and_orders(self):
        self.login()
        with self.Session() as db:
            unit = Unit(
                name="Archive unit",
                orders_api_url="https://example.test/orders",
                orders_api_token="token",
                pacs_aet="PACS",
                pacs_ip="127.0.0.1",
                pacs_port=2104,
                calling_aet="RETRIEVE",
                store_port=444,
                receive_dir=str(BASE_DIR),
                send_dir=str(BASE_DIR),
                error_dir=str(BASE_DIR),
                token="unit-token",
            )
            db.add(unit)
            db.flush()
            order = Order(
                unit_id=unit.id,
                acc="unit-archive-order",
                birth_date="20000101",
                status="done",
            )
            db.add(order)
            db.commit()
            unit_id = unit.id
            order_id = order.id

        token = self.token("/units")
        response = self.client.post(
            f"/units/{unit_id}/delete",
            data={"csrf_token": token},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        with self.Session() as db:
            unit = db.get(Unit, unit_id)
            order = db.get(Order, order_id)
            self.assertIsNotNone(unit.deleted_at)
            self.assertFalse(unit.enabled)
            self.assertIsNotNone(order.archived_at)
            self.assertEqual(order.unit_id, unit_id)

    def test_error_return_does_not_use_external_referer(self):
        self.login()
        response = self.client.get(
            "/orders",
            params={"page": "invalid"},
            headers={
                "Accept": "text/html",
                "Referer": "https://evil.example/phishing",
            },
        )
        self.assertEqual(response.status_code, 422)
        self.assertNotIn("evil.example", response.text)

    def test_templates_have_one_token_in_each_post_form(self):
        forms_component = (BASE_DIR / "app/templates/components/forms.html").read_text(
            encoding="utf-8"
        )
        self.assertEqual(forms_component.count('name="csrf_token"'), 1)
        for path in (BASE_DIR / "app/templates").glob("*.html"):
            for form in re.findall(
                r"<form\b.*?</form>", path.read_text(encoding="utf-8"), re.S
            ):
                if 'method="post"' in form:
                    with self.subTest(template=path.name):
                        token_sources = form.count('name="csrf_token"') + form.count(
                            "csrf_input(request)"
                        )
                        self.assertEqual(token_sources, 1)

    def test_origin_compares_scheme_host_and_port(self):
        def request(origin):
            return Request(
                {
                    "type": "http",
                    "method": "POST",
                    "scheme": "https",
                    "path": "/",
                    "query_string": b"",
                    "headers": [
                        (b"host", b"internal:8080"),
                        (b"origin", origin.encode()),
                    ],
                }
            )

        with patch("app.middleware.PUBLIC_ORIGIN", "https://public.example"):
            self.assertTrue(_same_origin(request("https://public.example:443")))
            for source in (
                "http://public.example",
                "https://public.example:444",
                "https://evil.example",
                "null",
                "https://public.example:bad",
            ):
                self.assertFalse(_same_origin(request(source)))


class ProductionConfigTest(unittest.TestCase):
    def test_production_requires_secure_session_and_https_origin(self):
        environment = {
            **os.environ,
            "PYTHON_DOTENV_DISABLED": "1",
            "APP_ENV": "production",
            "SECRET_KEY": "s" * 48,
            "RETRIEVE_ADMIN_PASSWORD": "test-password-123",
            "PUBLIC_ORIGIN": "https://retrieve.example",
            "SESSION_HTTPS_ONLY": "true",
            "ALLOW_HTTP_FOR_TESTS": "false",
        }
        environment.pop("SECRET_KEY_FILE", None)
        environment.pop("RETRIEVE_ADMIN_PASSWORD_FILE", None)
        for overrides, success in (
            ({}, True),
            ({"SESSION_HTTPS_ONLY": "false"}, False),
            ({"PUBLIC_ORIGIN": ""}, False),
            ({"PUBLIC_ORIGIN": "http://retrieve.example"}, False),
            (
                {
                    "ALLOW_HTTP_FOR_TESTS": "true",
                    "SESSION_HTTPS_ONLY": "false",
                    "PUBLIC_ORIGIN": "http://192.168.1.100:8080",
                },
                True,
            ),
            (
                {
                    "ALLOW_HTTP_FOR_TESTS": "true",
                    "SESSION_HTTPS_ONLY": "false",
                    "PUBLIC_ORIGIN": "",
                },
                True,
            ),
            ({"ALLOW_HTTP_FOR_TESTS": "true", "SECRET_KEY": "short"}, False),
        ):
            result = subprocess.run(
                [sys.executable, "-c", "import app.config"],
                cwd=Path(__file__).resolve().parents[1],
                env={**environment, **overrides},
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode == 0, success, result.stderr)

    def test_http_test_mode_supports_session_and_csrf_by_ip(self):
        environment = {
            **os.environ,
            "PYTHON_DOTENV_DISABLED": "1",
            "APP_ENV": "production",
            "SECRET_KEY": "s" * 48,
            "RETRIEVE_ADMIN_PASSWORD": "test-password-123",
            "ALLOW_HTTP_FOR_TESTS": "true",
            "PUBLIC_ORIGIN": "",
            "DATABASE_URL": "sqlite:///:memory:",
            "LOG_LEVEL": "CRITICAL",
        }
        for key in (
            "SESSION_HTTPS_ONLY",
            "SECRET_KEY_FILE",
            "RETRIEVE_ADMIN_PASSWORD_FILE",
        ):
            environment.pop(key, None)
        script = """
import re
from starlette.testclient import TestClient
from app.main import app
from app.config import SESSION_HTTPS_ONLY
assert SESSION_HTTPS_ONLY is False
client = TestClient(app, base_url="http://192.168.1.100:8080")
page = client.get("/login")
assert page.status_code == 200
assert "secure" not in page.headers["set-cookie"].lower()
assert "strict-transport-security" not in page.headers
token = re.search(r'name="csrf_token" value="([^"]+)"', page.text)[1]
assert client.post("/logout").status_code == 403
assert client.post("/logout", data={"csrf_token": token},
    headers={"Origin": "http://evil.example"}).status_code == 403
assert client.post("/logout", data={"csrf_token": token},
    headers={"Origin": "http://192.168.1.100:8080"},
    follow_redirects=False).status_code == 303
client.close()
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=BASE_DIR,
            env=environment,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
