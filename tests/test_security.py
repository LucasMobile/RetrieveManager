import os
import re
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from starlette.requests import Request
from starlette.testclient import TestClient

from app.config import BASE_DIR, DEFAULT_CLOUD_URL
from app.main import app, get_db
from app.middleware import _same_origin
from app.models import Base, DicomRule, Settings, Unit, User
from app.security import hash_password
from app.validation import validate_cloud_url


class SecurityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.password = "test-password-123"
        cls.password_hash = hash_password(cls.password)

    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(self.engine)
        with self.Session() as db:
            db.add(User(username="tester", password_hash=self.password_hash))
            db.add(Settings(id=1, cloud_url=DEFAULT_CLOUD_URL))
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
        app.dependency_overrides.clear()
        app.dependency_overrides.update(self.previous_overrides)
        self.engine.dispose()

    def token(self, path="/login"):
        response = self.client.get(path)
        self.assertEqual(response.status_code, 200)
        return re.search(r'name="csrf_token" value="([^"]+)"', response.text)[1]

    def login(self):
        token = self.token()
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
        return token

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

    def test_valid_settings_form_and_validation_do_not_mutate_on_error(self):
        self.login()
        settings_page = self.client.get("/settings")
        self.assertEqual(settings_page.status_code, 200)
        self.assertNotIn('name="drop_study_prefix"', settings_page.text)
        token = re.search(
            r'name="csrf_token" value="([^"]+)"', settings_page.text
        )[1]
        data = {
            "csrf_token": token,
            "cloud_url": DEFAULT_CLOUD_URL,
            "file_settle_seconds": "7",
        }
        self.assertEqual(
            self.client.post(
                "/settings",
                data=data,
                follow_redirects=False,
            ).status_code,
            303,
        )
        for value in ("-1", "3601", "invalid"):
            self.assertEqual(
                self.client.post(
                    "/settings",
                    data={**data, "file_settle_seconds": value},
                ).status_code,
                422,
            )
        with self.Session() as db:
            self.assertEqual(db.get(Settings, 1).file_settle_seconds, 7)
        self.assertEqual(
            self.client.get("/orders", params={"q": "x" * 201}).status_code, 422
        )
        self.assertEqual(
            self.client.get("/orders", params={"status": "invalid"}).status_code, 422
        )
        self.assertEqual(
            self.client.post(
                "/settings",
                data={
                    **data,
                    "cloud_url": DEFAULT_CLOUD_URL + "x" * 500,
                },
            ).status_code,
            422,
        )
        with self.assertRaises(ValueError):
            validate_cloud_url(DEFAULT_CLOUD_URL + "x" * 500)

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

    def test_echo_header_keeps_multipart_form_readable(self):
        self.login()
        token = self.token("/units/new")
        fields = {
            "calling_aet": "RETRIEVE",
            "pacs_aet": "PACS",
            "pacs_ip": "127.0.0.1",
            "pacs_port": "2104",
        }
        with patch("app.main.c_echo", return_value=(0, "OK")) as echo:
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
        for path in (BASE_DIR / "app/templates").glob("*.html"):
            for form in re.findall(
                r"<form\b.*?</form>", path.read_text(encoding="utf-8"), re.S
            ):
                if 'method="post"' in form:
                    with self.subTest(template=path.name):
                        self.assertEqual(form.count('name="csrf_token"'), 1)

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
