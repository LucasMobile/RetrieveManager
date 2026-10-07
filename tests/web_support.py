"""Logged-in admin client against a throwaway PostgreSQL schema."""

import re
import unittest

from sqlalchemy.orm import sessionmaker
from starlette.testclient import TestClient

from app.db import get_db
from app.main import app
from app.models import User
from app.rate_limit import reset_rate_limiters
from app.security import hash_password
from tests.support import postgres_test_engine

PASSWORD = "test-password-123"


class AdminWebTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.password_hash = hash_password(PASSWORD)

    def setUp(self):
        reset_rate_limiters()
        self.engine = postgres_test_engine(self)
        self.Session = sessionmaker(self.engine)
        with self.Session() as db:
            db.add(
                User(username="tester", password_hash=self.password_hash, role="admin")
            )
            db.commit()

        def database():
            with self.Session() as db:
                yield db

        self.previous_overrides = app.dependency_overrides.copy()
        app.dependency_overrides[get_db] = database
        # Do not run lifespan: it initializes the deployment database.
        self.client = TestClient(
            app, base_url="https://testserver", raise_server_exceptions=False
        )
        self.login()

    def tearDown(self):
        self.client.close()
        reset_rate_limiters()
        app.dependency_overrides.clear()
        app.dependency_overrides.update(self.previous_overrides)
        self.engine.dispose()

    def token(self, path):
        response = self.client.get(path)
        self.assertEqual(response.status_code, 200)
        return re.search(r'name="csrf_token" value="([^"]+)"', response.text)[1]

    def login(self):
        response = self.client.post(
            "/login",
            data={
                "username": "tester",
                "password": PASSWORD,
                "csrf_token": self.token("/login"),
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
