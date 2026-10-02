import unittest
from datetime import datetime
from types import SimpleNamespace

from sqlalchemy import select

from app.models import ModalityRule
from app.routes.rules import rules_retrieve_delete
from app.rules import monitor_plan_for, retrieve_rule_for, schedule_from_now
from tests.support import DatabaseTestCase


class RulesTest(DatabaseTestCase):
    def setUp(self):
        super().setUp()
        with self.Session() as db:
            db.add_all(
                [
                    ModalityRule(
                        modality="CT",
                        wait_minutes=15,
                        monitor_enabled=True,
                    ),
                    ModalityRule(
                        modality="MR",
                        wait_minutes=15,
                        monitor_enabled=True,
                        monitor_interval_minutes=10,
                        monitor_max_hours=12,
                    ),
                    ModalityRule(
                        modality="*",
                        wait_minutes=10,
                        monitor_enabled=False,
                    ),
                ]
            )
            db.commit()

    def test_ct_and_default(self):
        with self.Session() as db:
            ct = retrieve_rule_for(db, "CT")
            cr = retrieve_rule_for(db, "CR")
            self.assertEqual(ct.wait_minutes, 15)
            self.assertTrue(ct.monitor_enabled)
            self.assertEqual(cr.wait_minutes, 10)
            self.assertFalse(cr.monitor_enabled)

    def test_schedule_minutes(self):
        with self.Session() as db:
            _mod, first = schedule_from_now(db, "MR")
            delta = first - datetime.now()
            self.assertAlmostEqual(delta.total_seconds() / 60, 15, delta=0.2)
            _mod, first = schedule_from_now(db, "DX")
            self.assertAlmostEqual(
                (first - datetime.now()).total_seconds() / 60, 10, delta=0.2
            )

    def test_monitor_plan_per_modality(self):
        with self.Session() as db:
            ct = monitor_plan_for(db, "CT")
            mr = monitor_plan_for(db, "MR")
            assert ct is not None and mr is not None
            self.assertEqual((ct.interval_minutes, ct.max_hours), (5, 6))
            self.assertEqual((mr.interval_minutes, mr.max_hours), (10, 12))
            self.assertIsNone(monitor_plan_for(db, "DX"))

    def test_delete_specific_rule_and_protect_default(self):
        request = SimpleNamespace(session={})
        user = SimpleNamespace(id=1)
        with self.Session() as db:
            ct = db.scalar(select(ModalityRule).where(ModalityRule.modality == "CT"))
            default = db.scalar(
                select(ModalityRule).where(ModalityRule.modality == "*")
            )
            assert ct is not None
            assert default is not None

            response = rules_retrieve_delete(ct.id, request, db, user)
            self.assertEqual(response.status_code, 303)
            self.assertIsNone(db.get(ModalityRule, ct.id))

            response = rules_retrieve_delete(default.id, request, db, user)
            self.assertEqual(response.status_code, 303)
            self.assertIsNotNone(db.get(ModalityRule, default.id))
            self.assertEqual(request.session["flash"]["kind"], "err")


if __name__ == "__main__":
    unittest.main()
