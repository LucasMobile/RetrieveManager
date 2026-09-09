import unittest
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import Base, ModalityRule
from app.rules import retrieve_rule_for, schedule_from_now


class RulesTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        with self.Session() as db:
            db.add_all(
                [
                    ModalityRule(
                        modality="CT",
                        wait_minutes=15,
                        second_retrieve=True,
                        second_wait_minutes=90,
                    ),
                    ModalityRule(
                        modality="MR",
                        wait_minutes=15,
                        second_retrieve=True,
                        second_wait_minutes=90,
                    ),
                    ModalityRule(
                        modality="*",
                        wait_minutes=10,
                        second_retrieve=False,
                        second_wait_minutes=90,
                    ),
                ]
            )
            db.commit()

    def tearDown(self):
        self.engine.dispose()

    def test_ct_and_default(self):
        with self.Session() as db:
            ct = retrieve_rule_for(db, "CT")
            cr = retrieve_rule_for(db, "CR")
            self.assertEqual(ct.wait_minutes, 15)
            self.assertTrue(ct.second_retrieve)
            self.assertEqual(cr.wait_minutes, 10)
            self.assertFalse(cr.second_retrieve)

    def test_schedule_minutes(self):
        with self.Session() as db:
            _mod, first, second = schedule_from_now(db, "MR")
            delta = first - datetime.now()
            self.assertAlmostEqual(delta.total_seconds() / 60, 15, delta=0.2)
            assert second is not None
            self.assertAlmostEqual(
                (second - datetime.now()).total_seconds() / 60, 90, delta=0.2
            )
            _mod, first, second = schedule_from_now(db, "DX")
            self.assertIsNone(second)
            self.assertAlmostEqual(
                (first - datetime.now()).total_seconds() / 60, 10, delta=0.2
            )


if __name__ == "__main__":
    unittest.main()
