import threading
import unittest

from sqlalchemy import event, func, select

from app.db import INSTANCES_RECEIVED_CHANNEL
from app.instances import ReceivedObject, record_instance, record_instances
from app.models import DicomInstance, DicomStudy
from app.notify import NotificationListener
from app.receiver import InstanceWriter
from tests.support import DatabaseTestCase, make_unit


def received(unit_id, sop, sha="a", study="1.2.3", path=None):
    return ReceivedObject(
        unit_id=unit_id,
        study_uid=study,
        series_uid=f"{study}.1",
        sop_uid=sop,
        sop_class_uid="1.2.840.10008.5.1.4.1.1.2",
        transfer_syntax="1.2.840.10008.1.2.1",
        modality="CT",
        sha256=sha * 64,
        size=10,
        path=path or f"/receive/{sop}.{sha}",
        conflict_path=f"/error/{sop}.{sha}",
    )


class RecordInstancesTest(DatabaseTestCase):
    def setUp(self):
        super().setUp()
        with self.Session() as db:
            unit = make_unit()
            db.add(unit)
            db.commit()
            self.unit_id = unit.id

    def test_batch_decides_like_one_at_a_time(self):
        objs = [
            received(self.unit_id, "1"),
            received(self.unit_id, "1"),  # same bytes again: duplicate
            received(self.unit_id, "1", sha="b"),  # other bytes: conflict
            received(self.unit_id, "2"),
            received(self.unit_id, "3", study="9.9"),
        ]
        with self.Session() as db:
            batch = record_instances(db, objs)
            db.commit()
        with self.engine.begin() as connection:
            connection.exec_driver_sql("DELETE FROM dicom_instances")
            connection.exec_driver_sql("DELETE FROM dicom_studies")
        with self.Session() as db:
            single = [record_instance(db, obj) for obj in objs]
            db.commit()

        def shape(decisions):
            return [(d.outcome, d.row_state, d.row_path, d.keep) for d in decisions]

        self.assertEqual(shape(batch), shape(single))
        self.assertEqual(
            [d.outcome for d in batch], ["new", "duplicate", "conflict", "new", "new"]
        )
        # The duplicate points at the row created earlier in the same batch.
        self.assertEqual(batch[0].instance_id, batch[1].instance_id)
        with self.Session() as db:
            counts = dict(
                db.execute(
                    select(DicomStudy.study_uid, DicomStudy.instance_count)
                ).all()
            )
            ids = set(db.scalars(select(DicomInstance.id)))
        # Conflicts are not counted in their study.
        self.assertEqual(counts, {"1.2.3": 2, "9.9": 1})
        self.assertEqual({d.instance_id for d in single}, ids)

    def test_batch_uses_a_fixed_number_of_statements(self):
        statements = []

        def count(*_args):
            statements.append(1)

        event.listen(self.engine, "before_cursor_execute", count)
        self.addCleanup(event.remove, self.engine, "before_cursor_execute", count)
        objs = [received(self.unit_id, str(index)) for index in range(200)]
        with self.Session() as db:
            decisions = record_instances(db, objs)
            db.commit()

        # Read SOPs, read studies, insert the study, insert the instances
        # (one or a few multi-row statements), plus the transaction itself.
        self.assertLess(len(statements), 15)
        self.assertEqual(len({d.instance_id for d in decisions}), 200)
        with self.Session() as db:
            self.assertEqual(
                db.scalar(select(func.count()).select_from(DicomInstance)), 200
            )
            self.assertEqual(db.scalar(select(DicomStudy.instance_count)), 200)

    def test_resent_copy_refreshes_its_study(self):
        with self.Session() as db:
            record_instances(db, [received(self.unit_id, "1")])
            db.commit()
            study = db.scalar(select(DicomStudy))
            first = study.last_received_at

        with self.Session() as db:
            (decision,) = record_instances(db, [received(self.unit_id, "1")])
            db.commit()
            self.assertEqual(decision.outcome, "duplicate")
            self.assertGreater(db.scalar(select(DicomStudy.last_received_at)), first)


class WriterNotifyTest(DatabaseTestCase):
    def test_commit_wakes_the_compaction_of_the_unit(self):
        with self.Session() as db:
            unit = make_unit()
            db.add(unit)
            db.commit()
        listener = NotificationListener(
            INSTANCES_RECEIVED_CHANNEL,
            logger=__import__("logging").getLogger("test"),
            action="test.listen",
        )
        self.addCleanup(listener.close)
        stop = threading.Event()
        self.assertIsNone(listener.wait(stop, 0.1))  # connects and listens

        writer = InstanceWriter(self.Session)
        writer.start()
        self.addCleanup(writer.stop)
        writer.submit(received(unit.id, "1")).result(10)

        self.assertEqual(listener.wait(stop, 5), str(unit.id))
        # A duplicate leaves nothing to compact: no wakeup.
        writer.submit(received(unit.id, "1")).result(10)
        self.assertIsNone(listener.wait(stop, 0.5))


if __name__ == "__main__":
    unittest.main()
