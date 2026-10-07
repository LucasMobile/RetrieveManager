import tempfile
import threading
from datetime import datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm.exc import StaleDataError

from app.instances import ReceivedObject, clear_issue_instances, record_instance
from app.models import AuditLog, DicomInstance, DicomStudy
from tests.support import DatabaseTestCase, make_unit
from tests.web_support import AdminWebTestCase

ISSUES = ["conflict", "missing", "error"]


class IssueFixture:
    """Units with files in their folders and instances in every state."""

    def make_fixture(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        # Units are read after this session closes.
        with self.Session(expire_on_commit=False) as db:
            self.unit_a = self._unit(db, root, "a")
            self.unit_b = self._unit(db, root, "b")
            db.commit()

    def _unit(self, db, root, folder):
        unit = make_unit(
            name=f"Unidade {folder}",
            receive_dir=str(root / folder / "receive"),
            send_dir=str(root / folder / "send"),
            error_dir=str(root / folder / "error"),
        )
        db.add(unit)
        db.flush()
        for directory in (unit.receive_dir, unit.error_dir):
            Path(directory).mkdir(parents=True)
        return unit

    def add_instance(self, unit, state, *, name=None, folder=None, exists=True):
        name = name or f"CT.{state}.{unit.id}.{datetime.now().timestamp()}"
        folder = Path(
            folder or (unit.error_dir if state == "conflict" else unit.receive_dir)
        )
        path = folder / name
        if exists:
            path.write_bytes(b"dicom")
        with self.Session() as db:
            study = db.scalar(
                select(DicomStudy).where(DicomStudy.unit_id == unit.id)
            ) or DicomStudy(
                unit_id=unit.id,
                study_uid=f"1.2.{unit.id}",
                instance_count=0,
                first_received_at=datetime.now(),
                last_received_at=datetime.now(),
            )
            if state != "conflict":
                study.instance_count += 1
            db.add(study)
            db.flush()
            row = DicomInstance(
                unit_id=unit.id,
                study_id=study.id,
                sop_uid=f"1.2.{name}"[:64],
                source_path=str(path),
                source_sha256=name.ljust(64, "0")[:64],
                state=state,
                received_at=datetime.now(),
            )
            db.add(row)
            db.commit()
            return row.id, path

    def states(self):
        with self.Session() as db:
            return {row.id: row.state for row in db.scalars(select(DicomInstance))}


class ClearIssueInstancesTest(IssueFixture, DatabaseTestCase):
    def setUp(self):
        super().setUp()
        self.make_fixture()

    def test_removes_issue_rows_and_their_files_only(self):
        error_id, error_file = self.add_instance(self.unit_a, "error")
        conflict_id, conflict_file = self.add_instance(self.unit_a, "conflict")
        missing_id, _ = self.add_instance(self.unit_a, "missing", exists=False)
        done_id, done_file = self.add_instance(self.unit_a, "compacted")
        queued_id, queued_file = self.add_instance(self.unit_a, "received")

        with self.Session() as db:
            result = clear_issue_instances(db, ISSUES)
            db.commit()

        self.assertEqual((result.instances, result.files_removed), (3, 2))
        self.assertEqual(result.files_kept, 0)
        self.assertEqual(set(self.states()), {done_id, queued_id})
        self.assertFalse(error_file.exists() or conflict_file.exists())
        self.assertTrue(done_file.exists() and queued_file.exists())
        with self.Session() as db:
            # error + missing were counted in the study; the conflict was not.
            study = db.scalar(select(DicomStudy))
            self.assertEqual(study.instance_count, 2)
        self.assertNotIn(error_id, self.states())
        self.assertNotIn(conflict_id, self.states())
        self.assertNotIn(missing_id, self.states())

    def test_filters_by_unit_and_state(self):
        a_error, _ = self.add_instance(self.unit_a, "error")
        a_conflict, _ = self.add_instance(self.unit_a, "conflict")
        b_error, b_file = self.add_instance(self.unit_b, "error")

        with self.Session() as db:
            result = clear_issue_instances(db, ["error"], self.unit_a.id)
            db.commit()

        self.assertEqual(result.instances, 1)
        self.assertEqual(set(self.states()), {a_conflict, b_error})
        self.assertTrue(b_file.exists())

    def test_files_outside_the_unit_folders_are_never_deleted(self):
        outside = Path(self.tmp.name) / "outside"
        outside.mkdir()
        _id, path = self.add_instance(self.unit_a, "error", folder=outside)
        # Another unit's folder is outside this unit's folders too.
        _id, other = self.add_instance(
            self.unit_a, "error", folder=self.unit_b.receive_dir
        )

        with self.Session() as db:
            result = clear_issue_instances(db, ISSUES)
            db.commit()

        self.assertEqual((result.instances, result.files_kept), (2, 2))
        self.assertTrue(path.exists() and other.exists())

    def test_cleared_object_is_new_when_the_pacs_sends_it_again(self):
        _id, path = self.add_instance(self.unit_a, "conflict", name="CT.1.abc")
        with self.Session() as db:
            row = db.scalar(select(DicomInstance))
            obj = ReceivedObject(
                unit_id=self.unit_a.id,
                study_uid="1.2.new",
                series_uid="",
                sop_uid=row.sop_uid,
                sop_class_uid="",
                transfer_syntax="",
                modality="CT",
                sha256=row.source_sha256,
                size=5,
                path=str(Path(self.unit_a.receive_dir) / "CT.1.abc"),
                conflict_path=str(path),
            )
            # Before clearing, the same content is only a duplicate.
            self.assertEqual(record_instance(db, obj).outcome, "duplicate")
            db.rollback()
            clear_issue_instances(db, ISSUES)
            db.commit()

        with self.Session() as db:
            self.assertEqual(record_instance(db, obj).outcome, "new")

    def test_instance_revived_before_the_clear_is_kept(self):
        row_id, path = self.add_instance(self.unit_a, "error")
        with self.Session() as db:
            db.get(DicomInstance, row_id).state = "received"
            db.commit()

        with self.Session() as db:
            result = clear_issue_instances(db, ISSUES)
            db.commit()

        self.assertEqual(result.instances, 0)
        self.assertEqual(self.states(), {row_id: "received"})
        self.assertTrue(path.exists())

    def test_revive_racing_the_clear_waits_and_fails_for_retry(self):
        row_id, _path = self.add_instance(self.unit_a, "error")
        outcome: dict[str, object] = {}

        def revive():
            # What the receiver's writer does when the PACS resends the object.
            with self.Session() as db:
                row = db.get(DicomInstance, row_id)
                row.state = "received"
                try:
                    db.commit()
                    outcome["result"] = "committed"
                except StaleDataError:
                    outcome["result"] = "stale"

        with self.Session() as db:
            clear_issue_instances(db, ISSUES)
            thread = threading.Thread(target=revive)
            thread.start()
            thread.join(timeout=0.5)
            self.assertTrue(thread.is_alive())  # blocked on the deleted row
            db.commit()
        thread.join(timeout=10)

        # The writer fails, the C-STORE answers 0xA700 and the PACS resends.
        self.assertEqual(outcome["result"], "stale")
        self.assertEqual(self.states(), {})


class ClearInstancesPageTest(IssueFixture, AdminWebTestCase):
    def setUp(self):
        super().setUp()
        self.make_fixture()

    def clear(self, **filters):
        data = {"csrf_token": self.token("/instances"), **filters}
        return self.client.post("/instances/clear", data=data, follow_redirects=False)

    def test_button_clears_the_filtered_queue_and_is_audited(self):
        self.add_instance(self.unit_a, "error")
        self.add_instance(self.unit_a, "conflict")
        _id, kept = self.add_instance(self.unit_b, "error")

        page = self.client.get("/instances", params={"unit_id": self.unit_a.id})
        self.assertIn("Limpar pendências", page.text)
        self.assertIn("Unidade a", page.text)

        response = self.clear(unit_id=str(self.unit_a.id))

        self.assertEqual(response.status_code, 303)
        self.assertEqual(
            response.headers["location"], f"/instances?unit_id={self.unit_a.id}"
        )
        self.assertEqual(len(self.states()), 1)
        self.assertTrue(kept.exists())
        with self.Session() as db:
            entry = db.scalar(
                select(AuditLog).where(AuditLog.resource_type == "instance")
            )
        self.assertEqual(entry.action, "delete")
        self.assertIn("2 registros excluídos", entry.summary)

    def test_invalid_state_is_refused(self):
        self.add_instance(self.unit_a, "error")

        self.assertEqual(self.clear(state="compacted").status_code, 422)
        self.assertEqual(len(self.states()), 1)

    def test_button_is_hidden_without_pending_instances(self):
        self.assertNotIn("Limpar pendências", self.client.get("/instances").text)
