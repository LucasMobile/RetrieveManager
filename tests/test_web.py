import unittest
from types import SimpleNamespace

from sqlalchemy import select

from app.db import count_rows
from app.models import AuditLog, Unit
from app.web import commit_action, templates
from tests.support import DatabaseTestCase, make_unit


class CommitActionTest(DatabaseTestCase):
    def test_audits_commits_logs_and_redirects_with_a_notice(self):
        request = SimpleNamespace(session={}, client=SimpleNamespace(host="10.0.0.9"))
        user = SimpleNamespace(id=3, username="admin", role="admin")
        with self.Session() as db, self.assertLogs("web", "INFO") as logs:
            response = commit_action(
                db,
                request,
                user,
                action="delete",
                resource_type="dicom_rule",
                resource_id=7,
                resource_name="Regra X",
                summary="Regra DICOM removida.",
                notice="Regra DICOM excluída.",
                redirect_to="/rules",
                event="dicom.rule.delete",
                rule_id=7,
            )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/rules")
        self.assertEqual(
            request.session["flash"], {"text": "Regra DICOM excluída.", "kind": "ok"}
        )
        record = logs.records[0]
        self.assertEqual(record.action, "dicom.rule.delete")
        self.assertEqual(record.resource, "dicom-rule:7")
        self.assertEqual(record.status, "success")
        self.assertEqual((record.user_id, record.rule_id), (3, 7))
        with self.Session() as db:
            entry = db.scalar(select(AuditLog))
        self.assertEqual(
            (entry.action, entry.resource_type, entry.resource_id, entry.ip_address),
            ("delete", "dicom_rule", "7", "10.0.0.9"),
        )

    def test_without_event_nothing_is_logged(self):
        request = SimpleNamespace(session={}, client=None)
        user = SimpleNamespace(id=1, username="admin", role="admin")
        with self.Session() as db, self.assertNoLogs("web", "INFO"):
            commit_action(
                db,
                request,
                user,
                action="retry",
                resource_type="unit",
                resource_id=1,
                resource_name="Unidade",
                summary="ok",
                notice="ok",
                redirect_to="/units/1",
            )


class CountRowsTest(DatabaseTestCase):
    def test_counts_all_rows_or_only_the_matching_ones(self):
        with self.Session() as db:
            db.add_all(
                [
                    make_unit(name="a", store_port=11112),
                    make_unit(name="b", store_port=11113, enabled=False),
                ]
            )
            db.commit()
            self.assertEqual(count_rows(db, Unit), 2)
            self.assertEqual(count_rows(db, Unit, Unit.enabled.is_(True)), 1)
            self.assertEqual(count_rows(db, Unit, Unit.name == "z"), 0)


class PluralFilterTest(unittest.TestCase):
    def test_singular_only_for_exactly_one(self):
        template = templates.env.from_string(
            '{{ n }} {{ n | plural("imagem correspondida", "imagens correspondidas") }}'
        )
        self.assertEqual(template.render(n=0), "0 imagens correspondidas")
        self.assertEqual(template.render(n=1), "1 imagem correspondida")
        self.assertEqual(template.render(n=2), "2 imagens correspondidas")
