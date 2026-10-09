import unittest

from sqlalchemy import event, select
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.models import AuditLog
from app.pager import cursor_page_links, keyset_page, paginate
from tests.support import DatabaseTestCase


class PagerTest(unittest.TestCase):
    def test_small_result_sets_show_every_page(self):
        self.assertEqual(paginate(70, 4, 10)["page_items"], list(range(1, 8)))

    def test_large_result_sets_compact_page_numbers_near_each_edge(self):
        self.assertEqual(paginate(340, 1, 10)["page_items"], [1, 2, 3, None, 34])
        self.assertEqual(
            paginate(340, 17, 10)["page_items"],
            [1, None, 16, 17, 18, None, 34],
        )
        self.assertEqual(paginate(340, 34, 10)["page_items"], [1, None, 32, 33, 34])

    def test_cursor_links_require_a_cursor_for_each_intermediate_page(self):
        pager = paginate(190, 1, 30)
        links = cursor_page_links(
            pager,
            page_cursors={2: 161, 3: 131, 4: 101, 5: 71, 6: 41},
        )

        numbered = {item["page"]: item for item in links if item is not None}
        self.assertEqual(numbered[4]["query"], "before=101&page=4")
        self.assertEqual(numbered[6]["query"], "before=41&page=6")
        self.assertEqual(numbered[7]["query"], "last=1&page=7")
        self.assertFalse(any(item.get("disabled") for item in numbered.values()))

    def test_cursor_link_without_an_anchor_is_disabled(self):
        pager = paginate(190, 1, 30)
        links = cursor_page_links(pager, page_cursors={2: 161})

        page_four = next(item for item in links if item and item["page"] == 4)
        self.assertTrue(page_four["disabled"])


class KeysetPageTest(DatabaseTestCase):
    def setUp(self):
        super().setUp()
        self.db = self.Session()
        self.addCleanup(self.db.close)
        self.db.add_all(
            AuditLog(
                actor_username="tester",
                actor_role="admin",
                action="create",
                resource_type="unit",
                resource_id=str(index),
                resource_name=f"Log {index}",
                summary="",
            )
            for index in range(25)
        )
        self.db.commit()

    def page(self, **kwargs):
        args = {
            "page": 1,
            "page_size": 10,
            "size_options": (10, 20),
            "before": None,
            "after": None,
            "last": False,
            "keep": {"q": "x"},
        }
        args.update(kwargs)
        rows, pager = keyset_page(self.db, select(AuditLog), AuditLog.id, **args)
        return [row.id for row in rows], pager

    def test_walks_forward_back_and_to_the_last_page(self):
        first, pager = self.page()
        self.assertEqual(first, list(range(25, 15, -1)))
        self.assertEqual((pager["total"], pager["pages"]), (25, 3))
        self.assertFalse(pager["has_prev"])
        self.assertTrue(pager["has_next"])
        self.assertEqual(pager["keep"], {"q": "x"})
        self.assertEqual(pager["size_options"], (10, 20))

        second, pager = self.page(before=pager["next_cursor"], page=2)
        self.assertEqual(second, list(range(15, 5, -1)))
        self.assertEqual(pager["page"], 2)
        self.assertTrue(pager["has_prev"] and pager["has_next"])

        back, pager = self.page(after=pager["prev_cursor"], page=1)
        self.assertEqual(back, first)
        self.assertFalse(pager["has_prev"])

        last, pager = self.page(last=True, page=3)
        self.assertEqual(last, [5, 4, 3, 2, 1])
        self.assertEqual(pager["page"], 3)
        self.assertTrue(pager["has_prev"])
        self.assertFalse(pager["has_next"])

    def test_count_limit_caps_the_total_and_keeps_the_cursors_working(self):
        first, pager = self.page(count_limit=20)
        self.assertEqual(first, list(range(25, 15, -1)))
        self.assertTrue(pager["capped"])
        self.assertEqual(pager["total"], 20)
        self.assertEqual(pager["total_label"], "mais de 20")
        self.assertEqual((pager["page"], pager["from"], pager["to"]), (1, 1, 10))
        self.assertEqual(pager["page_links"], [{"page": 1, "current": True}])
        self.assertTrue(pager["has_next"])

        second, pager = self.page(before=pager["next_cursor"], page=2, count_limit=20)
        self.assertEqual(second, list(range(15, 5, -1)))
        self.assertEqual((pager["page"], pager["from"], pager["to"]), (2, 11, 20))
        self.assertEqual((pager["prev"], pager["next"]), (1, 3))

        # The oldest page is the oldest rows; its number is unknown (0).
        last, pager = self.page(last=True, page=0, count_limit=20)
        self.assertEqual(last, list(range(10, 0, -1)))
        self.assertEqual(
            (pager["page"], pager["shown"], pager["page_links"]), (0, 10, [])
        )
        self.assertTrue(pager["has_prev"])
        self.assertFalse(pager["has_next"])

        newer, pager = self.page(after=pager["prev_cursor"], page=0, count_limit=20)
        self.assertEqual(newer, list(range(20, 10, -1)))
        self.assertEqual((pager["page"], pager["prev"]), (0, 0))

    def test_count_limit_not_reached_keeps_the_exact_pager(self):
        _ids, pager = self.page(count_limit=25)
        self.assertFalse(pager["capped"])
        self.assertEqual((pager["total"], pager["pages"]), (25, 3))
        self.assertEqual(pager["total_label"], "25")
        self.assertEqual(len(pager["page_links"]), 3)

    def test_known_total_skips_the_count_query(self):
        statements = []

        def track(_conn, _cursor, statement, *_args):
            statements.append(statement)

        event.listen(self.engine, "before_cursor_execute", track)
        self.addCleanup(event.remove, self.engine, "before_cursor_execute", track)
        rows, pager = self.page(total=25)

        self.assertEqual(rows, list(range(25, 15, -1)))
        self.assertEqual((pager["total"], pager["pages"]), (25, 3))
        self.assertFalse(any("count(" in statement for statement in statements))

    def test_page_without_cursor_falls_back_to_the_first_page(self):
        ids, pager = self.page(page=1)
        self.assertEqual(ids[0], 25)
        self.assertEqual(pager["page"], 1)

    def test_empty_result_has_no_neighbours(self):
        rows, pager = keyset_page(
            self.db,
            select(AuditLog).where(AuditLog.action == "delete"),
            AuditLog.id,
            page=1,
            page_size=10,
            size_options=(10,),
            before=None,
            after=None,
            last=False,
            keep={},
        )
        self.assertEqual(rows, [])
        self.assertFalse(pager["has_prev"] or pager["has_next"])
        self.assertIsNone(pager["next_cursor"])

    def test_rejects_invalid_cursor_combinations(self):
        for kwargs in (
            {"page_size": 25},
            {"before": 10, "last": True},
            {"before": 10, "after": 5},
            {"page": 2},
        ):
            with (
                self.subTest(**kwargs),
                self.assertRaises(StarletteHTTPException) as raised,
            ):
                self.page(**kwargs)
            self.assertEqual(raised.exception.status_code, 422)


if __name__ == "__main__":
    unittest.main()
