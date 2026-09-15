import unittest

from app.pager import cursor_page_links, paginate


class PagerTest(unittest.TestCase):
    def test_small_result_sets_show_every_page(self):
        self.assertEqual(paginate(70, 4, 10)["page_items"], list(range(1, 8)))

    def test_large_result_sets_compact_page_numbers_near_each_edge(self):
        self.assertEqual(paginate(340, 1, 10)["page_items"], [1, 2, 3, None, 34])
        self.assertEqual(
            paginate(340, 17, 10)["page_items"],
            [1, None, 16, 17, 18, None, 34],
        )
        self.assertEqual(
            paginate(340, 34, 10)["page_items"], [1, None, 32, 33, 34]
        )

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


if __name__ == "__main__":
    unittest.main()
