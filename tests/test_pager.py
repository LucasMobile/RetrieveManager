import unittest

from app.pager import paginate


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


if __name__ == "__main__":
    unittest.main()
