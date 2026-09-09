import unittest

from app.parse import normalize_modality, parse_findscu_output, parse_order_file


class ParseOrderTest(unittest.TestCase):
    def test_example_file(self):
        parsed = parse_order_file("31841656:9202604600211333:19820226:20260902:60")
        assert parsed is not None
        self.assertEqual(parsed.pat_id, "31841656")
        self.assertEqual(parsed.acc, "9202604600211333")
        self.assertEqual(parsed.birth_date, "19820226")
        self.assertEqual(parsed.exam_date, "20260902")

    def test_requires_acc_and_birth(self):
        self.assertIsNone(parse_order_file(":acc:19820226:20260902:60"))
        self.assertIsNone(parse_order_file("id::19820226:20260902:60"))
        self.assertIsNone(parse_order_file("id:acc::20260902:60"))

    def test_normalize_last_known_wins(self):
        self.assertEqual(normalize_modality("CT\\MR"), "CT")
        self.assertEqual(normalize_modality("US"), "US")
        self.assertEqual(normalize_modality(""), "")

    def test_findscu_brackets(self):
        out = """
(0008,0061) CS [CT]                                      # ModalitiesInStudy
(0010,0010) PN [SILVA^JOAO]                              # PatientName
(0020,000d) UI [1.2.840.113619.2.55.3]                   # StudyInstanceUID
"""
        uid, mods, name = parse_findscu_output(out)
        self.assertEqual(uid, "1.2.840.113619.2.55.3")
        self.assertEqual(mods, "CT")
        self.assertEqual(name, "SILVA^JOAO")


if __name__ == "__main__":
    unittest.main()
