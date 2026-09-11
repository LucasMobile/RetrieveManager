import unittest

from app.parse import normalize_modality, parse_findscu_output


class ParseDicomOutputTest(unittest.TestCase):
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
