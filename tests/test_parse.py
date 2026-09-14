import unittest

from app.parse import (
    normalize_modality,
    parse_findscu_output,
    parse_prior_findscu_output,
    parse_series_body_part,
)


class ParseDicomOutputTest(unittest.TestCase):
    def test_normalize_last_known_wins(self):
        self.assertEqual(normalize_modality("CT\\MR"), "CT")
        self.assertEqual(normalize_modality("US"), "US")
        self.assertEqual(normalize_modality(""), "")

    def test_findscu_brackets(self):
        out = """
(0008,0061) CS [CT]                                      # ModalitiesInStudy
(0018,0015) CS [ABDOMEN]                                 # BodyPartExamined
(0010,0010) PN [SILVA^JOAO]                              # PatientName
(0020,000d) UI [1.2.840.113619.2.55.3]                   # StudyInstanceUID
"""
        uid, mods, name, body_part = parse_findscu_output(out)
        self.assertEqual(uid, "1.2.840.113619.2.55.3")
        self.assertEqual(mods, "CT")
        self.assertEqual(name, "SILVA^JOAO")
        self.assertEqual(body_part, "ABDOMEN")

    def test_findscu_removes_postgres_incompatible_nul_padding(self):
        out = """
(0020,000d) UI [1.2.3\x00] # StudyInstanceUID
(0010,0010) PN [PACIENTE\x00^TESTE] # PatientName
(0018,0015) CS [ABDOMEN\x00] # BodyPartExamined
"""
        uid, _mods, name, body_part = parse_findscu_output(out)
        self.assertEqual(uid, "1.2.3")
        self.assertEqual(name, "PACIENTE^TESTE")
        self.assertEqual(body_part, "ABDOMEN")

    def test_prior_find_ignores_request_and_parses_each_pending_response(self):
        out = """
# Dicom-Data-Set
(0008,0020) DA [20230913-20260912] # StudyDate
(0020,000d) UI [] # StudyInstanceUID
---------------------------
Find Response: 1 (Pending)
# Dicom-Data-Set
(0008,0020) DA [20240110] # StudyDate
(0008,0050) SH [ACC-1] # AccessionNumber
(0008,0060) CS [CT] # Modality
(0018,0015) CS [ABDOMEN] # BodyPartExamined
(0020,000d) UI [1.2.study] # StudyInstanceUID
(0020,000e) UI [1.2.series.1] # SeriesInstanceUID
---------------------------
Find Response: 2 (Pending)
# Dicom-Data-Set
(0008,0020) DA [20240110] # StudyDate
(0008,0060) CS [CT] # Modality
(0020,000d) UI [1.2.study] # StudyInstanceUID
(0020,000e) UI [1.2.series.2] # SeriesInstanceUID
Received Final Find Response (Success)
"""
        results = parse_prior_findscu_output(out)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].study_uid, "1.2.study")
        self.assertEqual(results[0].series_uid, "1.2.series.1")
        self.assertEqual(results[0].accession, "ACC-1")
        self.assertEqual(results[1].series_uid, "1.2.series.2")

    def test_series_body_part_skips_series_without_value(self):
        output = """
Find Response: 1 (Pending)
(0018,0015) CS (no value available) # BodyPartExamined
Find Response: 2 (Pending)
(0018,0015) CS [ABDOMEN] # BodyPartExamined
"""
        self.assertEqual(parse_series_body_part(output), "ABDOMEN")


if __name__ == "__main__":
    unittest.main()
