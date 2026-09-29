import unittest

from pydicom.dataset import Dataset

from app.parse import (
    dataset_text,
    first_series_body_part,
    normalize_modality,
    patient_id_matches,
    prior_series_results,
    series_have_modality,
    series_metadata,
    study_response,
)


def response(**values) -> Dataset:
    ds = Dataset()
    for keyword, value in values.items():
        setattr(ds, keyword, value)
    return ds


class ParseResponsesTest(unittest.TestCase):
    def test_normalize_last_known_wins(self):
        self.assertEqual(normalize_modality("CT\\MR"), "CT")
        self.assertEqual(normalize_modality("US"), "US")
        self.assertEqual(normalize_modality(""), "")

    def test_study_response_reads_identity_and_multi_valued_modalities(self):
        parsed = study_response(
            response(
                StudyInstanceUID="1.2.840.113619.2.55.3",
                AccessionNumber="ACC-1",
                PatientID="12345",
                PatientBirthDate="19800101",
                PatientName="SILVA^JOAO",
                ModalitiesInStudy=["CT", "SR"],
                BodyPartExamined="ABDOMEN",
                NumberOfStudyRelatedInstances="120",
            )
        )
        self.assertEqual(parsed.study_uid, "1.2.840.113619.2.55.3")
        self.assertEqual(parsed.accession, "ACC-1")
        self.assertEqual(parsed.patient_id, "12345")
        self.assertEqual(parsed.birth_date, "19800101")
        self.assertEqual(parsed.patient_name, "SILVA^JOAO")
        self.assertEqual(parsed.modalities, "CT\\SR")
        self.assertEqual(parsed.body_part, "ABDOMEN")
        self.assertEqual(parsed.instance_count, "120")

    def test_missing_elements_are_empty_and_nul_padding_is_removed(self):
        parsed = study_response(
            response(StudyInstanceUID="1.2.3\x00", BodyPartExamined="ABDOMEN\x00")
        )
        self.assertEqual(parsed.study_uid, "1.2.3")
        self.assertEqual(parsed.body_part, "ABDOMEN")
        self.assertEqual(parsed.patient_id, "")
        self.assertEqual(dataset_text(Dataset(), "PatientName"), "")

    def test_prior_series_are_unique_and_require_both_uids(self):
        results = prior_series_results(
            [
                response(
                    StudyInstanceUID="1.2.study",
                    SeriesInstanceUID="1.2.series.1",
                    AccessionNumber="ACC-1",
                    StudyDate="20240110",
                    Modality="CT",
                    PatientID="30211738",
                    PatientBirthDate="19691027",
                ),
                response(
                    StudyInstanceUID="1.2.study", SeriesInstanceUID="1.2.series.1"
                ),
                response(
                    StudyInstanceUID="1.2.study", SeriesInstanceUID="1.2.series.2"
                ),
                response(StudyInstanceUID="1.2.study"),
            ]
        )
        self.assertEqual(
            [result.series_uid for result in results], ["1.2.series.1", "1.2.series.2"]
        )
        self.assertEqual(results[0].study_uid, "1.2.study")

    def test_series_metadata_skips_non_clinical_first_series(self):
        responses = [
            response(Modality="SR", BodyPartExamined="CHEST"),
            response(Modality="MR", BodyPartExamined="ABDOMEN"),
            response(Modality="PR"),
        ]
        self.assertEqual(series_metadata(responses, {"CT", "MR"}), ("MR", "ABDOMEN"))
        self.assertTrue(series_have_modality(responses))

    def test_series_body_part_skips_series_without_value(self):
        responses = [response(BodyPartExamined=""), response(BodyPartExamined="HEAD")]
        self.assertEqual(first_series_body_part(responses), "HEAD")
        self.assertFalse(series_have_modality(responses))

    def test_patient_id_is_exact_unless_suffix_is_allowed(self):
        self.assertTrue(patient_id_matches(" 123 ", "123", allow_suffix=False))
        self.assertFalse(patient_id_matches("1234", "123", allow_suffix=False))
        self.assertTrue(patient_id_matches("123-1", "123", allow_suffix=True))
        self.assertFalse(patient_id_matches("12", "123", allow_suffix=True))
        self.assertFalse(patient_id_matches("", "123", allow_suffix=True))
        self.assertFalse(patient_id_matches("123", "", allow_suffix=True))


if __name__ == "__main__":
    unittest.main()
