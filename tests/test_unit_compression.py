from app.compression import (
    DEFAULT_PROFILE,
    compression_runtime_settings,
    default_unit_compression_form,
    find_modalities_for_unit,
    save_unit_compression_settings,
    validate_unit_compression_form,
)
from tests.support import DatabaseTestCase, make_unit


class UnitCompressionTest(DatabaseTestCase):
    def test_modality_cannot_belong_to_two_profiles(self):
        with self.assertRaisesRegex(ValueError, "mais de um perfil"):
            validate_unit_compression_form(
                {
                    "compress_lossless": "CT,MR",
                    "compress_lossy": "CT",
                    "drop_modalities": "",
                }
            )

    def test_drop_takes_precedence_over_compression(self):
        settings = validate_unit_compression_form(
            {
                "compress_lossless": "CT,MR",
                "compress_lossy": "CR,US",
                "drop_modalities": "US,SR",
            }
        )

        self.assertEqual(settings.profiles["lossless"], {"CT", "MR"})
        self.assertEqual(settings.profiles["lossy"], {"CR"})
        self.assertEqual(settings.drops, {"SR", "US"})

    def test_settings_are_isolated_per_unit_with_lossless_fallback(self):
        with self.Session() as db:
            first = make_unit(name="first")
            second = make_unit(name="second", store_port=11113)
            db.add_all([first, second])
            db.flush()
            save_unit_compression_settings(
                db,
                first,
                validate_unit_compression_form(
                    {
                        "compress_lossless": "MR",
                        "compress_lossy": "CT",
                        "drop_modalities": "SR",
                    }
                ),
            )
            save_unit_compression_settings(
                db,
                second,
                validate_unit_compression_form(
                    {
                        "compress_lossless": "CT",
                        "compress_lossy": "",
                        "drop_modalities": "US",
                    }
                ),
            )
            db.commit()

            first_drops, first_map = compression_runtime_settings(db, first.id)
            second_drops, second_map = compression_runtime_settings(db, second.id)

            self.assertEqual(first_drops, {"SR"})
            self.assertEqual(first_map["CT"], "lossy")
            self.assertEqual(first_map["*"], DEFAULT_PROFILE)
            self.assertEqual(second_drops, {"US"})
            self.assertEqual(second_map["CT"], "lossless")
            self.assertEqual(second_map["*"], DEFAULT_PROFILE)
            self.assertIn("MR", find_modalities_for_unit(db, first.id))
            self.assertNotIn("SR", find_modalities_for_unit(db, first.id))
            self.assertNotIn("US", find_modalities_for_unit(db, second.id))

    def test_new_unit_form_starts_from_the_defaults(self):
        settings = default_unit_compression_form()
        self.assertEqual(settings.profiles["lossless"], {"CT", "MR", "SC"})
        self.assertEqual(
            settings.profiles["lossy"],
            {"BMD", "CP", "CR", "DX", "ECG", "EEG", "ES", "MG", "NM", "OT", "US", "XA"},
        )
        self.assertIn("CR", settings.profiles["lossy"])
        self.assertIn("SR", settings.drops)
        self.assertFalse(settings.profiles["lossy"] & settings.drops)


if __name__ == "__main__":
    import unittest

    unittest.main()
