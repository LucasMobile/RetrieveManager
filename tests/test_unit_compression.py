from unittest.mock import patch

from sqlalchemy import select

from app.compression import (
    DEFAULT_PROFILE,
    compression_form_for_unit,
    compression_runtime_settings,
    find_modalities_for_unit,
    migrate_legacy_unit_compression,
    save_unit_compression_settings,
    validate_unit_compression_form,
)
from app.db import _migrate_compression_profiles
from app.models import (
    CompressRule,
    DropModality,
    UnitCompressionSettings,
    UnitCompressRule,
    UnitDropModality,
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

    def test_legacy_settings_are_cloned_once_for_existing_units(self):
        with self.Session() as db:
            unit = make_unit()
            db.add(unit)
            db.add_all(
                [
                    CompressRule(modality="CT", jpeg_flag="lossy"),
                    CompressRule(modality="US", jpeg_flag="lossy"),
                    CompressRule(modality="*", jpeg_flag="lossless"),
                    DropModality(code="US"),
                ]
            )
            db.commit()

            migrate_legacy_unit_compression(db)
            db.commit()
            settings = compression_form_for_unit(db, unit.id)

            self.assertEqual(settings.profiles["lossy"], {"CT"})
            self.assertEqual(settings.drops, {"US"})
            self.assertIsNotNone(db.get(UnitCompressionSettings, unit.id))

            db.execute(
                UnitCompressRule.__table__.delete().where(
                    UnitCompressRule.unit_id == unit.id
                )
            )
            db.execute(
                UnitDropModality.__table__.delete().where(
                    UnitDropModality.unit_id == unit.id
                )
            )
            db.commit()
            migrate_legacy_unit_compression(db)
            db.commit()

            self.assertEqual(
                list(
                    db.scalars(
                        select(UnitCompressRule).where(
                            UnitCompressRule.unit_id == unit.id
                        )
                    )
                ),
                [],
            )
            self.assertEqual(
                list(
                    db.scalars(
                        select(UnitDropModality).where(
                            UnitDropModality.unit_id == unit.id
                        )
                    )
                ),
                [],
            )

    def test_dcmtk_flags_are_migrated_to_jpeg2000_profiles(self):
        with self.Session() as db:
            unit = make_unit()
            db.add(unit)
            db.flush()
            db.add_all(
                [
                    CompressRule(modality="CR", jpeg_flag="+eb"),
                    CompressRule(modality="*", jpeg_flag="+e1"),
                    UnitCompressionSettings(unit_id=unit.id, default_jpeg_flag="+e1"),
                    UnitCompressRule(unit_id=unit.id, modality="CT", jpeg_flag="+e1"),
                    UnitCompressRule(unit_id=unit.id, modality="DX", jpeg_flag="+eb"),
                    UnitCompressRule(unit_id=unit.id, modality="US", jpeg_flag="+ee"),
                ]
            )
            db.commit()
            unit_id = unit.id

        with patch("app.db.engine", self.engine):
            _migrate_compression_profiles()

        with self.Session() as db:
            settings = compression_form_for_unit(db, unit_id)
            self.assertEqual(settings.profiles["lossless"], {"CT"})
            self.assertEqual(settings.profiles["lossy"], {"DX", "US"})
            self.assertEqual(
                db.get(UnitCompressionSettings, unit_id).default_jpeg_flag,
                "lossless",
            )
            self.assertEqual(
                {
                    row.modality: row.jpeg_flag
                    for row in db.scalars(select(CompressRule))
                },
                {"CR": "lossy", "*": "lossless"},
            )


if __name__ == "__main__":
    import unittest

    unittest.main()
