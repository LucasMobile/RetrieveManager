import tempfile
import unittest
from pathlib import Path

from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
from pydicom.uid import (
    ExplicitVRLittleEndian,
    SecondaryCaptureImageStorage,
    generate_uid,
)
from sqlalchemy import select

from app.dicom_rules import (
    RuleConditionSpec,
    RuleSpec,
    apply_rule_specs,
    condition_matches,
    load_rule_specs,
    migrate_legacy_study_rule,
    normalize_dicom_tag,
    validate_rule_payload,
)
from app.models import (
    DicomRule,
    DicomRuleCondition,
    DicomRuleUnit,
    Settings,
    Unit,
)
from app.pipeline import _compact_one
from tests.support import DatabaseTestCase, make_unit


def rule(
    *conditions: RuleConditionSpec,
    combinator: str = "and",
    action: str = "delete",
    action_tag: str = "",
    action_value: str = "",
) -> RuleSpec:
    return RuleSpec(
        id=1,
        name="Regra de teste",
        combinator=combinator,
        action=action,
        action_tag=action_tag,
        action_value=action_value,
        conditions=conditions,
    )


class DicomRulesTest(DatabaseTestCase):
    @staticmethod
    def _unit(name: str) -> Unit:
        return make_unit(
            name=name,
            orders_api_url="https://integracao.example/v1/pedidos",
            orders_api_token="integration-token",
            pacs_port=2104,
            store_port=444,
            receive_dir="/receive",
            send_dir="/send",
            error_dir="/error",
            token="token",
        )

    def test_tag_normalization_accepts_only_standard_tags(self):
        self.assertEqual(normalize_dicom_tag("(0020, 0010)"), "0020,0010")
        self.assertEqual(normalize_dicom_tag("00200010"), "0020,0010")
        with self.assertRaises(ValueError):
            normalize_dicom_tag("0011,0010")
        with self.assertRaises(ValueError):
            normalize_dicom_tag("not-a-tag")

    def test_and_or_and_case_insensitive_comparisons(self):
        image = Dataset()
        image.StudyID = "SlRx123"
        image.Modality = "CT"
        study = RuleConditionSpec("0020,0010", "starts_with_digits", "slrx")
        modality = RuleConditionSpec("0008,0060", "equals", "ct")

        self.assertTrue(apply_rule_specs(image, [rule(study, modality)]).delete_image)
        self.assertTrue(
            apply_rule_specs(
                image,
                [
                    rule(
                        RuleConditionSpec("0008,0060", "equals", "MR"),
                        study,
                        combinator="or",
                    )
                ],
            ).delete_image
        )

    def test_legacy_prefix_preserves_numeric_suffix_behavior(self):
        condition = RuleConditionSpec("0020,0010", "starts_with_digits", "SLRX")
        for value in ("SLRX1", "slrx123ABC"):
            image = Dataset()
            image.StudyID = value
            self.assertTrue(condition_matches(image, condition))
        for value in ("SLRX", "SLRXABC", "OTHER123"):
            image = Dataset()
            image.StudyID = value
            self.assertFalse(condition_matches(image, condition))

    def test_missing_tag_is_distinct_from_not_equal(self):
        image = Dataset()
        self.assertTrue(
            condition_matches(
                image,
                RuleConditionSpec("0008,0080", "not_exists", ""),
            )
        )
        self.assertFalse(
            condition_matches(
                image,
                RuleConditionSpec("0008,0080", "not_equals", "Hospital"),
            )
        )

    def test_replace_can_create_tag_and_remove_deletes_it(self):
        image = Dataset()
        replace = rule(
            RuleConditionSpec("0008,0080", "not_exists", ""),
            action="replace",
            action_tag="0008,0080",
            action_value="Mobilemed",
        )
        result = apply_rule_specs(image, [replace])
        self.assertTrue(result.modified)
        self.assertEqual(image.InstitutionName, "Mobilemed")

        remove = rule(
            RuleConditionSpec("0008,0080", "exists", ""),
            action="remove",
            action_tag="0008,0080",
        )
        apply_rule_specs(image, [remove])
        self.assertNotIn("InstitutionName", image)

    def test_compaction_pipeline_deletes_file_when_rule_matches(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "CT-image"
            file_meta = FileMetaDataset()
            file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
            file_meta.MediaStorageSOPClassUID = SecondaryCaptureImageStorage
            file_meta.MediaStorageSOPInstanceUID = generate_uid()
            image = FileDataset(
                str(source),
                {},
                file_meta=file_meta,
                preamble=b"\0" * 128,
            )
            image.SOPClassUID = file_meta.MediaStorageSOPClassUID
            image.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
            image.StudyInstanceUID = generate_uid()
            image.StudyID = "SLRX123"
            image.Modality = "CT"
            image.save_as(source, enforce_file_format=True)

            result = _compact_one(
                str(source),
                str(Path(directory) / "output.dcm"),
                str(Path(directory) / "error.dcm"),
                "token",
                set(),
                {"*": "+e1"},
                (
                    rule(
                        RuleConditionSpec(
                            "0020,0010",
                            "starts_with_digits",
                            "SLRX",
                        )
                    ),
                ),
            )

            self.assertEqual(result.status, "discarded_rule")
            self.assertEqual(result.rule_matches[0].rule_name, "Regra de teste")
            self.assertFalse(source.exists())

    def test_payload_validates_units_conditions_and_action_tag(self):
        payload = validate_rule_payload(
            name="Normalizar instituição",
            enabled=True,
            priority="20",
            combinator="and",
            action="replace",
            action_tag="0008,0080",
            action_value="Mobilemed",
            unit_ids=["1", "2"],
            condition_tags=["0008,0060", "0008,0080"],
            condition_operators=["equals", "not_exists"],
            condition_values=["CT", ""],
            valid_unit_ids={1, 2},
        )
        self.assertEqual(payload["unit_ids"], [1, 2])
        self.assertEqual(len(payload["conditions"]), 2)
        self.assertEqual(payload["action_tag"], "0008,0080")

    def test_payload_limits_binary_tags_to_safe_operations(self):
        common = {
            "name": "Regra binária",
            "enabled": True,
            "priority": "20",
            "combinator": "and",
            "unit_ids": ["1"],
            "valid_unit_ids": {1},
        }
        with self.assertRaisesRegex(ValueError, "apenas Existe ou Não existe"):
            validate_rule_payload(
                **common,
                action="delete",
                action_tag="",
                action_value="",
                condition_tags=["7FE0,0010"],
                condition_operators=["equals"],
                condition_values=["conteúdo"],
            )

        payload = validate_rule_payload(
            **common,
            action="delete",
            action_tag="",
            action_value="",
            condition_tags=["7FE0,0010"],
            condition_operators=["exists"],
            condition_values=[""],
        )
        self.assertEqual(payload["conditions"][0]["operator"], "exists")

        with self.assertRaisesRegex(ValueError, "substituição textual segura"):
            validate_rule_payload(
                **common,
                action="replace",
                action_tag="7FE0,0010",
                action_value="novo valor",
                condition_tags=["0008,0060"],
                condition_operators=["equals"],
                condition_values=["CT"],
            )

    def test_rules_are_loaded_only_for_selected_unit_in_priority_order(self):
        with self.Session() as db:
            first = self._unit("A")
            second = self._unit("B")
            db.add_all([first, second])
            db.flush()
            high = DicomRule(
                name="Prioridade alta",
                enabled=True,
                priority=10,
                combinator="and",
                action="delete",
            )
            low = DicomRule(
                name="Prioridade baixa",
                enabled=True,
                priority=100,
                combinator="and",
                action="delete",
            )
            for item in (high, low):
                item.conditions.append(
                    DicomRuleCondition(tag="0008,0060", operator="equals", value="CT")
                )
                item.unit_links.append(DicomRuleUnit(unit_id=first.id))
            db.add_all([low, high])
            db.commit()

            loaded = load_rule_specs(db, first.id)
            self.assertEqual(
                [item.name for item in loaded],
                ["Prioridade alta", "Prioridade baixa"],
            )
            self.assertEqual(load_rule_specs(db, second.id), ())

    def test_legacy_setting_is_migrated_to_all_existing_units(self):
        with self.Session() as db:
            db.add(
                Settings(
                    id=1,
                    drop_study_prefix="SLRX",
                )
            )
            db.add_all([self._unit("A"), self._unit("B")])
            db.commit()

            self.assertTrue(migrate_legacy_study_rule(db))
            db.commit()

            migrated = db.scalar(select(DicomRule))
            self.assertEqual(migrated.action, "delete")
            self.assertEqual(migrated.conditions[0].tag, "0020,0010")
            self.assertEqual(
                migrated.conditions[0].operator,
                "starts_with_digits",
            )
            self.assertEqual(len(migrated.unit_links), 2)
            self.assertEqual(db.get(Settings, 1).drop_study_prefix, "")
            self.assertFalse(migrate_legacy_study_rule(db))


if __name__ == "__main__":
    unittest.main()
