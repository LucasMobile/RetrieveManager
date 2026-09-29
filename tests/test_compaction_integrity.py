import tempfile
import unittest
from pathlib import Path

import pydicom
from pydicom.dataelem import DataElement
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import (
    ExplicitVRLittleEndian,
    SecondaryCaptureImageStorage,
    generate_uid,
)

from app.dicom_rules import (
    RuleConditionSpec,
    RuleSpec,
    is_protected_tag,
    load_rule_specs,
    validate_rule_payload,
)
from app.models import DicomRule, DicomRuleCondition, DicomRuleUnit
from app.pipeline.compact import _compact_one
from tests.support import DatabaseTestCase, make_unit


def compact(dataset_setup, rules=()) -> pydicom.Dataset:
    """Run _compact_one on an object without pixel data (kept as received)."""
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        source = root / "CT.1"
        output = root / "send" / "CT.1.dcm"
        output.parent.mkdir()
        file_meta = FileMetaDataset()
        file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
        file_meta.MediaStorageSOPClassUID = SecondaryCaptureImageStorage
        file_meta.MediaStorageSOPInstanceUID = generate_uid()
        image = FileDataset(str(source), {}, file_meta=file_meta, preamble=b"\0" * 128)
        image.SOPClassUID = file_meta.MediaStorageSOPClassUID
        image.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
        image.StudyInstanceUID = generate_uid()
        image.Modality = "CT"
        dataset_setup(image)
        image.save_as(source, enforce_file_format=True)

        result = _compact_one(
            str(source),
            str(output),
            str(root / "error" / "CT.1"),
            str(root / ".work"),
            "unit-token",
            set(),
            {"*": "lossless"},
            tuple(rules),
        )
        assert result.status == "compressed", result
        return pydicom.dcmread(result.temp_output)


class CharacterSetTest(unittest.TestCase):
    def test_declared_utf8_is_preserved_without_replacement_characters(self):
        description = "Crânio – controle “pós” €"

        def setup(image):
            image.SpecificCharacterSet = "ISO_IR 192"
            image.PatientName = "João^Conceição"
            image.StudyDescription = description

        output = compact(setup)
        self.assertEqual(output.SpecificCharacterSet, "ISO_IR 192")
        self.assertEqual(str(output.PatientName), "João^Conceição")
        self.assertEqual(output.StudyDescription, description)
        self.assertEqual(output.InstitutionalDepartmentName, "unit-token")

    def test_undeclared_latin1_bytes_are_labeled_iso_ir_100(self):
        def setup(image):
            # Equipment that writes Latin-1 without declaring (0008,0005).
            image.add(DataElement(0x00100010, "PN", "João^Silva".encode("latin-1")))

        output = compact(setup)
        self.assertEqual(output.SpecificCharacterSet, "ISO_IR 100")
        self.assertEqual(str(output.PatientName), "João^Silva")

    def test_explicit_ascii_is_upgraded_but_iso2022_is_kept(self):
        output = compact(
            lambda image: setattr(image, "SpecificCharacterSet", "ISO_IR 6")
        )
        self.assertEqual(output.SpecificCharacterSet, "ISO_IR 100")

        output = compact(
            lambda image: setattr(
                image, "SpecificCharacterSet", ["ISO 2022 IR 6", "ISO 2022 IR 100"]
            )
        )
        self.assertEqual(
            list(output.SpecificCharacterSet), ["ISO 2022 IR 6", "ISO 2022 IR 100"]
        )


def rule_payload(action: str, action_tag: str, condition_tag: str = "0008,0080"):
    return {
        "name": "Regra",
        "enabled": True,
        "priority": "10",
        "combinator": "and",
        "action": action,
        "action_tag": action_tag,
        "action_value": "NOVO" if action == "replace" else "",
        "unit_ids": ["1"],
        "condition_tags": [condition_tag],
        "condition_operators": ["exists"],
        "condition_values": [""],
        "valid_unit_ids": {1},
    }


class ProtectedTagTest(unittest.TestCase):
    def test_identity_pixel_and_system_tags_are_protected(self):
        for tag in (
            "0010,0020",  # PatientID
            "0010,0010",  # PatientName
            "0010,0030",  # PatientBirthDate
            "0008,0050",  # AccessionNumber
            "0020,000D",  # StudyInstanceUID
            "0020,000e",  # SeriesInstanceUID (lower case input)
            "0008,0018",  # SOPInstanceUID
            "0008,0005",  # SpecificCharacterSet
            "0008,1040",  # token da unidade
            "0028,0010",  # Rows (group 0028)
            "0028,1053",  # RescaleSlope
            "7FE0,0010",  # PixelData
            "0002,0010",  # TransferSyntaxUID (file meta)
        ):
            with self.subTest(tag=tag):
                self.assertTrue(is_protected_tag(tag))
        for tag in ("0008,0080", "0008,1030", "0018,0015", "0008,0060"):
            with self.subTest(tag=tag):
                self.assertFalse(is_protected_tag(tag))

    def test_rule_cannot_replace_or_remove_protected_tag(self):
        for action, tag in (("replace", "0010,0020"), ("remove", "0028,0010")):
            with (
                self.subTest(action=action, tag=tag),
                self.assertRaisesRegex(ValueError, "protegida"),
            ):
                validate_rule_payload(**rule_payload(action, tag))

    def test_protected_tag_is_still_allowed_in_conditions(self):
        payload = validate_rule_payload(
            **rule_payload("replace", "0008,0080", condition_tag="0010,0020")
        )
        self.assertEqual(payload["conditions"][0]["tag"], "0010,0020")
        self.assertEqual(payload["action_tag"], "0008,0080")

    def test_allowed_rule_still_changes_the_image(self):
        rule = RuleSpec(
            id=1,
            name="Instituição",
            combinator="and",
            action="replace",
            action_tag="0008,0080",
            action_value="HOSPITAL",
            conditions=(RuleConditionSpec("0008,0060", "equals", "CT"),),
        )
        output = compact(lambda _image: None, rules=[rule])
        self.assertEqual(output.InstitutionName, "HOSPITAL")


class LegacyProtectedRuleTest(DatabaseTestCase):
    def test_existing_rule_on_protected_tag_is_not_executed(self):
        with self.Session() as db:
            unit = make_unit()
            db.add(unit)
            db.flush()
            for name, action_tag in (
                ("Troca PatientID", "0010,0020"),
                ("Instituição", "0008,0080"),
            ):
                rule = DicomRule(
                    name=name,
                    enabled=True,
                    priority=10,
                    combinator="and",
                    action="replace",
                    action_tag=action_tag,
                    action_value="X",
                )
                db.add(rule)
                db.flush()
                db.add(
                    DicomRuleCondition(
                        rule_id=rule.id,
                        position=0,
                        tag="0008,0060",
                        operator="exists",
                        value="",
                    )
                )
                db.add(DicomRuleUnit(rule_id=rule.id, unit_id=unit.id))
            db.commit()

            specs = load_rule_specs(db, unit.id)
        self.assertEqual([spec.name for spec in specs], ["Instituição"])
