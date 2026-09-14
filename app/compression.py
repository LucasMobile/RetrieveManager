from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models import (
    CompressRule,
    DropModality,
    Unit,
    UnitCompressionSettings,
    UnitCompressRule,
    UnitDropModality,
)
from app.validation import validate_modality

DEFAULT_JPEG_FLAG = "+e1"
JPEG_PROFILES = {
    "lossless": "+e1",
    "lossy_8": "+eb",
    "lossy_12": "+ee",
}
COMPRESSION_MODALITIES = (
    "BMD",
    "CP",
    "CR",
    "CT",
    "DX",
    "ECG",
    "EEG",
    "ES",
    "MG",
    "MR",
    "NM",
    "OT",
    "SC",
    "US",
    "XA",
)


@dataclass(frozen=True)
class UnitCompressionForm:
    profiles: dict[str, frozenset[str]]
    drops: frozenset[str]

    @property
    def by_flag(self) -> dict[str, frozenset[str]]:
        return {JPEG_PROFILES[name]: values for name, values in self.profiles.items()}


def _parse_modalities(raw: str) -> set[str]:
    if len(raw) > 2048:
        raise ValueError("A lista de modalidades excede o limite permitido.")
    values: set[str] = set()
    for item in raw.split(","):
        item = item.strip()
        if item:
            modality = validate_modality(item)
            if modality == "*":
                raise ValueError("A modalidade padrão não pode ser selecionada.")
            values.add(modality)
            if len(values) > 128:
                raise ValueError("A lista aceita no máximo 128 modalidades.")
    return values


def validate_unit_compression_form(form: dict[str, str]) -> UnitCompressionForm:
    profiles = {
        "lossless": _parse_modalities(form.get("compress_lossless", "")),
        "lossy_8": _parse_modalities(form.get("compress_lossy_8", "")),
        "lossy_12": _parse_modalities(form.get("compress_lossy_12", "")),
    }
    assigned: dict[str, str] = {}
    for profile, modalities in profiles.items():
        for modality in modalities:
            previous = assigned.get(modality)
            if previous is not None:
                raise ValueError(
                    f"A modalidade {modality} aparece em mais de um perfil de "
                    "compactação."
                )
            assigned[modality] = profile

    drops = _parse_modalities(form.get("drop_modalities", ""))
    # O descarte tem precedência: arquivos descartados nunca chegam à compactação.
    normalized = {
        profile: frozenset(modalities - drops)
        for profile, modalities in profiles.items()
    }
    return UnitCompressionForm(normalized, frozenset(drops))


def compression_form_for_unit(db: Session, unit_id: int) -> UnitCompressionForm:
    grouped = {name: set() for name in JPEG_PROFILES}
    flag_to_profile = {flag: name for name, flag in JPEG_PROFILES.items()}
    for rule in db.scalars(
        select(UnitCompressRule).where(UnitCompressRule.unit_id == unit_id)
    ):
        profile = flag_to_profile.get(rule.jpeg_flag)
        if profile:
            grouped[profile].add(rule.modality.upper())
    drops = {
        row.code.upper()
        for row in db.scalars(
            select(UnitDropModality).where(UnitDropModality.unit_id == unit_id)
        )
    }
    return UnitCompressionForm(
        {name: frozenset(values) for name, values in grouped.items()},
        frozenset(drops),
    )


def compression_modalities_for_form(settings: UnitCompressionForm) -> tuple[str, ...]:
    saved = set(settings.drops)
    for values in settings.profiles.values():
        saved.update(values)
    return tuple(sorted(set(COMPRESSION_MODALITIES) | saved))


def save_unit_compression_settings(
    db: Session, unit: Unit, settings: UnitCompressionForm
) -> None:
    if unit.id is None:
        db.flush()
    config = db.get(UnitCompressionSettings, unit.id)
    if config is None:
        db.add(
            UnitCompressionSettings(
                unit_id=unit.id, default_jpeg_flag=DEFAULT_JPEG_FLAG
            )
        )
    else:
        config.default_jpeg_flag = DEFAULT_JPEG_FLAG

    db.execute(delete(UnitCompressRule).where(UnitCompressRule.unit_id == unit.id))
    db.execute(delete(UnitDropModality).where(UnitDropModality.unit_id == unit.id))
    for profile, modalities in settings.profiles.items():
        flag = JPEG_PROFILES[profile]
        db.add_all(
            UnitCompressRule(unit_id=unit.id, modality=code, jpeg_flag=flag)
            for code in sorted(modalities)
        )
    db.add_all(
        UnitDropModality(unit_id=unit.id, code=code) for code in sorted(settings.drops)
    )


def migrate_legacy_unit_compression(db: Session) -> None:
    """Clone the former global configuration once for every existing unit."""
    settings = legacy_unit_compression_form(db)
    configured_units = set(db.scalars(select(UnitCompressionSettings.unit_id)))
    for unit in db.scalars(select(Unit)):
        if unit.id not in configured_units:
            save_unit_compression_settings(db, unit, settings)


def legacy_unit_compression_form(db: Session) -> UnitCompressionForm:
    """Return safe defaults for migration and newly-created units."""
    legacy_drops = {
        row.code.upper() for row in db.scalars(select(DropModality))
    }
    legacy_profiles = {name: set() for name in JPEG_PROFILES}
    flag_to_profile = {flag: name for name, flag in JPEG_PROFILES.items()}
    for row in db.scalars(select(CompressRule)):
        if row.modality == "*":
            continue
        profile = flag_to_profile.get(row.jpeg_flag)
        if profile and row.modality.upper() not in legacy_drops:
            legacy_profiles[profile].add(row.modality.upper())
    return UnitCompressionForm(
        {name: frozenset(values) for name, values in legacy_profiles.items()},
        frozenset(legacy_drops),
    )


def compression_runtime_settings(
    db: Session, unit_id: int
) -> tuple[set[str], dict[str, str]]:
    drops = {
        row.code.upper()
        for row in db.scalars(
            select(UnitDropModality).where(UnitDropModality.unit_id == unit_id)
        )
    }
    compress_map = {
        row.modality.upper(): row.jpeg_flag
        for row in db.scalars(
            select(UnitCompressRule).where(UnitCompressRule.unit_id == unit_id)
        )
    }
    compress_map["*"] = DEFAULT_JPEG_FLAG
    return drops, compress_map


def find_modalities_for_unit(db: Session, unit_id: int) -> frozenset[str]:
    """Return clinical modalities eligible to identify a C-FIND result."""
    drops, compress_map = compression_runtime_settings(db, unit_id)
    configured = set(COMPRESSION_MODALITIES)
    configured.update(code for code in compress_map if code != "*")
    return frozenset(configured - drops)
