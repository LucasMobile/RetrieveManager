from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.codec import PROFILE_LOSSLESS, PROFILE_LOSSY, PROFILES
from app.models import (
    Unit,
    UnitCompressionSettings,
    UnitCompressRule,
    UnitDropModality,
)
from app.validation import validate_modality

# Stored in the jpeg_flag columns. Modalities without a rule use lossless.
DEFAULT_PROFILE = PROFILE_LOSSLESS
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
        profile: _parse_modalities(form.get(f"compress_{profile}", ""))
        for profile in PROFILES
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
    grouped = {name: set() for name in PROFILES}
    for rule in db.scalars(
        select(UnitCompressRule).where(UnitCompressRule.unit_id == unit_id)
    ):
        if rule.jpeg_flag in grouped:
            grouped[rule.jpeg_flag].add(rule.modality.upper())
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
            UnitCompressionSettings(unit_id=unit.id, default_jpeg_flag=DEFAULT_PROFILE)
        )
    else:
        config.default_jpeg_flag = DEFAULT_PROFILE

    db.execute(delete(UnitCompressRule).where(UnitCompressRule.unit_id == unit.id))
    db.execute(delete(UnitDropModality).where(UnitDropModality.unit_id == unit.id))
    for profile, modalities in settings.profiles.items():
        db.add_all(
            UnitCompressRule(unit_id=unit.id, modality=code, jpeg_flag=profile)
            for code in sorted(modalities)
        )
    db.add_all(
        UnitDropModality(unit_id=unit.id, code=code) for code in sorted(settings.drops)
    )


# Starting point for a new unit's form; each unit is configured on its own.
DEFAULT_LOSSLESS_MODALITIES = frozenset({"CT", "MR", "SC"})
DEFAULT_DROP_MODALITIES = frozenset({"PR", "PS", "SG", "SR", "RA"})
# Every other catalog modality that is not discarded on reception.
DEFAULT_LOSSY_MODALITIES = (
    frozenset(COMPRESSION_MODALITIES)
    - DEFAULT_LOSSLESS_MODALITIES
    - DEFAULT_DROP_MODALITIES
)


def default_unit_compression_form() -> UnitCompressionForm:
    return UnitCompressionForm(
        {
            PROFILE_LOSSLESS: DEFAULT_LOSSLESS_MODALITIES,
            PROFILE_LOSSY: DEFAULT_LOSSY_MODALITIES,
        },
        DEFAULT_DROP_MODALITIES,
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
    compress_map["*"] = DEFAULT_PROFILE
    return drops, compress_map


def find_modalities_for_unit(db: Session, unit_id: int) -> frozenset[str]:
    """Return clinical modalities eligible to identify a C-FIND result."""
    drops, compress_map = compression_runtime_settings(db, unit_id)
    configured = set(COMPRESSION_MODALITIES)
    configured.update(code for code in compress_map if code != "*")
    return frozenset(configured - drops)
