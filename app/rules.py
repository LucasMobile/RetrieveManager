from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models import ModalityRule, Unit, UnitPriorModality
from app.parse import normalize_modality


@dataclass(frozen=True)
class MonitorPlan:
    """How often and for how long new images are looked for."""

    interval_minutes: int
    max_hours: int


def retrieve_rule_for(db: Session, modality: str) -> ModalityRule:
    rules = list(db.scalars(select(ModalityRule)))
    by_mod = {r.modality.upper(): r for r in rules}
    if modality and modality.upper() in by_mod:
        return by_mod[modality.upper()]
    return by_mod.get("*") or ModalityRule(
        modality="*",
        wait_minutes=10,
        monitor_enabled=False,
        monitor_interval_minutes=5,
        monitor_max_hours=6,
    )


def schedule_from_now(db: Session, modality_raw: str) -> tuple[str, datetime]:
    modality = normalize_modality(modality_raw)
    rule = retrieve_rule_for(db, modality)
    return modality, datetime.now() + timedelta(minutes=rule.wait_minutes)


def monitor_plan_for(db: Session, modality: str) -> MonitorPlan | None:
    """Monitoring settings of the modality, or None when it is disabled."""
    rule = retrieve_rule_for(db, normalize_modality(modality))
    if not rule.monitor_enabled:
        return None
    return MonitorPlan(
        interval_minutes=max(1, rule.monitor_interval_minutes or 5),
        max_hours=max(1, rule.monitor_max_hours or 6),
    )


ALL_MODALITIES = "ALL"
# Starting point of a new unit's form; afterwards the saved list prevails.
DEFAULT_PRIOR_MODALITIES = ("CT", "MR")


def normalize_prior_modalities(codes) -> tuple[str, ...]:
    """Sorted codes; empty or containing ALL collapses to ALL alone."""
    values = {str(code).strip().upper() for code in codes if str(code).strip()}
    if not values or ALL_MODALITIES in values:
        return (ALL_MODALITIES,)
    return tuple(sorted(values))


def prior_modalities_for(db: Session, unit_id: int) -> tuple[str, ...]:
    return normalize_prior_modalities(
        db.scalars(
            select(UnitPriorModality.code).where(UnitPriorModality.unit_id == unit_id)
        )
    )


def save_prior_modalities(db: Session, unit: Unit, codes: tuple[str, ...]) -> None:
    db.execute(delete(UnitPriorModality).where(UnitPriorModality.unit_id == unit.id))
    db.add_all(
        UnitPriorModality(unit_id=unit.id, code=code)
        for code in normalize_prior_modalities(codes)
    )


def prior_skip_reason(db: Session, unit: Unit, modality: str) -> str | None:
    """Why the current exam does not retrieve prior exams; None when it does.

    An empty string means the unit has the prior retrieve disabled, which is
    not worth an order event.
    """
    if not unit.retrieve_prior_enabled:
        return ""
    codes = prior_modalities_for(db, unit.id)
    if ALL_MODALITIES in codes or normalize_modality(modality) in codes:
        return None
    return (
        f"Histórico não solicitado: {normalize_modality(modality) or 'modalidade'} "
        "não está nas modalidades de exames anteriores da unidade "
        f"({', '.join(codes)})"
    )
