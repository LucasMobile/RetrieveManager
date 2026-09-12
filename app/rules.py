from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import DropModality, ModalityRule
from app.parse import normalize_modality


def retrieve_rule_for(db: Session, modality: str) -> ModalityRule:
    rules = list(db.scalars(select(ModalityRule)))
    by_mod = {r.modality.upper(): r for r in rules}
    if modality and modality.upper() in by_mod:
        return by_mod[modality.upper()]
    return by_mod.get("*") or ModalityRule(
        modality="*", wait_minutes=10, second_retrieve=False, second_wait_minutes=90
    )


def drop_codes(db: Session) -> set[str]:
    return {r.code.upper() for r in db.scalars(select(DropModality))}


def schedule_from_now(
    db: Session, modality_raw: str
) -> tuple[str, datetime, datetime | None]:
    modality = normalize_modality(modality_raw)
    rule = retrieve_rule_for(db, modality)
    now = datetime.now()
    retrieve_at = now + timedelta(minutes=rule.wait_minutes)
    second_at = None
    if rule.second_retrieve:
        second_at = now + timedelta(minutes=rule.second_wait_minutes)
    return modality, retrieve_at, second_at
