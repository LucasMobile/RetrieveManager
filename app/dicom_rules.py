from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from pydicom.datadict import dictionary_description, dictionary_VR
from pydicom.dataset import Dataset
from pydicom.multival import MultiValue
from pydicom.tag import BaseTag, Tag
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.models import (
    DicomRule,
    DicomRuleCondition,
    DicomRuleUnit,
    Settings,
    Unit,
)

COMBINATORS = {"and": "Todas as condições (E)", "or": "Qualquer condição (OU)"}
OPERATORS = {
    "equals": "Igual a",
    "not_equals": "Diferente de",
    "exists": "Existe",
    "not_exists": "Não existe",
    "contains": "Contém",
    "starts_with": "Começa com",
    "ends_with": "Termina com",
    "starts_with_digits": "Começa com e é seguido por números",
}
ACTIONS = {
    "delete": "Excluir imagem",
    "replace": "Substituir valor da tag",
    "remove": "Remover tag",
}
VALUELESS_OPERATORS = frozenset({"exists", "not_exists"})
LEGACY_RULE_KEY = "legacy_study_id_prefix"
NON_TEXT_VRS = frozenset({"OB", "OD", "OF", "OL", "OV", "OW", "SQ", "UN"})


@dataclass(frozen=True)
class RuleConditionSpec:
    tag: str
    operator: str
    value: str


@dataclass(frozen=True)
class RuleSpec:
    id: int
    name: str
    combinator: str
    action: str
    action_tag: str
    action_value: str
    conditions: tuple[RuleConditionSpec, ...]


@dataclass(frozen=True)
class RuleMatch:
    rule_id: int
    rule_name: str
    action: str


@dataclass(frozen=True)
class RuleEvaluation:
    delete_image: bool
    modified: bool
    matches: tuple[RuleMatch, ...]


class RuleExecutionError(RuntimeError):
    def __init__(self, message: str, matches: tuple[RuleMatch, ...]):
        super().__init__(message)
        self.matches = matches


def normalize_dicom_tag(raw: str) -> str:
    value = re.sub(r"[()\s]", "", (raw or "").upper())
    if re.fullmatch(r"[0-9A-F]{8}", value):
        value = f"{value[:4]},{value[4:]}"
    if not re.fullmatch(r"[0-9A-F]{4},[0-9A-F]{4}", value):
        raise ValueError("Use a tag DICOM no formato GGGG,EEEE, por exemplo 0020,0010.")
    tag = Tag(int(value[:4], 16), int(value[5:], 16))
    if tag.is_private:
        raise ValueError(
            "Tags privadas não são permitidas; informe uma tag DICOM padrão."
        )
    try:
        dictionary_VR(tag)
    except KeyError as exc:
        raise ValueError(
            "A tag informada não existe no dicionário DICOM padrão."
        ) from exc
    return value


def dicom_tag_name(raw: str) -> str:
    tag = normalize_dicom_tag(raw)
    try:
        return dictionary_description(Tag(int(tag[:4], 16), int(tag[5:], 16)))
    except KeyError:
        return "Tag DICOM padrão"


def _dicom_tag_vr(raw: str) -> str:
    tag = normalize_dicom_tag(raw)
    return dictionary_VR(Tag(int(tag[:4], 16), int(tag[5:], 16)))


def _has_non_text_vr(vr: str) -> bool:
    return any(part.strip() in NON_TEXT_VRS for part in vr.split(" or "))


def validate_rule_payload(
    *,
    name: str,
    enabled: bool,
    priority: str,
    combinator: str,
    action: str,
    action_tag: str,
    action_value: str,
    unit_ids: Iterable[str],
    condition_tags: Iterable[str],
    condition_operators: Iterable[str],
    condition_values: Iterable[str],
    valid_unit_ids: set[int],
) -> dict[str, Any]:
    clean_name = (name or "").strip()
    if not 1 <= len(clean_name) <= 120:
        raise ValueError("O nome da regra deve ter entre 1 e 120 caracteres.")
    try:
        clean_priority = int(priority)
    except (TypeError, ValueError) as exc:
        raise ValueError("A prioridade deve ser um número inteiro.") from exc
    if not 1 <= clean_priority <= 9999:
        raise ValueError("A prioridade deve estar entre 1 e 9999.")
    if combinator not in COMBINATORS:
        raise ValueError("Combinador inválido.")
    if action not in ACTIONS:
        raise ValueError("Ação inválida.")

    selected_units: list[int] = []
    for raw_unit_id in unit_ids:
        try:
            unit_id = int(raw_unit_id)
        except (TypeError, ValueError) as exc:
            raise ValueError("Unidade inválida.") from exc
        if unit_id not in valid_unit_ids:
            raise ValueError("Uma das unidades selecionadas não existe.")
        if unit_id not in selected_units:
            selected_units.append(unit_id)
    if not selected_units:
        raise ValueError("Selecione pelo menos uma unidade para a regra.")

    tags = list(condition_tags)
    operators = list(condition_operators)
    values = list(condition_values)
    if not tags or len(tags) != len(operators) or len(tags) != len(values):
        raise ValueError("Informe ao menos uma condição completa.")
    if len(tags) > 20:
        raise ValueError("Cada regra pode ter no máximo 20 condições.")

    conditions: list[dict[str, str]] = []
    for raw_tag, operator, value in zip(tags, operators, values, strict=True):
        if operator not in OPERATORS:
            raise ValueError("Operador de condição inválido.")
        clean_value = value.strip()
        if operator not in VALUELESS_OPERATORS and not clean_value:
            raise ValueError("Informe o valor de comparação de todas as condições.")
        clean_tag = normalize_dicom_tag(raw_tag)
        if operator not in VALUELESS_OPERATORS and _has_non_text_vr(
            _dicom_tag_vr(clean_tag)
        ):
            raise ValueError(
                "Tags binárias e sequências aceitam apenas Existe ou Não existe."
            )
        conditions.append(
            {
                "tag": clean_tag,
                "operator": operator,
                "value": "" if operator in VALUELESS_OPERATORS else clean_value,
            }
        )

    clean_action_tag = ""
    clean_action_value = ""
    if action in {"replace", "remove"}:
        clean_action_tag = normalize_dicom_tag(action_tag)
    if action == "replace":
        action_vr = _dicom_tag_vr(clean_action_tag)
        if _has_non_text_vr(action_vr) or " or " in action_vr:
            raise ValueError(
                "Essa tag não aceita substituição textual segura. "
                "Use outra tag padrão ou a ação Remover tag."
            )
        clean_action_value = action_value.strip()
        if not clean_action_value:
            raise ValueError("Informe o novo valor da tag.")
        if len(clean_action_value) > 2048:
            raise ValueError("O novo valor da tag excede 2048 caracteres.")

    return {
        "name": clean_name,
        "enabled": enabled,
        "priority": clean_priority,
        "combinator": combinator,
        "action": action,
        "action_tag": clean_action_tag,
        "action_value": clean_action_value,
        "unit_ids": selected_units,
        "conditions": conditions,
    }


def load_rule_specs(db: Session, unit_id: int) -> tuple[RuleSpec, ...]:
    rules = list(
        db.scalars(
            select(DicomRule)
            .join(DicomRuleUnit)
            .where(
                DicomRule.enabled.is_(True),
                DicomRuleUnit.unit_id == unit_id,
            )
            .options(selectinload(DicomRule.conditions))
            .order_by(DicomRule.priority, DicomRule.id)
        ).unique()
    )
    return tuple(
        RuleSpec(
            id=rule.id,
            name=rule.name,
            combinator=rule.combinator,
            action=rule.action,
            action_tag=rule.action_tag,
            action_value=rule.action_value,
            conditions=tuple(
                RuleConditionSpec(
                    tag=condition.tag,
                    operator=condition.operator,
                    value=condition.value,
                )
                for condition in rule.conditions
            ),
        )
        for rule in rules
        if rule.conditions
    )


def _dataset_tag(tag: str) -> BaseTag:
    return Tag(int(tag[:4], 16), int(tag[5:], 16))


def _normalized_values(dataset: Dataset, tag: str) -> list[str] | None:
    dicom_tag = _dataset_tag(tag)
    if dicom_tag not in dataset:
        return None
    value = dataset[dicom_tag].value
    raw_values = value if isinstance(value, (list, tuple, MultiValue)) else [value]
    return [str(item).strip().casefold() for item in raw_values]


def condition_matches(dataset: Dataset, condition: RuleConditionSpec) -> bool:
    values = _normalized_values(dataset, condition.tag)
    if condition.operator == "exists":
        return values is not None
    if condition.operator == "not_exists":
        return values is None
    if values is None:
        return False
    expected = condition.value.strip().casefold()
    if condition.operator == "equals":
        return any(value == expected for value in values)
    if condition.operator == "not_equals":
        return all(value != expected for value in values)
    if condition.operator == "contains":
        return any(expected in value for value in values)
    if condition.operator == "starts_with":
        return any(value.startswith(expected) for value in values)
    if condition.operator == "ends_with":
        return any(value.endswith(expected) for value in values)
    if condition.operator == "starts_with_digits":
        pattern = rf"^{re.escape(expected)}\d+"
        return any(
            re.match(pattern, value, re.IGNORECASE) is not None for value in values
        )
    return False


def _rule_matches(dataset: Dataset, rule: RuleSpec) -> bool:
    results = [condition_matches(dataset, condition) for condition in rule.conditions]
    return all(results) if rule.combinator == "and" else any(results)


def apply_rule_specs(dataset: Dataset, rules: Iterable[RuleSpec]) -> RuleEvaluation:
    matches: list[RuleMatch] = []
    modified = False
    for rule in rules:
        if not _rule_matches(dataset, rule):
            continue
        match = RuleMatch(rule.id, rule.name, rule.action)
        matches.append(match)
        if rule.action == "delete":
            return RuleEvaluation(True, modified, tuple(matches))
        target = _dataset_tag(rule.action_tag)
        try:
            if rule.action == "remove":
                if target in dataset:
                    del dataset[target]
                    modified = True
            elif rule.action == "replace":
                if target in dataset:
                    dataset[target].value = rule.action_value
                else:
                    dataset.add_new(target, dictionary_VR(target), rule.action_value)
                modified = True
        except Exception as exc:
            raise RuleExecutionError(
                f"Falha ao executar a regra {rule.name}: {type(exc).__name__}",
                tuple(matches),
            ) from exc
    return RuleEvaluation(False, modified, tuple(matches))


def migrate_legacy_study_rule(db: Session) -> bool:
    settings = db.get(Settings, 1)
    if settings is None:
        return False
    prefix = (settings.drop_study_prefix or "").strip().upper()
    if not prefix:
        return False
    units = list(
        db.scalars(
            select(Unit).where(Unit.deleted_at.is_(None)).order_by(Unit.id)
        )
    )
    if not units:
        return False
    existing = db.scalar(
        select(DicomRule).where(DicomRule.system_key == LEGACY_RULE_KEY)
    )
    if existing is None:
        existing = DicomRule(
            name=f"Descartar Study ID {prefix}",
            enabled=True,
            priority=10,
            combinator="and",
            action="delete",
            system_key=LEGACY_RULE_KEY,
        )
        existing.conditions.append(
            DicomRuleCondition(
                position=0,
                tag="0020,0010",
                operator="starts_with_digits",
                value=prefix,
            )
        )
        existing.unit_links.extend(DicomRuleUnit(unit_id=unit.id) for unit in units)
        db.add(existing)
    settings.drop_study_prefix = ""
    return True
