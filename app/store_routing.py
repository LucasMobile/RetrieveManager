"""Which unit receives a Store SCP association.

Units may share a Store SCP port and called AE title; the sender's AE title
then picks the unit. The same rules serve the unit form, which refuses a
configuration that would be ambiguous, and the receiver, which refuses (never
guesses) when the database holds one anyway.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from itertools import combinations
from pathlib import Path
from typing import Any

from app.config import store_bind_port

# Serializes unit saves that change routing (pg_advisory_xact_lock key).
STORE_ROUTING_LOCK_KEY = 0x53544F52  # "STOR"

FOLDER_LABELS = ("recebimento", "envio", "erro")


def normalize_aet(value: str | None) -> str:
    """Comparison form of an AE title: AE titles are case-insensitive here."""
    return (value or "").strip().upper()


def parse_senders(value: str | None) -> frozenset[str]:
    """Stored comma-separated sender list in its comparison form."""
    return frozenset(
        normalize_aet(item) for item in (value or "").split(",") if item.strip()
    )


def _folder(value: str) -> Path:
    return Path(value).resolve(strict=False)


def _folders_overlap(a: Path, b: Path) -> bool:
    return a == b or a.is_relative_to(b) or b.is_relative_to(a)


@dataclass(frozen=True)
class StoreEndpoint:
    unit_id: int | None
    name: str
    enabled: bool
    store_port: int
    port: int  # bind port inside the container (STORE_PORT_MAP applied)
    called_aet: str
    senders: frozenset[str]
    folders: tuple[Path, Path, Path]  # receive, send, error

    @classmethod
    def from_values(
        cls,
        *,
        unit_id: int | None,
        name: str,
        enabled: bool,
        store_port: int,
        calling_aet: str,
        store_allowed_aets: str,
        receive_dir: str,
        send_dir: str,
        error_dir: str,
    ) -> StoreEndpoint:
        return cls(
            unit_id=unit_id,
            name=name,
            enabled=enabled,
            store_port=int(store_port),
            port=store_bind_port(int(store_port)),
            called_aet=normalize_aet(calling_aet),
            senders=parse_senders(store_allowed_aets),
            folders=(_folder(receive_dir), _folder(send_dir), _folder(error_dir)),
        )

    @classmethod
    def from_unit(cls, unit: Any) -> StoreEndpoint:
        return cls.from_values(
            unit_id=unit.id,
            name=unit.name,
            enabled=bool(unit.enabled),
            store_port=unit.store_port,
            calling_aet=unit.calling_aet,
            store_allowed_aets=unit.store_allowed_aets,
            receive_dir=unit.receive_dir,
            send_dir=unit.send_dir,
            error_dir=unit.error_dir,
        )

    @property
    def listener(self) -> tuple[int, str]:
        return (self.port, self.called_aet)


def _folder_problems(a: StoreEndpoint, b: StoreEndpoint) -> list[str]:
    """Folders shared with another unit: its files would be taken as ours."""
    return [
        f"A pasta de {label_a} ({path_a}) coincide com a pasta de {label_b} da "
        f"unidade {b.name} ({path_b}) ou fica dentro dela."
        for label_a, path_a in zip(FOLDER_LABELS, a.folders, strict=True)
        for label_b, path_b in zip(FOLDER_LABELS, b.folders, strict=True)
        if _folders_overlap(path_a, path_b)
    ]


def _listener_problems(a: StoreEndpoint, b: StoreEndpoint) -> list[str]:
    if a.listener != b.listener:
        return []
    shared = f"porta {a.store_port} e AET {a.called_aet}"
    if not a.senders:
        return [
            f"A {shared} também são usados pela unidade {b.name}; preencha "
            "AE Titles autorizados a enviar para indicar quais remetentes vêm "
            "para esta unidade."
        ]
    if not b.senders:
        return [
            f"A unidade {b.name} usa a mesma {shared} sem AE Titles autorizados; "
            "preencha a lista dela antes de compartilhar a porta."
        ]
    overlap = sorted(a.senders & b.senders)
    if overlap:
        return [
            f"O AE Title {', '.join(overlap)} já direciona as imagens da {shared} "
            f"para a unidade {b.name}."
        ]
    return []


def endpoint_conflicts(
    candidate: StoreEndpoint, others: Iterable[StoreEndpoint]
) -> list[str]:
    """Problems that keep ``candidate`` from being saved next to ``others``.

    ``others`` are every unit not archived, paused ones included: enabling a
    paused unit must never create a conflict.
    """
    problems: list[str] = []
    for other in others:
        if candidate.unit_id is not None and other.unit_id == candidate.unit_id:
            continue
        problems.extend(_listener_problems(candidate, other))
        problems.extend(_folder_problems(candidate, other))
    return problems


@dataclass(frozen=True)
class RoutingPlan:
    """Routing keys of every unit not archived; paused units hold theirs too."""

    exact: Mapping[tuple[int, str, str], int] = field(default_factory=dict)
    open: Mapping[tuple[int, str], int] = field(default_factory=dict)
    ambiguous: frozenset[tuple[int, str, str]] = frozenset()
    listeners: frozenset[tuple[int, str]] = frozenset()
    # Units that must not receive anything, with the reason.
    unit_problems: Mapping[int, str] = field(default_factory=dict)

    def lookup(self, port: int, called: str, calling: str) -> int | None:
        """Unit for an association; None when no unit (or more than one) fits."""
        key = (port, normalize_aet(called), normalize_aet(calling))
        if key in self.ambiguous:
            return None
        unit_id = self.exact.get(key)
        if unit_id is not None:
            return unit_id
        return self.open.get(key[:2])

    def is_ambiguous(self, port: int, called: str, calling: str) -> bool:
        return (port, normalize_aet(called), normalize_aet(calling)) in self.ambiguous

    def knows_listener(self, port: int, called: str) -> bool:
        return (port, normalize_aet(called)) in self.listeners


def build_routing_plan(endpoints: Iterable[StoreEndpoint]) -> RoutingPlan:
    endpoints = [item for item in endpoints if item.unit_id is not None]
    groups: dict[tuple[int, str], list[StoreEndpoint]] = defaultdict(list)
    for endpoint in endpoints:
        groups[endpoint.listener].append(endpoint)

    exact: dict[tuple[int, str, str], int] = {}
    open_routes: dict[tuple[int, str], int] = {}
    ambiguous: set[tuple[int, str, str]] = set()
    problems: dict[int, str] = {}

    for listener, members in groups.items():
        if len(members) == 1 and not members[0].senders:
            open_routes[listener] = members[0].unit_id  # type: ignore[assignment]
            continue
        claims: dict[tuple[int, str, str], list[StoreEndpoint]] = defaultdict(list)
        for member in members:
            if not member.senders:
                # Never a catch-all next to other units: it would take the
                # images of any sender whose AE title changed.
                problems[member.unit_id] = (  # type: ignore[index]
                    f"Porta {member.store_port} compartilhada sem AE Titles "
                    "autorizados a enviar."
                )
                continue
            for sender in member.senders:
                claims[(*listener, sender)].append(member)
        for key, owners in claims.items():
            if len(owners) == 1:
                exact[key] = owners[0].unit_id  # type: ignore[assignment]
                continue
            ambiguous.add(key)
            for owner in owners:
                problems.setdefault(
                    owner.unit_id,  # type: ignore[arg-type]
                    f"AE Title {key[2]} também direcionado para outra unidade na "
                    f"porta {owner.store_port}.",
                )

    for a, b in combinations(endpoints, 2):
        if _folder_problems(a, b):
            for unit, other in ((a, b), (b, a)):
                problems.setdefault(
                    unit.unit_id,  # type: ignore[arg-type]
                    f"Pastas em comum com a unidade {other.name}.",
                )

    return RoutingPlan(
        exact=exact,
        open=open_routes,
        ambiguous=frozenset(ambiguous),
        listeners=frozenset(groups),
        unit_problems=problems,
    )


def units_sharing_listener(
    endpoint: StoreEndpoint, others: Iterable[StoreEndpoint]
) -> list[StoreEndpoint]:
    return [
        other
        for other in others
        if other.unit_id != endpoint.unit_id and other.listener == endpoint.listener
    ]
