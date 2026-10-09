"""Diagnóstico: por que uma cópia em conflito não bateu com a primeira.

A primeira cópia já foi compactada e apagada, mas o hash dela está no banco.
Para cada conflito, o script testa hipóteses sobre a cópia em disco até
reproduzir esse hash: bytes idênticos, outras tags privadas, um elemento a
mais. Mostra só tags, VRs e tamanhos, nunca valores.

Uso no servidor:
    docker exec -i retrieve-receiver-1 python - < explain_conflicts.py
    docker exec -i -e AFTER='2026-10-09 12:37:21' -e LIMIT=300 \
        retrieve-receiver-1 python - < explain_conflicts.py
"""

import hashlib
import os
import struct
from collections import Counter
from io import BytesIO
from pathlib import Path

import pydicom
from pydicom.dataelem import RawDataElement
from pydicom.filereader import read_dataset
from pydicom.uid import UID
from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.db import SessionLocal
from app.instances import identity_sha256, parse_header
from app.models import DicomInstance

AFTER = os.environ.get("AFTER", "2026-10-09 12:37:21")
LIMIT = int(os.environ.get("LIMIT", "200"))
LONG_VRS = {
    "OB",
    "OD",
    "OF",
    "OL",
    "OV",
    "OW",
    "SQ",
    "SV",
    "UC",
    "UN",
    "UR",
    "UT",
    "UV",
}


def tag_name(tag):
    keyword = pydicom.datadict.keyword_for_tag(tag) or "privada"
    return f"({tag.group:04X},{tag.element:04X}) {keyword}"


def dataset_bytes(path):
    meta = pydicom.filereader.read_file_meta_info(path)
    data = Path(path).read_bytes()
    return data[132 + 12 + meta.FileMetaInformationGroupLength :], UID(
        meta.TransferSyntaxUID
    )


def layout(raw, implicit):
    """Every top-level element with its byte span; spans of undefined-length
    sequences are inferred from their neighbours."""
    ds = read_dataset(BytesIO(raw), implicit, True)
    items = []
    for tag in list(ds.keys()):
        element = ds.get_item(tag)
        span = None
        if isinstance(element, RawDataElement) and element.value_tell is not None:
            size = 8 if implicit else (12 if element.VR in LONG_VRS else 8)
            start = element.value_tell - size
            end = (
                element.value_tell + element.length
                if element.length != 0xFFFFFFFF
                else None
            )
            if raw[start : start + 4] == struct.pack("<HH", tag.group, tag.element):
                span = [start, end]
        vr = getattr(element, "VR", None)
        undefined = getattr(element, "length", None) == 0xFFFFFFFF or not isinstance(
            element, RawDataElement
        )
        items.append([tag, vr, undefined, span])
    for index, item in enumerate(items):
        if item[3] is None and index > 0 and items[index - 1][3]:
            item[3] = [items[index - 1][3][1], None]
        if item[3] and item[3][1] is None:
            following = items[index + 1][3] if index + 1 < len(items) else None
            item[3][1] = following[0] if following else len(raw)
    return items


def sha_without(raw, spans):
    digest = hashlib.sha256()
    position = 0
    for start, end in sorted(spans):
        if start is None or end is None or start < position:
            continue
        digest.update(raw[position:start])
        position = end
    digest.update(raw[position:])
    return digest.hexdigest()


def nested_private(dataset, depth=0):
    found = set()
    for element in dataset:
        if element.VR == "SQ":
            for item in element.value or []:
                found |= nested_private(item, depth + 1)
        elif depth and element.tag.is_private:
            found.add(tag_name(element.tag))
    return found


Original = aliased(DicomInstance)
with SessionLocal() as db:
    pairs = db.execute(
        select(
            DicomInstance.source_path,
            DicomInstance.modality,
            Original.source_sha256,
            Original.received_at,
        )
        .join(
            Original,
            (Original.unit_id == DicomInstance.unit_id)
            & (Original.sop_uid == DicomInstance.sop_uid)
            & (Original.state != "conflict"),
        )
        .where(DicomInstance.state == "conflict", Original.received_at >= AFTER)
        .order_by(DicomInstance.received_at.desc())
        .limit(LIMIT)
    ).all()

outcomes = Counter()
added = Counter()
private_tops = Counter()
nested = Counter()
examples = []
for path, modality, stored, _received in pairs:
    if not Path(path).is_file():
        outcomes["arquivo do conflito não existe mais"] += 1
        continue
    raw, ts = dataset_bytes(path)
    implicit = ts.is_implicit_VR
    if parse_header(raw, ts) is None:
        outcomes["cabeçalho não pôde ser lido (hash = bytes brutos)"] += 1
    if hashlib.sha256(raw).hexdigest() == stored:
        outcomes["bytes idênticos (não deveria ser conflito)"] += 1
        continue
    if identity_sha256(raw, ts) == stored:
        outcomes["hash de identidade igual (não deveria ser conflito)"] += 1
        continue
    items = layout(raw, implicit)
    for tag, vr, undefined, _span in items:
        if tag.is_private:
            private_tops[
                f"{tag_name(tag)} {vr}{' indefinido' if undefined else ''}"
            ] += 1
    current = [
        tuple(span)
        for tag, _vr, undefined, span in items
        if tag.is_private and span and not undefined
    ]
    every_private = [
        tuple(span) for tag, _vr, _u, span in items if tag.is_private and span
    ]
    if sha_without(raw, every_private) == stored:
        outcomes["só tags privadas (sequência privada ou após Pixel Data)"] += 1
        continue
    explained = False
    for base_name, base in (("+privadas", every_private), ("", current)):
        for tag, _vr, _u, span in items:
            if (
                span
                and not tag.is_private
                and sha_without(raw, base + [tuple(span)]) == stored
            ):
                added[f"{tag_name(tag)} {base_name}"] += 1
                outcomes["um elemento público a mais na cópia nova"] += 1
                explained = True
                break
        if explained:
            break
    if explained:
        continue
    outcomes["não explicado (elemento alterado, não acrescentado)"] += 1
    full = pydicom.dcmread(path, force=True)
    for name in nested_private(full):
        nested[name] += 1
    if len(examples) < 3:
        examples.append(
            (
                modality,
                [
                    f"{tag_name(t)} {vr} {'indef' if u else ''}"
                    for t, vr, u, _s in items
                ],
            )
        )

print(f"Conflitos com primeira cópia depois de {AFTER}: {len(pairs)} analisados\n")
print("Resultado:")
for name, count in outcomes.most_common():
    print(f"  {count:5d}  {name}")
if added:
    print("\nElemento público a mais na cópia nova:")
    for name, count in added.most_common(15):
        print(f"  {count:5d}  {name}")
print("\nTags privadas no nível principal das cópias analisadas:")
for name, count in private_tops.most_common(25):
    print(f"  {count:5d}  {name}")
if nested:
    print("\nTags privadas dentro de sequências (casos não explicados):")
    for name, count in nested.most_common(15):
        print(f"  {count:5d}  {name}")
for modality, tags in examples:
    print(f"\n--- exemplo não explicado ({modality}): elementos do nível principal")
    print("  " + "\n  ".join(tags))
