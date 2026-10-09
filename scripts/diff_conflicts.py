"""Diagnóstico: quais tags mudam entre cópias do mesmo SOP em conflito.

Uso no servidor:
    docker exec -i retrieve-receiver-1 python - < scripts/diff_conflicts.py
"""

from collections import Counter, defaultdict
from pathlib import Path

from pydicom import dcmread
from sqlalchemy import select

from app.db import SessionLocal
from app.models import DicomInstance

EXAMPLES = 3

with SessionLocal() as db:
    conflicted = select(DicomInstance.sop_uid).where(DicomInstance.state == "conflict")
    rows = db.execute(
        select(
            DicomInstance.sop_uid,
            DicomInstance.state,
            DicomInstance.source_path,
            DicomInstance.received_at,
        ).where(DicomInstance.sop_uid.in_(conflicted))
    ).all()

by_sop = defaultdict(list)
for row in rows:
    if Path(row.source_path).is_file():
        by_sop[row.sop_uid].append(row)
pairs = [files for files in by_sop.values() if len(files) >= 2]
print(
    f"SOPs em conflito: {len({row.sop_uid for row in rows})}; "
    f"com 2+ cópias em disco: {len(pairs)}"
)


def flatten(ds, prefix=""):
    values = {}
    for element in ds:
        if element.tag == 0x7FE00010:
            continue
        key = (
            f"{prefix}({element.tag.group:04X},{element.tag.element:04X}) "
            f"{element.keyword or 'privada'}"
        )
        if element.VR == "SQ":
            for index, item in enumerate(element.value):
                values.update(flatten(item, f"{key}[{index}]."))
        else:
            values[key] = element.value
    return values


changed = Counter()
pixels_equal = 0
for number, files in enumerate(pairs):
    files.sort(key=lambda row: row.received_at)
    first = dcmread(files[0].source_path)
    last = dcmread(files[-1].source_path)
    a, b = flatten(first), flatten(last)
    diff = sorted(key for key in a.keys() | b.keys() if a.get(key) != b.get(key))
    changed.update(diff)
    pixels_equal += getattr(first, "PixelData", None) == getattr(
        last, "PixelData", None
    )
    if number < EXAMPLES:
        print(
            f"\n--- exemplo {number + 1} ({files[0].received_at} -> {files[-1].received_at})"
        )
        for key in diff:
            print(f"  {key}: {repr(a.get(key))[:70]} -> {repr(b.get(key))[:70]}")

if pairs:
    print(f"\nPixelData idêntico em {pixels_equal} de {len(pairs)} pares")
    print("Tags que mudaram (quantidade de pares):")
    for key, count in changed.most_common(30):
        print(f"  {count:5d}  {key}")
