from dataclasses import dataclass

from app.config import KNOWN_MODALITIES


@dataclass
class OrderFile:
    pat_id: str
    acc: str
    birth_date: str
    exam_date: str


def parse_order_file(text: str) -> OrderFile | None:
    """pat_id:acc:nasc:date_exam:tempo — acc e nasc obrigatórios."""
    line = (text or "").strip().splitlines()
    if not line:
        return None
    parts = line[0].strip().split(":")
    if len(parts) < 4:
        return None
    pat_id = parts[0].strip()
    acc = parts[1].strip()
    birth_date = parts[2].strip()
    exam_date = parts[3].strip()
    if not pat_id or not acc or not birth_date:
        return None
    return OrderFile(pat_id, acc, birth_date, exam_date)


def normalize_modality(
    modalities_in_study: str, known: tuple[str, ...] = KNOWN_MODALITIES
) -> str:
    raw = (modalities_in_study or "").strip()
    chosen = ""
    for mod in known:
        if mod and mod in raw:
            chosen = mod
    if chosen:
        return chosen
    if not raw:
        return ""
    return raw.replace("\\", " ").split()[0]


def parse_findscu_output(output: str) -> tuple[str, str, str]:
    """Retorna (study_uid, modalities_in_study, patient_name)."""
    study_uid = _first_bracket(output, "StudyInstanceUID")
    modalities = _first_bracket(output, "ModalitiesInStudy")
    name = _first_bracket(output, "PatientName")
    return study_uid, modalities, name


def _first_bracket(text: str, tag_name: str) -> str:
    for line in (text or "").splitlines():
        if tag_name in line and "[" in line and "]" in line:
            start = line.find("[")
            end = line.find("]", start)
            if start >= 0 and end > start:
                return line[start + 1 : end].strip()
    return ""
