from app.config import KNOWN_MODALITIES


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
