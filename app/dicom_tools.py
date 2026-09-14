from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

from app.config import DCMCJPEG, ECHOSCU, FINDSCU, MOVESCU, STORESCP


class ToolMissing(RuntimeError):
    pass


_SENSITIVE_DICOM_FIELDS = re.compile(
    r"(PatientName|PatientID|PatientBirthDate|AccessionNumber|"
    r"BodyPartExamined|0010,0010|0010,0020|0010,0030|0008,0050|0018,0015)",
    re.IGNORECASE,
)


def _require(path: str, name: str) -> str:
    if Path(path).exists() or shutil.which(path):
        return path
    which = shutil.which(name)
    if which:
        return which
    raise ToolMissing(f"{name} não encontrado ({path})")


def findscu_cmd(
    bin_path: str,
    calling_aet: str,
    pacs_aet: str,
    pacs_ip: str,
    pacs_port: int,
    accession: str,
    birth_date: str,
) -> list[str]:
    return [
        bin_path,
        "-v",
        "-S",
        "-k",
        "0008,0052=STUDY",
        "-k",
        "0008,0061=",
        "-k",
        "0018,0015=",
        "-k",
        "0010,0010=",
        "-k",
        "0010,0020=",
        "-k",
        f"0010,0030={birth_date}",
        "-k",
        f"0008,0050={accession}",
        "-k",
        "0020,000d=",
        "-aet",
        calling_aet,
        "-aec",
        pacs_aet,
        pacs_ip,
        str(pacs_port),
    ]


def echoscu_cmd(
    bin_path: str,
    calling_aet: str,
    pacs_aet: str,
    pacs_ip: str,
    pacs_port: int,
) -> list[str]:
    return [
        bin_path,
        "-v",
        "-aet",
        calling_aet,
        "-aec",
        pacs_aet,
        pacs_ip,
        str(pacs_port),
    ]


def movescu_cmd(
    bin_path: str,
    calling_aet: str,
    pacs_aet: str,
    pacs_ip: str,
    pacs_port: int,
    study_uid: str,
) -> list[str]:
    """Build a Study Root C-MOVE using the calling AET as move destination."""
    return [
        bin_path,
        "-v",
        "-S",
        "-pdu",
        "65534",
        "-aet",
        calling_aet,
        "-aec",
        pacs_aet,
        "-k",
        "0008,0052=STUDY",
        "-k",
        f"0020,000D={study_uid}",
        pacs_ip,
        str(pacs_port),
    ]


def prior_findscu_cmd(
    bin_path: str,
    calling_aet: str,
    pacs_aet: str,
    pacs_ip: str,
    pacs_port: int,
    body_part: str,
    modality: str,
    patient_id: str,
    birth_date: str,
    date_range: str,
) -> list[str]:
    """Build a SERIES query that discovers the exact historical studies."""
    return [
        bin_path,
        "-v",
        "-S",
        "-k",
        "0008,0052=SERIES",
        "-k",
        "0020,000D=",
        "-k",
        "0020,000E=",
        "-k",
        "0008,0050=",
        "-k",
        "0008,1030=",
        "-k",
        f"0018,0015={body_part}",
        "-k",
        f"0008,0060={modality}",
        "-k",
        f"0010,0020={patient_id}*",
        "-k",
        f"0010,0030={birth_date}",
        "-k",
        f"0008,0020={date_range}",
        "-aet",
        calling_aet,
        "-aec",
        pacs_aet,
        pacs_ip,
        str(pacs_port),
    ]


def series_body_part_findscu_cmd(
    bin_path: str,
    calling_aet: str,
    pacs_aet: str,
    pacs_ip: str,
    pacs_port: int,
    study_uid: str,
) -> list[str]:
    """Build a SERIES query used to find a non-empty BodyPartExamined."""
    return [
        bin_path,
        "-v",
        "-S",
        "-k",
        "0008,0052=SERIES",
        "-k",
        f"0020,000D={study_uid}",
        "-k",
        "0020,000E=",
        "-k",
        "0018,0015=",
        "-aet",
        calling_aet,
        "-aec",
        pacs_aet,
        pacs_ip,
        str(pacs_port),
    ]


def prior_series_movescu_cmd(
    bin_path: str,
    calling_aet: str,
    pacs_aet: str,
    pacs_ip: str,
    pacs_port: int,
    study_uid: str,
    series_uid: str,
) -> list[str]:
    """Build an exact SERIES-level C-MOVE for a discovered historical series."""
    return [
        bin_path,
        "-v",
        "-S",
        "-pdu",
        "65534",
        "-aet",
        calling_aet,
        "-aec",
        pacs_aet,
        "-k",
        "0008,0052=SERIES",
        "-k",
        f"0020,000D={study_uid}",
        "-k",
        f"0020,000E={series_uid}",
        pacs_ip,
        str(pacs_port),
    ]


def storescp_cmd(bin_path: str, aet: str, port: int, output_dir: str) -> list[str]:
    return [
        bin_path,
        "+xa",
        "-pdu",
        "65534",
        "--fork",
        "-od",
        output_dir,
        "-aet",
        aet,
        str(port),
    ]


def dcmcjpeg_cmd(bin_path: str, flag: str, src: str, dest: str) -> list[str]:
    return [bin_path, "-q", "+un", flag, src, dest]


def c_find(
    calling_aet: str,
    pacs_aet: str,
    pacs_ip: str,
    pacs_port: int,
    accession: str,
    birth_date: str,
    timeout: int = 60,
) -> tuple[int, str]:
    bin_path = _require(FINDSCU, "findscu")
    return _run(
        findscu_cmd(
            bin_path, calling_aet, pacs_aet, pacs_ip, pacs_port, accession, birth_date
        ),
        timeout,
    )


def c_echo(
    calling_aet: str,
    pacs_aet: str,
    pacs_ip: str,
    pacs_port: int,
    timeout: int = 10,
) -> tuple[int, str]:
    bin_path = _require(ECHOSCU, "echoscu")
    return _run(
        echoscu_cmd(bin_path, calling_aet, pacs_aet, pacs_ip, pacs_port), timeout
    )


def c_find_series_body_part(
    calling_aet: str,
    pacs_aet: str,
    pacs_ip: str,
    pacs_port: int,
    study_uid: str,
    timeout: int = 60,
) -> tuple[int, str]:
    bin_path = _require(FINDSCU, "findscu")
    return _run(
        series_body_part_findscu_cmd(
            bin_path,
            calling_aet,
            pacs_aet,
            pacs_ip,
            pacs_port,
            study_uid,
        ),
        timeout,
    )


def c_move(
    calling_aet: str,
    pacs_aet: str,
    pacs_ip: str,
    pacs_port: int,
    study_uid: str,
    timeout: int,
) -> tuple[int, str]:
    bin_path = _require(MOVESCU, "movescu")
    return _run(
        movescu_cmd(
            bin_path,
            calling_aet,
            pacs_aet,
            pacs_ip,
            pacs_port,
            study_uid,
        ),
        timeout,
    )


def c_find_prior(
    calling_aet: str,
    pacs_aet: str,
    pacs_ip: str,
    pacs_port: int,
    body_part: str,
    modality: str,
    patient_id: str,
    birth_date: str,
    date_range: str,
    timeout: int,
) -> tuple[int, str]:
    bin_path = _require(FINDSCU, "findscu")
    return _run(
        prior_findscu_cmd(
            bin_path,
            calling_aet,
            pacs_aet,
            pacs_ip,
            pacs_port,
            body_part,
            modality,
            patient_id,
            birth_date,
            date_range,
        ),
        timeout,
    )


def c_move_prior_series(
    calling_aet: str,
    pacs_aet: str,
    pacs_ip: str,
    pacs_port: int,
    study_uid: str,
    series_uid: str,
    timeout: int,
) -> tuple[int, str]:
    bin_path = _require(MOVESCU, "movescu")
    return _run(
        prior_series_movescu_cmd(
            bin_path,
            calling_aet,
            pacs_aet,
            pacs_ip,
            pacs_port,
            study_uid,
            series_uid,
        ),
        timeout,
    )


def start_storescp(aet: str, port: int, output_dir: str) -> subprocess.Popen:
    bin_path = _require(STORESCP, "storescp")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    return subprocess.Popen(
        storescp_cmd(bin_path, aet, port, output_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        bufsize=1,
    )


def dcmcjpeg(flag: str, src: str, dest: str, timeout: int = 120) -> tuple[int, str]:
    bin_path = _require(DCMCJPEG, "dcmcjpeg")
    return _run(dcmcjpeg_cmd(bin_path, flag, src, dest), timeout)


def _run(cmd: list[str], timeout: int) -> tuple[int, str]:
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            errors="replace",
        )
        out = (proc.stdout or "") + (proc.stderr or "")
        return proc.returncode, out
    except subprocess.TimeoutExpired as exc:
        stdout = (
            exc.stdout.decode("utf-8", "replace")
            if isinstance(exc.stdout, bytes)
            else (exc.stdout or "")
        )
        stderr = (
            exc.stderr.decode("utf-8", "replace")
            if isinstance(exc.stderr, bytes)
            else (exc.stderr or "")
        )
        out = stdout + stderr
        return 124, out + "\nTIMEOUT"
    except FileNotFoundError as exc:
        raise ToolMissing(str(exc)) from exc


def redact_dicom_output(output: str) -> str:
    """Remove PHI-bearing DCMTK lines before persisting diagnostic output."""
    # PostgreSQL text fields reject NUL bytes. Some non-conformant PACS values can
    # carry DICOM padding through findscu, so remove it before building an event.
    clean_output = (output or "").replace("\x00", "")
    return "\n".join(
        "[REDACTED]" if _SENSITIVE_DICOM_FIELDS.search(line) else line
        for line in clean_output.splitlines()
    )
