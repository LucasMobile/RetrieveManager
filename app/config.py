from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()


def _env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name, str(default))
    try:
        return max(minimum, int(raw))
    except ValueError as exc:
        raise RuntimeError(f"{name} deve ser um número inteiro") from exc


def _read_secret(name: str, default: str) -> str:
    """Read a secret directly or from NAME_FILE (Docker/Kubernetes secret mount)."""
    file_path = os.getenv(f"{name}_FILE", "").strip()
    if file_path:
        try:
            return Path(file_path).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise RuntimeError(f"não foi possível ler {name}_FILE") from exc
    return os.getenv(name, default)


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR / "data"))).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

APP_ENV = os.getenv("APP_ENV", "development").strip().lower()
IS_PRODUCTION = APP_ENV == "production"
SERVICE_NAME = os.getenv("SERVICE_NAME", "retrieve-manager").strip()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()

SECRET_KEY = _read_secret("SECRET_KEY", "dev-secret-change-me")
ADMIN_USER = os.getenv("RETRIEVE_ADMIN_USER", "admin").strip()
ADMIN_PASSWORD = _read_secret("RETRIEVE_ADMIN_PASSWORD", "admin")
SESSION_HTTPS_ONLY = _env_bool("SESSION_HTTPS_ONLY", False)

if IS_PRODUCTION:
    if len(SECRET_KEY) < 32 or SECRET_KEY == "dev-secret-change-me":
        raise RuntimeError("SECRET_KEY deve ter pelo menos 32 caracteres em produção")
    admin_password_bytes = len(ADMIN_PASSWORD.encode("utf-8"))
    if ADMIN_PASSWORD == "admin" or not 12 <= admin_password_bytes <= 72:
        raise RuntimeError(
            "RETRIEVE_ADMIN_PASSWORD deve ter entre 12 e 72 bytes em produção"
        )

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"sqlite:///{(DATA_DIR / 'retrieve.db').as_posix()}",
)

WORKER_INTERVAL_SECONDS = _env_int("WORKER_INTERVAL_SECONDS", 5)
FIND_BATCH_SIZE = _env_int("FIND_BATCH_SIZE", 10)
HTTP_TOTAL_TIMEOUT_SECONDS = _env_int("HTTP_TOTAL_TIMEOUT_SECONDS", 120)
HTTP_CONNECT_TIMEOUT_SECONDS = _env_int("HTTP_CONNECT_TIMEOUT_SECONDS", 10)
SEND_BATCH_SIZE = _env_int("SEND_BATCH_SIZE", 250)
SEND_RETRY_BASE_SECONDS = _env_int("SEND_RETRY_BASE_SECONDS", 10)
SEND_RETRY_MAX_SECONDS = _env_int("SEND_RETRY_MAX_SECONDS", 900)
CIRCUIT_BREAKER_FAILURES = _env_int("CIRCUIT_BREAKER_FAILURES", 5)
CIRCUIT_BREAKER_SECONDS = _env_int("CIRCUIT_BREAKER_SECONDS", 60)
_default_health_file = (
    "/tmp/retrieve-worker.ready"
    if IS_PRODUCTION
    else str(DATA_DIR / "retrieve-worker.ready")
)
WORKER_HEALTH_FILE = Path(os.getenv("WORKER_HEALTH_FILE", _default_health_file))

_DCMTK_BIN = "/opt/dcmtk/bin"
FINDSCU = os.getenv("FINDSCU", f"{_DCMTK_BIN}/findscu")
MOVESCU = os.getenv("MOVESCU", f"{_DCMTK_BIN}/movescu")
STORESCP = os.getenv("STORESCP", f"{_DCMTK_BIN}/storescp")
DCMCJPEG = os.getenv("DCMCJPEG", f"{_DCMTK_BIN}/dcmcjpeg")

KNOWN_MODALITIES = ("CR", "DX", "MR", "CT", "US", "SC", "OT", "XA", "MG")
DEFAULT_CLOUD_URL = "https://idr.mobilemed.com.br/api/router/send-image"

_default_roots = (
    f"{BASE_DIR},{DATA_DIR}" if not IS_PRODUCTION else "/mobilemed,/opt/idr"
)
ALLOWED_DATA_ROOTS = tuple(
    Path(item.strip()).resolve()
    for item in os.getenv("ALLOWED_DATA_ROOTS", _default_roots).split(",")
    if item.strip()
)

_default_cloud_host = urlparse(DEFAULT_CLOUD_URL).hostname or ""
CLOUD_ALLOWED_HOSTS = frozenset(
    host.strip().lower()
    for host in os.getenv("CLOUD_ALLOWED_HOSTS", _default_cloud_host).split(",")
    if host.strip()
)
