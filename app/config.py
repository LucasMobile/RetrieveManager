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


def _env_port_map(name: str, default: str = "") -> dict[int, int]:
    """Parse external:internal port mappings used by the store listener."""
    mappings: dict[int, int] = {}
    for item in os.getenv(name, default).split(","):
        item = item.strip()
        if not item:
            continue
        try:
            external_raw, internal_raw = item.split(":", maxsplit=1)
            external, internal = int(external_raw), int(internal_raw)
        except ValueError as exc:
            raise RuntimeError(f"{name} deve usar o formato externa:interna") from exc
        if not 1 <= external <= 65535 or not 1024 <= internal <= 65535:
            raise RuntimeError(
                f"{name}: portas devem estar entre 1-65535; a interna deve ser >= 1024"
            )
        mappings[external] = internal
    return mappings


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
ALLOW_HTTP_FOR_TESTS = _env_bool("ALLOW_HTTP_FOR_TESTS", False)
SESSION_HTTPS_ONLY = _env_bool(
    "SESSION_HTTPS_ONLY", IS_PRODUCTION and not ALLOW_HTTP_FOR_TESTS
)
PUBLIC_ORIGIN = os.getenv("PUBLIC_ORIGIN", "").strip().rstrip("/")
if PUBLIC_ORIGIN:
    parsed_origin = urlparse(PUBLIC_ORIGIN)
    if (
        parsed_origin.scheme not in {"http", "https"}
        or not parsed_origin.hostname
        or parsed_origin.username
        or parsed_origin.password
        or parsed_origin.path
        or parsed_origin.query
        or parsed_origin.fragment
    ):
        raise RuntimeError("PUBLIC_ORIGIN deve conter somente esquema, host e porta")
    try:
        _public_port = parsed_origin.port
        if _public_port == 0:
            raise ValueError("port zero")
    except ValueError as exc:
        raise RuntimeError("PUBLIC_ORIGIN contém uma porta inválida") from exc

if IS_PRODUCTION:
    if not ALLOW_HTTP_FOR_TESTS and not SESSION_HTTPS_ONLY:
        raise RuntimeError("SESSION_HTTPS_ONLY deve ser true em produção")
    if not ALLOW_HTTP_FOR_TESTS and (
        not PUBLIC_ORIGIN or urlparse(PUBLIC_ORIGIN).scheme != "https"
    ):
        raise RuntimeError("PUBLIC_ORIGIN deve ser uma origem HTTPS em produção")
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
ORDERS_API_POLL_SECONDS = _env_int("ORDERS_API_POLL_SECONDS", 30)
ORDERS_API_TIMEOUT_SECONDS = _env_int("ORDERS_API_TIMEOUT_SECONDS", 60)
ORDERS_API_MAX_RETRIES = _env_int("ORDERS_API_MAX_RETRIES", 3)
ORDERS_API_ACK_BATCH_SIZE = _env_int("ORDERS_API_ACK_BATCH_SIZE", 32)
ORDERS_API_ACK_CONCURRENCY = _env_int("ORDERS_API_ACK_CONCURRENCY", 8)
ORDERS_API_ACK_UNIT_WORKERS = _env_int("ORDERS_API_ACK_UNIT_WORKERS", 4)
FIND_BATCH_SIZE = _env_int("FIND_BATCH_SIZE", 10)
FIND_UNIT_SCHEDULERS = _env_int("FIND_UNIT_SCHEDULERS", 4)
COMPACT_BATCH_SIZE = _env_int("COMPACT_BATCH_SIZE", 250)
COMPACT_GLOBAL_WORKERS = _env_int("COMPACT_GLOBAL_WORKERS", 8)
COMPACT_DB_BATCH_SIZE = _env_int("COMPACT_DB_BATCH_SIZE", 25)
COMPACT_TEMP_MAX_AGE_SECONDS = _env_int("COMPACT_TEMP_MAX_AGE_SECONDS", 3600)
DCMCJPEG_TIMEOUT_SECONDS = _env_int("DCMCJPEG_TIMEOUT_SECONDS", 300)
COMPACT_UNIT_SCHEDULERS = _env_int("COMPACT_UNIT_SCHEDULERS", 4)
SEND_UNIT_SCHEDULERS = _env_int("SEND_UNIT_SCHEDULERS", 4)
DASHBOARD_FILE_COUNT_CACHE_SECONDS = _env_int(
    "DASHBOARD_FILE_COUNT_CACHE_SECONDS", 30
)
HTTP_TOTAL_TIMEOUT_SECONDS = _env_int("HTTP_TOTAL_TIMEOUT_SECONDS", 120)
HTTP_CONNECT_TIMEOUT_SECONDS = _env_int("HTTP_CONNECT_TIMEOUT_SECONDS", 10)
GLOBAL_RATE_LIMIT_REQUESTS = _env_int("GLOBAL_RATE_LIMIT_REQUESTS", 100)
GLOBAL_RATE_LIMIT_WINDOW_SECONDS = _env_int("GLOBAL_RATE_LIMIT_WINDOW_SECONDS", 60)
LOGIN_RATE_LIMIT_FAILURES = _env_int("LOGIN_RATE_LIMIT_FAILURES", 5)
LOGIN_RATE_LIMIT_WINDOW_SECONDS = _env_int("LOGIN_RATE_LIMIT_WINDOW_SECONDS", 15 * 60)
SEND_BATCH_SIZE = _env_int("SEND_BATCH_SIZE", 250)
SEND_DB_BATCH_SIZE = _env_int("SEND_DB_BATCH_SIZE", 50)
SEND_RECONCILE_BATCH_SIZE = _env_int("SEND_RECONCILE_BATCH_SIZE", 500)
SEND_RECONCILE_INTERVAL_SECONDS = _env_int(
    "SEND_RECONCILE_INTERVAL_SECONDS", 30
)
SEND_GLOBAL_CONCURRENCY = _env_int("SEND_GLOBAL_CONCURRENCY", 32)
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
ECHOSCU = os.getenv("ECHOSCU", f"{_DCMTK_BIN}/echoscu")
MOVESCU = os.getenv("MOVESCU", f"{_DCMTK_BIN}/movescu")
STORESCP = os.getenv("STORESCP", f"{_DCMTK_BIN}/storescp")
DCMCJPEG = os.getenv("DCMCJPEG", f"{_DCMTK_BIN}/dcmcjpeg")
STORE_PORT_MAP = _env_port_map("STORE_PORT_MAP", "444:10444")


def store_bind_port(external_port: int) -> int:
    return STORE_PORT_MAP.get(external_port, external_port)


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
