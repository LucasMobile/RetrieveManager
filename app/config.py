from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv
from sqlalchemy.engine import URL

load_dotenv()


def _env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name, str(default))
    try:
        return max(minimum, int(raw))
    except ValueError as exc:
        raise RuntimeError(f"{name} deve ser um número inteiro") from exc


def available_cpus(cgroup_root: Path = Path("/sys/fs/cgroup")) -> int:
    """CPUs this process may use: affinity, then a container CPU quota."""
    count = os.process_cpu_count() or os.cpu_count() or 1
    try:
        quota, period = (cgroup_root / "cpu.max").read_text().split()[:2]
        if quota != "max":
            # "200000 100000" = 2 CPUs; a fraction still gets one.
            count = min(count, max(1, -(-int(quota) // int(period))))
    except OSError, ValueError:
        pass
    return count


def compact_workers_for(cpus: int) -> int:
    """Codec processes by default: two CPUs stay free for the receiver,
    PostgreSQL and the rest of the worker, at least one process."""
    return max(1, cpus - 2)


def _env_compact_workers() -> int:
    """COMPACT_GLOBAL_WORKERS when set; empty, 0 or "auto" use the CPUs."""
    raw = os.getenv("COMPACT_GLOBAL_WORKERS", "").strip().lower()
    if raw in {"", "0", "auto"}:
        return compact_workers_for(available_cpus())
    return _env_int("COMPACT_GLOBAL_WORKERS", 1)


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

# PostgreSQL is the only supported database. The connection is built from
# its parts, so the password may contain @, :, / or any other character.
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1").strip()
POSTGRES_PORT = _env_int("POSTGRES_PORT", 5432)
POSTGRES_DB = os.getenv("POSTGRES_DB", "retrieve").strip()
POSTGRES_USER = os.getenv("POSTGRES_USER", "retrieve").strip()
POSTGRES_PASSWORD = _read_secret("POSTGRES_PASSWORD", "")
if not POSTGRES_PASSWORD:
    raise RuntimeError("Defina POSTGRES_PASSWORD (ou POSTGRES_PASSWORD_FILE) no .env")
DATABASE_URL = URL.create(
    "postgresql+psycopg",
    username=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    database=POSTGRES_DB,
)

WORKER_INTERVAL_SECONDS = _env_int("WORKER_INTERVAL_SECONDS", 5)
DB_POOL_SIZE = _env_int("DB_POOL_SIZE", 5)
DB_MAX_OVERFLOW = _env_int("DB_MAX_OVERFLOW", 10, minimum=0)
DB_POOL_TIMEOUT_SECONDS = _env_int("DB_POOL_TIMEOUT_SECONDS", 30)
ORDERS_API_POLL_SECONDS = _env_int("ORDERS_API_POLL_SECONDS", 30)
ORDERS_API_TIMEOUT_SECONDS = _env_int("ORDERS_API_TIMEOUT_SECONDS", 60)
ORDERS_API_MAX_RETRIES = _env_int("ORDERS_API_MAX_RETRIES", 3)
ORDERS_API_ACK_BATCH_SIZE = _env_int("ORDERS_API_ACK_BATCH_SIZE", 32)
ORDERS_API_ACK_CONCURRENCY = _env_int("ORDERS_API_ACK_CONCURRENCY", 8)
ORDERS_API_ACK_UNIT_WORKERS = _env_int("ORDERS_API_ACK_UNIT_WORKERS", 4)
ORDERS_API_INGEST_UNIT_WORKERS = _env_int("ORDERS_API_INGEST_UNIT_WORKERS", 4)
FIND_BATCH_SIZE = _env_int("FIND_BATCH_SIZE", 10)
FIND_UNIT_SCHEDULERS = _env_int("FIND_UNIT_SCHEDULERS", 8)
FIND_ORDERS_PER_UNIT = _env_int("FIND_ORDERS_PER_UNIT", 4)
FIND_TIMEOUT_SECONDS = _env_int("FIND_TIMEOUT_SECONDS", 20)
# A C-MOVE whose PACS sends no image (no pending response, no new sub-op) for
# this long is aborted, so a stalled transfer frees its slot instead of holding
# it for the whole unit timeout. 0 disables the check.
MOVE_IDLE_TIMEOUT_SECONDS = _env_int("MOVE_IDLE_TIMEOUT_SECONDS", 120, minimum=0)
# The same check for the historical retrieve: old studies often sit on slower
# PACS storage and pause longer between images.
PRIOR_MOVE_IDLE_TIMEOUT_SECONDS = _env_int(
    "PRIOR_MOVE_IDLE_TIMEOUT_SECONDS", 300, minimum=0
)
# After a C-MOVE the PACS calls successful, how long to wait for the receiver
# to record at least one image of it before treating the images as lost.
MOVE_RECEIVE_CONFIRM_SECONDS = _env_int("MOVE_RECEIVE_CONFIRM_SECONDS", 15, minimum=0)
# Monitoring checks (C-FIND of studies already retrieved) have their own
# threads, so a long monitoring queue never delays the search for new exams.
MONITOR_UNIT_SCHEDULERS = _env_int("MONITOR_UNIT_SCHEDULERS", 4)
MONITOR_CHECKS_PER_UNIT = _env_int("MONITOR_CHECKS_PER_UNIT", 2)
MONITOR_BATCH_SIZE = _env_int("MONITOR_BATCH_SIZE", 25)
COMPACT_BATCH_SIZE = _env_int("COMPACT_BATCH_SIZE", 250)
# One compaction drain keeps refilling its codec slots for this long before
# returning control to the worker scheduler (0 = a single claim per call).
COMPACT_DRAIN_SECONDS = _env_int("COMPACT_DRAIN_SECONDS", 60, minimum=0)
# Codec processes of the whole server, shared fairly by the units compacting
# at the same time (pipeline.compact.FairSlots).
COMPACT_GLOBAL_WORKERS = _env_compact_workers()
COMPACT_DB_BATCH_SIZE = _env_int("COMPACT_DB_BATCH_SIZE", 25)
COMPACT_TEMP_MAX_AGE_SECONDS = _env_int("COMPACT_TEMP_MAX_AGE_SECONDS", 3600)
COMPACT_FILE_TIMEOUT_SECONDS = _env_int("COMPACT_FILE_TIMEOUT_SECONDS", 300)
# Larger objects are sent as received: encoding them would need several
# times their size in memory per codec worker.
COMPACT_MAX_ENCODE_BYTES = _env_int("COMPACT_MAX_ENCODE_BYTES", 256 * 1024 * 1024)
COMPACT_UNIT_SCHEDULERS = _env_int("COMPACT_UNIT_SCHEDULERS", 4)
SEND_UNIT_SCHEDULERS = _env_int("SEND_UNIT_SCHEDULERS", 4)
DASHBOARD_FILE_COUNT_CACHE_SECONDS = _env_int("DASHBOARD_FILE_COUNT_CACHE_SECONDS", 30)
HTTP_TOTAL_TIMEOUT_SECONDS = _env_int("HTTP_TOTAL_TIMEOUT_SECONDS", 120)
HTTP_CONNECT_TIMEOUT_SECONDS = _env_int("HTTP_CONNECT_TIMEOUT_SECONDS", 10)
GLOBAL_RATE_LIMIT_REQUESTS = _env_int("GLOBAL_RATE_LIMIT_REQUESTS", 100)
GLOBAL_RATE_LIMIT_WINDOW_SECONDS = _env_int("GLOBAL_RATE_LIMIT_WINDOW_SECONDS", 60)
LOGIN_RATE_LIMIT_FAILURES = _env_int("LOGIN_RATE_LIMIT_FAILURES", 5)
LOGIN_RATE_LIMIT_WINDOW_SECONDS = _env_int("LOGIN_RATE_LIMIT_WINDOW_SECONDS", 15 * 60)
SEND_DB_BATCH_SIZE = _env_int("SEND_DB_BATCH_SIZE", 50)
SEND_DB_FLUSH_MILLISECONDS = _env_int("SEND_DB_FLUSH_MILLISECONDS", 250)
# One upload drain keeps refilling its HTTP connections for this long before
# returning control to the worker scheduler (0 = a single refill per call).
SEND_DRAIN_SECONDS = _env_int("SEND_DRAIN_SECONDS", 20, minimum=0)
SEND_GLOBAL_CONCURRENCY = _env_int("SEND_GLOBAL_CONCURRENCY", 32)
# Failed uploads are retried with growing waits; after this many attempts the
# transfer stays in send_error until an operator resends it.
SEND_MAX_ATTEMPTS = _env_int("SEND_MAX_ATTEMPTS", 7)
CIRCUIT_BREAKER_FAILURES = _env_int("CIRCUIT_BREAKER_FAILURES", 5)
CIRCUIT_BREAKER_SECONDS = _env_int("CIRCUIT_BREAKER_SECONDS", 60)
_default_health_file = (
    "/tmp/retrieve-worker.ready"
    if IS_PRODUCTION
    else str(DATA_DIR / "retrieve-worker.ready")
)
WORKER_HEALTH_FILE = Path(os.getenv("WORKER_HEALTH_FILE", _default_health_file))

# DICOM receiver (pynetdicom Store SCP), one listener per enabled unit.
RECEIVER_HEALTH_FILE = Path(
    os.getenv(
        "RECEIVER_HEALTH_FILE",
        "/tmp/retrieve-receiver.ready"
        if IS_PRODUCTION
        else str(DATA_DIR / "retrieve-receiver.ready"),
    )
)
# Each association holds one object in memory while it is stored; together with
# the largest expected object this bounds the receiver's peak memory.
RECEIVER_MAX_ASSOCIATIONS = _env_int("RECEIVER_MAX_ASSOCIATIONS", 10)
RECEIVER_MAX_PDU = _env_int("RECEIVER_MAX_PDU", 65534, minimum=4096)
RECEIVER_MIN_FREE_MB = _env_int("RECEIVER_MIN_FREE_MB", 1024, minimum=0)
# fsync each received file before acknowledging it. Off by default: the rename
# is atomic and a process crash keeps the page cache; only a power loss inside
# the flush window can lose a file already acknowledged (the instance is then
# marked missing and can be retrieved again; one acknowledged early whose row
# was not committed yet in that instant leaves no trace).
RECEIVER_FSYNC = _env_bool("RECEIVER_FSYNC", False)
# How long a C-STORE waits for its database commit before answering 0xA700.
RECEIVER_COMMIT_TIMEOUT_SECONDS = _env_int("RECEIVER_COMMIT_TIMEOUT_SECONDS", 30)
# Acknowledge a SOP the unit never recorded as soon as its file is on disk and
# commit its row right after, as storescp did: the PACS no longer waits for the
# database on every image. Resends, revivals and conflicts, a failing writer or
# a long queue still wait for the commit. A file whose row cannot be recorded
# stays in the receive folder and is adopted by the worker.
RECEIVER_EARLY_ACK = _env_bool("RECEIVER_EARLY_ACK", True)
# Objects waiting for their commit above which new ones wait too (backpressure).
RECEIVER_EARLY_ACK_MAX_PENDING = _env_int("RECEIVER_EARLY_ACK_MAX_PENDING", 100)
# Files in the receive folder without a database row (legacy storescp files,
# "reprocessar erros") are adopted after this age, at most once per interval.
RECEIVE_ADOPT_MIN_AGE_SECONDS = _env_int("RECEIVE_ADOPT_MIN_AGE_SECONDS", 60)
RECEIVE_ADOPT_INTERVAL_SECONDS = _env_int("RECEIVE_ADOPT_INTERVAL_SECONDS", 30)
RECEIVE_ADOPT_BATCH_SIZE = _env_int("RECEIVE_ADOPT_BATCH_SIZE", 200)

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
