import os
from collections.abc import Generator
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, event, inspect, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from app.config import (
    ADMIN_PASSWORD,
    ADMIN_USER,
    DATABASE_URL,
    DB_MAX_OVERFLOW,
    DB_POOL_SIZE,
    DB_POOL_TIMEOUT_SECONDS,
)
from app.models import (
    Base,
    CompressRule,
    DropModality,
    ModalityRule,
    Order,
    Settings,
    User,
)
from app.security import hash_password

connect_args = {}
if DATABASE_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False}


def _ensure_sqlite_writable(database_url: str) -> None:
    database = make_url(database_url).database
    if not database or database == ":memory:":
        return

    database_path = Path(database)
    try:
        database_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RuntimeError(
            f"não foi possível preparar o diretório do SQLite: {database_path.parent}"
        ) from exc

    directory_writable = os.access(database_path.parent, os.W_OK | os.X_OK)
    file_writable = not database_path.exists() or os.access(database_path, os.W_OK)
    if directory_writable and file_writable:
        return

    effective_uid = getattr(os, "geteuid", lambda: "desconhecido")()
    raise RuntimeError(
        "SQLite sem permissão de escrita. "
        f"diretório={database_path.parent}, "
        f"arquivo={database_path}, "
        f"uid={effective_uid}. "
        "No Docker Compose, execute o serviço data-permissions antes do web e worker."
    )


if DATABASE_URL.startswith("sqlite"):
    _ensure_sqlite_writable(DATABASE_URL)

pool_options = {}
if DATABASE_URL.startswith("postgresql"):
    pool_options = {
        "pool_size": DB_POOL_SIZE,
        "max_overflow": DB_MAX_OVERFLOW,
        "pool_timeout": DB_POOL_TIMEOUT_SECONDS,
    }
engine = create_engine(
    DATABASE_URL, connect_args=connect_args, pool_pre_ping=True, **pool_options
)


if DATABASE_URL.startswith("sqlite"):

    @event.listens_for(engine, "connect")
    def _sqlite_pragmas(dbapi_conn, _connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def get_db() -> Generator[Session]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    _migrate_schema()
    Base.metadata.create_all(bind=engine)
    _ensure_postgresql_indexes()
    _migrate_compression_profiles()
    with SessionLocal() as db:
        _seed(db)


def _add_missing_sqlite_columns(table: str, additions: dict[str, str]) -> set[str]:
    columns = {column["name"] for column in inspect(engine).get_columns(table)}
    missing = [
        (name, definition)
        for name, definition in additions.items()
        if name not in columns
    ]
    if missing:
        with engine.begin() as connection:
            for name, definition in missing:
                connection.execute(
                    text(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
                )
    return columns


def _migrate_schema() -> None:
    """Apply additive deployment migrations and legacy SQLite compatibility.

    This keeps existing installations bootable without introducing a migration
    framework. Future schema changes should use Alembic.
    """
    if DATABASE_URL.startswith(("postgresql://", "postgresql+psycopg://")):
        with engine.begin() as connection:
            # Web and worker may migrate the existing installation together.
            connection.execute(text("SELECT pg_advisory_xact_lock(847263519)"))
            connection.execute(
                text(
                    "ALTER TABLE IF EXISTS units ADD COLUMN IF NOT EXISTS "
                    "orders_api_company_id VARCHAR(64) NOT NULL DEFAULT ''"
                )
            )
            connection.execute(
                text(
                    "ALTER TABLE IF EXISTS units ADD COLUMN IF NOT EXISTS "
                    "pacs_patient_id_wildcard BOOLEAN NOT NULL DEFAULT false"
                )
            )
            connection.execute(
                text(
                    "ALTER TABLE IF EXISTS units ADD COLUMN IF NOT EXISTS "
                    "store_allowed_aets VARCHAR(600) NOT NULL DEFAULT ''"
                )
            )
            connection.execute(
                text(
                    "ALTER TABLE IF EXISTS units ADD COLUMN IF NOT EXISTS "
                    "store_allowed_ips VARCHAR(1200) NOT NULL DEFAULT ''"
                )
            )
            connection.execute(
                text(
                    "ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS "
                    "session_version INTEGER NOT NULL DEFAULT 0"
                )
            )
            connection.execute(
                text(
                    "ALTER TABLE IF EXISTS image_transfers ADD COLUMN IF NOT EXISTS "
                    "sha256 VARCHAR(64) NOT NULL DEFAULT ''"
                )
            )
            # Only delayed the send-folder scan, which no longer exists.
            connection.execute(
                text(
                    "ALTER TABLE IF EXISTS units DROP COLUMN IF EXISTS "
                    "file_settle_seconds"
                )
            )
        return
    if not DATABASE_URL.startswith("sqlite"):
        return
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    if "users" in table_names:
        _add_missing_sqlite_columns(
            "users", {"session_version": "INTEGER NOT NULL DEFAULT 0"}
        )
    if "image_transfers" in table_names:
        _add_missing_sqlite_columns(
            "image_transfers", {"sha256": "VARCHAR(64) NOT NULL DEFAULT ''"}
        )
    if "units" in table_names:
        unit_columns = {column["name"] for column in inspector.get_columns("units")}
        for obsolete in ("dest_aet", "file_settle_seconds"):
            if obsolete in unit_columns:
                with engine.begin() as connection:
                    connection.execute(
                        text(f"ALTER TABLE units DROP COLUMN {obsolete}")
                    )
        unit_additions = {
            "orders_api_url": "VARCHAR(500) NOT NULL DEFAULT ''",
            "orders_api_token": "VARCHAR(2048) NOT NULL DEFAULT ''",
            "orders_api_station_id": "VARCHAR(64) NOT NULL DEFAULT ''",
            "orders_api_company_id": "VARCHAR(64) NOT NULL DEFAULT ''",
            "pacs_patient_id_wildcard": "BOOLEAN NOT NULL DEFAULT 0",
            "store_allowed_aets": "VARCHAR(600) NOT NULL DEFAULT ''",
            "store_allowed_ips": "VARCHAR(1200) NOT NULL DEFAULT ''",
            "retrieve_prior_enabled": "BOOLEAN NOT NULL DEFAULT 0",
            "move_timeout_prior": "INTEGER NOT NULL DEFAULT 1800",
            "deleted_at": "DATETIME",
            "deleted_by_user_id": "INTEGER",
            "deleted_by_username": "VARCHAR(80) NOT NULL DEFAULT ''",
        }
        _add_missing_sqlite_columns("units", unit_additions)
    if "orders" not in table_names:
        return
    order_additions = {
        "correlation_id": "VARCHAR(64) NOT NULL DEFAULT ''",
        "source_id": "VARCHAR(64) NOT NULL DEFAULT ''",
        "api_read_status": "VARCHAR(16) NOT NULL DEFAULT 'confirmed'",
        "api_read_attempts": "INTEGER NOT NULL DEFAULT 0",
        "api_read_last_error": "VARCHAR(500) NOT NULL DEFAULT ''",
        "api_read_at": "DATETIME",
        "body_part": "VARCHAR(64) NOT NULL DEFAULT ''",
        "prior_status": "VARCHAR(32) NOT NULL DEFAULT 'disabled'",
        "prior_date_from": "VARCHAR(8) NOT NULL DEFAULT ''",
        "prior_date_to": "VARCHAR(8) NOT NULL DEFAULT ''",
        "prior_due_at": "DATETIME",
        "prior_started_at": "DATETIME",
        "prior_completed_at": "DATETIME",
        "prior_heartbeat_at": "DATETIME",
        "prior_attempts": "INTEGER NOT NULL DEFAULT 0",
        "prior_last_error": "TEXT NOT NULL DEFAULT ''",
        "archived_at": "DATETIME",
        "archive_reason": "VARCHAR(255) NOT NULL DEFAULT ''",
        "archived_by_user_id": "INTEGER",
        "archived_by_username": "VARCHAR(80) NOT NULL DEFAULT ''",
    }
    columns = _add_missing_sqlite_columns("orders", order_additions)
    with engine.begin() as connection:
        if "source_id" not in columns and "filename" in columns:
            connection.execute(
                text("UPDATE orders SET source_id = filename WHERE source_id = ''")
            )
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_orders_correlation_id "
                "ON orders (correlation_id)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_orders_unit_status_retrieve "
                "ON orders (unit_id, status, retrieve_at)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_orders_unit_status_find "
                "ON orders (unit_id, status, last_find_at)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_orders_unit_status_second_retrieve "
                "ON orders (unit_id, status, second_retrieve_at)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_orders_unit_prior_status_due "
                "ON orders (unit_id, prior_status, prior_due_at)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_orders_active_id "
                "ON orders (archived_at, id)"
            )
        )
        if "order_events" in table_names:
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_order_events_order_id_id "
                    "ON order_events (order_id, id)"
                )
            )
        if "audit_logs" in table_names:
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_audit_logs_resource_action_id "
                    "ON audit_logs (resource_type, action, id)"
                )
            )
        if "image_transfers" in table_names:
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_transfer_order_status "
                    "ON image_transfers (order_id, status)"
                )
            )


# DCMTK flags used before the pydicom JPEG 2000 codec: +e1 was lossless; +eb
# (8 bits) and +ee (12 bits) were lossy, now chosen automatically by BitsStored.
_LEGACY_COMPRESSION_FLAGS = {"+e1": "lossless", "+eb": "lossy", "+ee": "lossy"}
_COMPRESSION_PROFILE_COLUMNS = (
    ("compress_rules", "jpeg_flag"),
    ("unit_compress_rules", "jpeg_flag"),
    ("unit_compression_settings", "default_jpeg_flag"),
)


def _migrate_compression_profiles() -> None:
    with engine.begin() as connection:
        for table, column in _COMPRESSION_PROFILE_COLUMNS:
            for legacy, profile in _LEGACY_COMPRESSION_FLAGS.items():
                connection.execute(
                    text(
                        f"UPDATE {table} SET {column} = :profile "
                        f"WHERE {column} = :legacy"
                    ),
                    {"profile": profile, "legacy": legacy},
                )


def _ensure_postgresql_indexes() -> None:
    """Install PostgreSQL-only indexes used by large operational queues."""
    if not DATABASE_URL.startswith(("postgresql://", "postgresql+psycopg://")):
        return
    statements = (
        "CREATE EXTENSION IF NOT EXISTS pg_trgm",
        "CREATE INDEX IF NOT EXISTS ix_orders_active_id_partial "
        "ON orders (id DESC) WHERE archived_at IS NULL",
        "CREATE INDEX IF NOT EXISTS ix_orders_active_unit_status_id_partial "
        "ON orders (unit_id, status, id DESC) WHERE archived_at IS NULL",
        "CREATE INDEX IF NOT EXISTS ix_orders_active_prior_status_unit_partial "
        "ON orders (prior_status, unit_id) WHERE archived_at IS NULL",
        "CREATE INDEX IF NOT EXISTS ix_orders_unmatched_cleanup_v2_partial "
        "ON orders (created_at, id) WHERE archived_at IS NULL "
        "AND status = 'watching' AND study_uid = '' "
        "AND last_find_at IS NOT NULL AND last_error = ''",
        "CREATE INDEX IF NOT EXISTS ix_orders_completed_retention_partial "
        "ON orders (done_at, id) WHERE archived_at IS NULL "
        "AND status = 'done' AND prior_status IN ('disabled', 'done')",
        "CREATE INDEX IF NOT EXISTS ix_orders_archived_at_id_partial "
        "ON orders (archived_at DESC, id DESC) WHERE archived_at IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS ix_orders_acc_trgm "
        "ON orders USING gin (acc gin_trgm_ops)",
        "CREATE INDEX IF NOT EXISTS ix_orders_pat_id_trgm "
        "ON orders USING gin (pat_id gin_trgm_ops)",
        "CREATE INDEX IF NOT EXISTS ix_orders_source_id_trgm "
        "ON orders USING gin (source_id gin_trgm_ops)",
        "CREATE INDEX IF NOT EXISTS ix_orders_unit_study_uid "
        "ON orders (unit_id, study_uid)",
        "CREATE INDEX IF NOT EXISTS ix_audit_logs_resource_action_id "
        "ON audit_logs (resource_type, action, id DESC)",
        # Compaction claims only received instances.
        "CREATE INDEX IF NOT EXISTS ix_dicom_instance_received_partial "
        "ON dicom_instances (unit_id, id) WHERE state = 'received'",
        # The upload queue reads only pending rows; uploaded history keeps growing.
        "CREATE INDEX IF NOT EXISTS ix_transfer_pending_unit_id_partial "
        "ON image_transfers (unit_id, id) "
        "WHERE status IN ('compressed', 'upload_error')",
    )
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def _seed(db: Session) -> None:
    if DATABASE_URL.startswith(("postgresql://", "postgresql+psycopg://")):
        # Web e worker podem iniciar juntos. Serializa seed/backfill no PostgreSQL.
        db.execute(text("SELECT pg_advisory_xact_lock(847263519)"))
    if db.scalar(select(User).limit(1)) is None:
        db.add(
            User(
                username=ADMIN_USER,
                password_hash=hash_password(ADMIN_PASSWORD),
                role="admin",
            )
        )

    if db.scalar(select(Settings).limit(1)) is None:
        db.add(
            Settings(
                id=1,
                drop_study_prefix="SLRX",
            )
        )

    if db.scalar(select(ModalityRule).limit(1)) is None:
        db.add_all(
            [
                ModalityRule(
                    modality="CT",
                    wait_minutes=15,
                    second_retrieve=True,
                    second_wait_minutes=90,
                ),
                ModalityRule(
                    modality="MR",
                    wait_minutes=15,
                    second_retrieve=True,
                    second_wait_minutes=90,
                ),
                ModalityRule(
                    modality="*",
                    wait_minutes=10,
                    second_retrieve=False,
                    second_wait_minutes=90,
                ),
            ]
        )

    if db.scalar(select(CompressRule).limit(1)) is None:
        db.add_all(
            [
                CompressRule(modality="CR", jpeg_flag="lossy"),
                CompressRule(modality="CT", jpeg_flag="lossless"),
                CompressRule(modality="DX", jpeg_flag="lossy"),
                CompressRule(modality="MG", jpeg_flag="lossy"),
                CompressRule(modality="MR", jpeg_flag="lossless"),
                CompressRule(modality="OT", jpeg_flag="lossy"),
                CompressRule(modality="SC", jpeg_flag="lossless"),
                CompressRule(modality="US", jpeg_flag="lossy"),
                CompressRule(modality="XA", jpeg_flag="lossy"),
                CompressRule(modality="*", jpeg_flag="lossless"),
            ]
        )

    if db.scalar(select(DropModality).limit(1)) is None:
        db.add_all([DropModality(code=c) for c in ("PR", "PS", "SG", "SR", "RA", "US")])

    for order in db.scalars(select(Order).where(Order.correlation_id == "")):
        order.correlation_id = str(uuid4())

    db.flush()
    from app.compression import migrate_legacy_unit_compression
    from app.dicom_rules import migrate_legacy_study_rule

    migrate_legacy_study_rule(db)
    migrate_legacy_unit_compression(db)
    db.commit()
