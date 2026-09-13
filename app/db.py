import os
from collections.abc import Generator
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, event, inspect, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from app.config import ADMIN_PASSWORD, ADMIN_USER, DATABASE_URL
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

engine = create_engine(DATABASE_URL, connect_args=connect_args, pool_pre_ping=True)


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
    """Apply small forward-only migrations for the bundled SQLite deployment.

    This keeps existing installations bootable without introducing a migration
    framework. Future schema changes should use Alembic.
    """
    if not DATABASE_URL.startswith("sqlite"):
        return
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    if "units" in table_names:
        unit_columns = {column["name"] for column in inspector.get_columns("units")}
        if "dest_aet" in unit_columns:
            with engine.begin() as connection:
                connection.execute(text("ALTER TABLE units DROP COLUMN dest_aet"))
        unit_additions = {
            "orders_api_url": "VARCHAR(500) NOT NULL DEFAULT ''",
            "orders_api_token": "VARCHAR(2048) NOT NULL DEFAULT ''",
            "orders_api_station_id": "VARCHAR(64) NOT NULL DEFAULT ''",
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
        if "image_transfers" in table_names:
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_transfer_order_status "
                    "ON image_transfers (order_id, status)"
                )
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
        "CREATE INDEX IF NOT EXISTS ix_orders_unmatched_cleanup_partial "
        "ON orders (created_at, id) WHERE archived_at IS NULL "
        "AND status = 'watching' AND study_uid = '' "
        "AND last_find_at IS NOT NULL AND attempts > 0",
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
    )
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def _seed(db: Session) -> None:
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
                CompressRule(modality="CR", jpeg_flag="+eb"),
                CompressRule(modality="CT", jpeg_flag="+e1"),
                CompressRule(modality="DX", jpeg_flag="+eb"),
                CompressRule(modality="MG", jpeg_flag="+eb"),
                CompressRule(modality="MR", jpeg_flag="+e1"),
                CompressRule(modality="OT", jpeg_flag="+ee"),
                CompressRule(modality="SC", jpeg_flag="+e1"),
                CompressRule(modality="US", jpeg_flag="+ee"),
                CompressRule(modality="XA", jpeg_flag="+ee"),
                CompressRule(modality="*", jpeg_flag="+e1"),
            ]
        )

    if db.scalar(select(DropModality).limit(1)) is None:
        db.add_all([DropModality(code=c) for c in ("PR", "PS", "SG", "SR", "RA", "US")])

    for order in db.scalars(select(Order).where(Order.correlation_id == "")):
        order.correlation_id = str(uuid4())

    db.flush()
    from app.dicom_rules import migrate_legacy_study_rule

    migrate_legacy_study_rule(db)
    db.commit()
