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


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    _migrate_schema()
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        _seed(db)


def _migrate_schema() -> None:
    """Small forward-only migration for the bundled SQLite deployment.

    This keeps existing installations bootable without introducing a migration
    framework for a single additive column. Future schema changes should use
    Alembic.
    """
    if not DATABASE_URL.startswith("sqlite"):
        return
    inspector = inspect(engine)
    if "orders" not in inspector.get_table_names():
        return
    columns = {column["name"] for column in inspector.get_columns("orders")}
    if "correlation_id" not in columns:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "ALTER TABLE orders ADD COLUMN correlation_id "
                    "VARCHAR(64) NOT NULL DEFAULT ''"
                )
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


def _seed(db: Session) -> None:
    if db.scalar(select(User).limit(1)) is None:
        db.add(User(username=ADMIN_USER, password_hash=hash_password(ADMIN_PASSWORD)))

    if db.scalar(select(Settings).limit(1)) is None:
        from app.config import DEFAULT_CLOUD_URL

        db.add(
            Settings(
                id=1,
                cloud_url=DEFAULT_CLOUD_URL,
                drop_study_prefix="SLRX",
                file_settle_seconds=3,
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

    db.commit()
