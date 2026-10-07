import logging
from collections.abc import Generator
from time import perf_counter, sleep
from typing import Any

from sqlalchemy import (
    ColumnElement,
    Connection,
    Engine,
    create_engine,
    func,
    select,
    text,
)
from sqlalchemy.orm import Session, sessionmaker

from app.config import (
    ADMIN_PASSWORD,
    ADMIN_USER,
    DATABASE_URL,
    DB_MAX_OVERFLOW,
    DB_POOL_SIZE,
    DB_POOL_TIMEOUT_SECONDS,
)
from app.dicom_rules import DEFAULT_STUDY_ID_RULE_KEY
from app.models import (
    Base,
    DicomRule,
    DicomRuleCondition,
    ModalityRule,
    User,
)
from app.observability import log_event
from app.security import hash_password

log = logging.getLogger("db")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=DB_POOL_SIZE,
    max_overflow=DB_MAX_OVERFLOW,
    pool_timeout=DB_POOL_TIMEOUT_SECONDS,
)


SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def get_db() -> Generator[Session]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def count_rows(db: Session, model: Any, *where: ColumnElement[bool]) -> int:
    """``SELECT count(*) FROM model WHERE ...`` as a plain int."""
    return int(db.scalar(select(func.count()).select_from(model).where(*where)) or 0)


# NOTIFY channel raised by any change to the units table; the receiver
# listens on it to apply routing changes without waiting for its next poll.
UNITS_CHANGED_CHANNEL = "units_changed"


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    ensure_postgresql_schema(engine)
    ensure_units_changed_trigger(engine)
    with SessionLocal() as db:
        _seed(db)


# Partial, trigram and replacement indexes kept outside the models, as
# (name, "table (columns) [WHERE ...]"). They are built CONCURRENTLY: on a
# database that already holds years of rows the tables keep taking writes.
POSTGRESQL_INDEXES: tuple[tuple[str, str], ...] = (
    ("ix_orders_active_id_partial", "orders (id DESC) WHERE archived_at IS NULL"),
    (
        "ix_orders_active_unit_status_id_partial",
        "orders (unit_id, status, id DESC) WHERE archived_at IS NULL",
    ),
    (
        "ix_orders_active_prior_status_unit_partial",
        "orders (prior_status, unit_id) WHERE archived_at IS NULL",
    ),
    (
        "ix_orders_unmatched_cleanup_v2_partial",
        "orders (created_at, id) WHERE archived_at IS NULL "
        "AND status = 'watching' AND study_uid = '' "
        "AND last_find_at IS NOT NULL AND last_error = ''",
    ),
    (
        "ix_orders_completed_retention_partial",
        "orders (done_at, id) WHERE archived_at IS NULL "
        "AND status = 'done' AND prior_status IN ('disabled', 'done')",
    ),
    ("ix_orders_acc_trgm", "orders USING gin (acc gin_trgm_ops)"),
    ("ix_orders_pat_id_trgm", "orders USING gin (pat_id gin_trgm_ops)"),
    ("ix_orders_source_id_trgm", "orders USING gin (source_id gin_trgm_ops)"),
    ("ix_orders_unit_study_uid", "orders (unit_id, study_uid)"),
    (
        "ix_audit_logs_resource_action_id",
        "audit_logs (resource_type, action, id DESC)",
    ),
    # Compaction claims only received instances.
    (
        "ix_dicom_instance_received_partial",
        "dicom_instances (unit_id, id) WHERE state = 'received'",
    ),
    # Adoption only looks up the files of instances still being processed.
    (
        "ix_dicom_instance_active_path_partial",
        "dicom_instances (unit_id, source_path) "
        "WHERE state IN ('received', 'compacting')",
    ),
    # Recovery of interrupted publications reads the instances of a transfer.
    (
        "ix_dicom_instance_transfer",
        "dicom_instances (transfer_id) WHERE transfer_id IS NOT NULL",
    ),
    # Prior-study linking runs for every compacted file.
    ("ix_historical_study_unit_uid", "historical_studies (unit_id, study_uid)"),
    # The upload queue reads only pending rows; uploaded history keeps growing.
    (
        "ix_transfer_pending_unit_id_partial",
        "image_transfers (unit_id, id) WHERE status IN ('compressed', 'upload_error')",
    ),
)

# Indexes of earlier versions that no query uses, or that another index covers
# (checked with EXPLAIN on ~4 years of synthetic rows). Every update of an
# indexed row rewrites them, so they only cost writes, space and vacuum time.
OBSOLETE_POSTGRESQL_INDEXES: tuple[str, ...] = (
    # Prefix of uq_dicom_instance_content (unit_id, sop_uid, source_sha256).
    "ix_dicom_instance_unit_sop",
    # Replaced by ix_dicom_instance_active_path_partial.
    "ix_dicom_instance_unit_path",
    # correlation_id is written for the logs, never searched.
    "ix_orders_correlation_id",
    "ix_image_transfers_correlation_id",
    "ix_manual_move_requests_correlation_id",
    # Duplicates, or full-table copies of the partial indexes above.
    "ix_orders_active_id",
    "ix_orders_archive_date_id",
    "ix_orders_active_unit_status_id",
    "ix_orders_active_prior_status_unit",
    "ix_orders_completed_retention",
    "ix_orders_archived_at_id_partial",
    # Queues filter the active orders through the partial indexes instead.
    "ix_orders_unit_status_retrieve",
    "ix_orders_unit_status_find",
    "ix_orders_unit_status_monitor_next",
    "ix_orders_unit_prior_status_due",
    # Exact lookups go through uq_order_unit_accession; searches use trigrams.
    "ix_orders_acc",
    "ix_orders_source_id",
)

# Tables that grow for years: the defaults (20% of the table) would let tens of
# millions of dead or new rows pile up between two autovacuum runs.
AUTOVACUUM_SETTINGS: dict[str, dict[str, str]] = {
    table: {
        "autovacuum_vacuum_scale_factor": "0.02",
        "autovacuum_vacuum_insert_scale_factor": "0.02",
        "autovacuum_analyze_scale_factor": "0.01",
    }
    for table in ("dicom_instances", "image_transfers", "orders", "order_events")
}

# Columns that grow with every image. Databases created before they became
# bigint in the models are converted in place (see _widen_ids).
BIGINT_COLUMNS: dict[str, tuple[str, ...]] = {
    "image_transfers": ("id",),
    "dicom_instances": ("id", "transfer_id"),
    "historical_image_links": ("id", "transfer_id"),
    "dicom_rule_applications": ("id", "transfer_id"),
}

# How long a column type change waits for its exclusive table lock before the
# service gives up (and is restarted), instead of queueing every other query on
# the table behind it. Concurrent index builds block no one and wait freely.
SCHEMA_LOCK_TIMEOUT = "60s"

_SCHEMA_MAINTENANCE_LOCK = 847263521


def ensure_postgresql_schema(bind: Engine) -> None:
    """Bring an existing database to the current ids, indexes and autovacuum.

    Idempotent and run by every service at startup, one at a time: only what
    differs is changed, so later startups only read the catalog.
    """
    with bind.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        # A blocking pg_advisory_lock() would keep its statement (and snapshot)
        # open while waiting, and CREATE INDEX CONCURRENTLY waits for older
        # snapshots: the services would deadlock. Poll a try-lock instead.
        while not connection.scalar(
            text("SELECT pg_try_advisory_lock(:key)"),
            {"key": _SCHEMA_MAINTENANCE_LOCK},
        ):
            sleep(0.5)
        try:
            _widen_ids(connection)
            _ensure_indexes(connection)
            _ensure_autovacuum(connection)
        finally:
            connection.execute(
                text("SELECT pg_advisory_unlock(:key)"),
                {"key": _SCHEMA_MAINTENANCE_LOCK},
            )


def _widen_ids(connection: Connection) -> None:
    """Convert the per-image integer ids (and their sequences) to bigint.

    ALTER COLUMN TYPE rewrites the table and its indexes under an exclusive
    lock, so each table is rewritten once, with all its columns together; the
    earlier it runs, the smaller the table.
    """
    for table, columns in BIGINT_COLUMNS.items():
        narrow = list(
            connection.scalars(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = current_schema() AND table_name = :table "
                    "AND column_name = ANY(:columns) AND data_type = 'integer' "
                    "ORDER BY ordinal_position"
                ),
                {"table": table, "columns": list(columns)},
            )
        )
        if narrow:
            started = perf_counter()
            log_event(
                log,
                logging.INFO,
                "db.schema.bigint",
                resource=f"table:{table}",
                status="started",
                columns=narrow,
            )
            changes = ", ".join(
                f"ALTER COLUMN {column} TYPE bigint" for column in narrow
            )
            connection.execute(text(f"SET lock_timeout = '{SCHEMA_LOCK_TIMEOUT}'"))
            try:
                connection.execute(text(f"ALTER TABLE {table} {changes}"))
            finally:
                connection.execute(text("RESET lock_timeout"))
            log_event(
                log,
                logging.INFO,
                "db.schema.bigint",
                resource=f"table:{table}",
                status="success",
                started_at=started,
                columns=narrow,
            )
        sequence = connection.scalar(
            text("SELECT pg_get_serial_sequence(:table, 'id')"), {"table": table}
        )
        if sequence and (
            connection.scalar(
                text(
                    "SELECT seqtypid::regtype::text FROM pg_sequence "
                    "WHERE seqrelid = to_regclass(:sequence)"
                ),
                {"sequence": sequence},
            )
            == "integer"
        ):
            # Its maximum follows the type, from 2^31 - 1 to 2^63 - 1.
            connection.execute(text(f"ALTER SEQUENCE {sequence} AS bigint"))


def _ensure_indexes(connection: Connection) -> None:
    """Create the indexes above, rebuild INVALID ones, drop obsolete ones.

    Builds and drops are CONCURRENT, so they cannot run in a transaction; an
    index left INVALID by an interrupted build is dropped and built again.
    """
    connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    for name, definition in POSTGRESQL_INDEXES:
        valid = connection.scalar(
            text(
                "SELECT i.indisvalid FROM pg_index i "
                "JOIN pg_class c ON c.oid = i.indexrelid "
                "WHERE c.relname = :name "
                "AND c.relnamespace = current_schema()::regnamespace"
            ),
            {"name": name},
        )
        if valid is False:
            connection.execute(text(f"DROP INDEX CONCURRENTLY IF EXISTS {name}"))
        if valid is not True:
            connection.execute(
                text(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} ON {definition}")
            )
    # Only after their replacements exist.
    for name in OBSOLETE_POSTGRESQL_INDEXES:
        connection.execute(text(f"DROP INDEX CONCURRENTLY IF EXISTS {name}"))


def _ensure_autovacuum(connection: Connection) -> None:
    for table, settings in AUTOVACUUM_SETTINGS.items():
        current = set(
            connection.scalar(
                text(
                    "SELECT coalesce(reloptions, '{}') FROM pg_class "
                    "WHERE oid = to_regclass(:table)"
                ),
                {"table": table},
            )
            or ()
        )
        wanted = {f"{key}={value}" for key, value in settings.items()}
        if not wanted <= current:
            options = ", ".join(f"{key} = {value}" for key, value in settings.items())
            connection.execute(text(f"ALTER TABLE {table} SET ({options})"))


def ensure_units_changed_trigger(bind: Engine) -> None:
    with bind.begin() as connection:
        # Web, worker and receiver run init_db together: concurrent CREATE OR
        # REPLACE of the same function fails with "tuple concurrently updated".
        connection.execute(text("SELECT pg_advisory_xact_lock(847263520)"))
        connection.execute(
            text(
                "CREATE OR REPLACE FUNCTION notify_units_changed() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                f"PERFORM pg_notify('{UNITS_CHANGED_CHANNEL}', ''); "
                "RETURN NULL; END $$"
            )
        )
        connection.execute(
            text(
                "CREATE OR REPLACE TRIGGER units_changed "
                "AFTER INSERT OR UPDATE OR DELETE ON units "
                "FOR EACH STATEMENT EXECUTE FUNCTION notify_units_changed()"
            )
        )


def _seed(db: Session) -> None:
    # Web e worker podem iniciar juntos: serializa o seed.
    db.execute(text("SELECT pg_advisory_xact_lock(847263519)"))
    first_boot = db.scalar(select(User).limit(1)) is None
    if first_boot:
        db.add(
            User(
                username=ADMIN_USER,
                password_hash=hash_password(ADMIN_PASSWORD),
                role="admin",
            )
        )

    if first_boot:
        # Default rule, linked to every new unit; it can be edited or removed.
        rule = DicomRule(
            name="Descartar Study ID SLRX",
            enabled=True,
            priority=10,
            combinator="and",
            action="delete",
            system_key=DEFAULT_STUDY_ID_RULE_KEY,
        )
        rule.conditions.append(
            DicomRuleCondition(
                position=0,
                tag="0020,0010",
                operator="starts_with_digits",
                value="SLRX",
            )
        )
        db.add(rule)

    if db.scalar(select(ModalityRule).limit(1)) is None:
        db.add_all(
            [
                ModalityRule(modality="CT", wait_minutes=10, monitor_enabled=True),
                ModalityRule(modality="MR", wait_minutes=10, monitor_enabled=True),
                ModalityRule(modality="*", wait_minutes=10, monitor_enabled=False),
            ]
        )

    db.commit()
