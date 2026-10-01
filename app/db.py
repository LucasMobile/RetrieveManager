from collections.abc import Generator

from sqlalchemy import create_engine, select, text
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
from app.security import hash_password

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


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _ensure_postgresql_indexes()
    with SessionLocal() as db:
        _seed(db)


def _ensure_postgresql_indexes() -> None:
    """Partial and trigram indexes used by large operational queues."""
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

    db.commit()
