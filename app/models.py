from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(80), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)


class Settings(Base):
    __tablename__ = "settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cloud_url: Mapped[str] = mapped_column(String(500), nullable=False)
    drop_study_prefix: Mapped[str] = mapped_column(String(32), default="SLRX")
    file_settle_seconds: Mapped[int] = mapped_column(Integer, default=3)


class Unit(Base):
    __tablename__ = "units"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)

    orders_api_url: Mapped[str] = mapped_column(String(500), nullable=False)
    orders_api_token: Mapped[str] = mapped_column(String(2048), nullable=False)
    orders_api_station_id: Mapped[str] = mapped_column(String(64), default="")

    # Mantidos apenas para que bancos SQLite de desenvolvimento já criados
    # possam iniciar; não participam mais do cadastro nem do processamento.
    input_dir: Mapped[str] = mapped_column(String(500), default="")
    sent_dir: Mapped[str] = mapped_column(String(500), default="")

    pacs_aet: Mapped[str] = mapped_column(String(64), nullable=False)
    pacs_ip: Mapped[str] = mapped_column(String(64), nullable=False)
    pacs_port: Mapped[int] = mapped_column(Integer, nullable=False)

    calling_aet: Mapped[str] = mapped_column(String(64), nullable=False)
    store_port: Mapped[int] = mapped_column(Integer, nullable=False)

    receive_dir: Mapped[str] = mapped_column(String(500), nullable=False)
    send_dir: Mapped[str] = mapped_column(String(500), nullable=False)
    error_dir: Mapped[str] = mapped_column(String(500), nullable=False)

    token: Mapped[str] = mapped_column(String(255), default="")

    move_timeout_first: Mapped[int] = mapped_column(Integer, default=600)
    move_timeout_second: Mapped[int] = mapped_column(Integer, default=900)
    retrieve_prior_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    move_timeout_prior: Mapped[int] = mapped_column(Integer, default=1800)
    max_parallel_moves: Mapped[int] = mapped_column(Integer, default=1)
    find_interval_seconds: Mapped[int] = mapped_column(Integer, default=30)
    compact_workers: Mapped[int] = mapped_column(Integer, default=8)
    send_workers: Mapped[int] = mapped_column(Integer, default=16)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now, onupdate=datetime.now
    )

    orders: Mapped[list["Order"]] = relationship(
        back_populates="unit", cascade="all, delete-orphan"
    )


class ModalityRule(Base):
    __tablename__ = "modality_rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    modality: Mapped[str] = mapped_column(String(16), unique=True, nullable=False)
    wait_minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    second_retrieve: Mapped[bool] = mapped_column(Boolean, default=False)
    second_wait_minutes: Mapped[int] = mapped_column(Integer, default=90)


class CompressRule(Base):
    __tablename__ = "compress_rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    modality: Mapped[str] = mapped_column(String(16), unique=True, nullable=False)
    jpeg_flag: Mapped[str] = mapped_column(String(8), nullable=False)


class DropModality(Base):
    __tablename__ = "drop_modalities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(8), unique=True, nullable=False)


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (
        UniqueConstraint("unit_id", "acc", name="uq_order_unit_accession"),
        Index("ix_orders_unit_status_retrieve", "unit_id", "status", "retrieve_at"),
        Index("ix_orders_unit_status_find", "unit_id", "status", "last_find_at"),
        Index(
            "ix_orders_unit_status_second_retrieve",
            "unit_id",
            "status",
            "second_retrieve_at",
        ),
        Index(
            "ix_orders_unit_prior_status_due",
            "unit_id",
            "prior_status",
            "prior_due_at",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    unit_id: Mapped[int] = mapped_column(ForeignKey("units.id"), nullable=False)
    source_id: Mapped[str] = mapped_column(String(64), default="")
    # Compatibilidade física com o SQLite de desenvolvimento anterior.
    filename: Mapped[str] = mapped_column(String(255), default="")
    pat_id: Mapped[str] = mapped_column(String(64), default="")
    acc: Mapped[str] = mapped_column(String(64), nullable=False)
    birth_date: Mapped[str] = mapped_column(String(16), nullable=False)
    exam_date: Mapped[str] = mapped_column(String(16), default="")
    correlation_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    api_read_status: Mapped[str] = mapped_column(String(16), default="pending")
    api_read_attempts: Mapped[int] = mapped_column(Integer, default=0)
    api_read_last_error: Mapped[str] = mapped_column(String(500), default="")
    api_read_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)

    status: Mapped[str] = mapped_column(String(32), default="watching", index=True)
    study_uid: Mapped[str] = mapped_column(String(128), default="")
    modality: Mapped[str] = mapped_column(String(32), default="")
    patient_name: Mapped[str] = mapped_column(String(255), default="")
    body_part: Mapped[str] = mapped_column(String(64), default="")

    prior_status: Mapped[str] = mapped_column(String(32), default="disabled")
    prior_date_from: Mapped[str] = mapped_column(String(8), default="")
    prior_date_to: Mapped[str] = mapped_column(String(8), default="")
    prior_due_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    prior_started_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    prior_completed_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    prior_heartbeat_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    prior_attempts: Mapped[int] = mapped_column(Integer, default=0)
    prior_last_error: Mapped[str] = mapped_column(Text, default="")

    retrieve_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    second_retrieve_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    last_find_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)

    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str] = mapped_column(Text, default="")

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now, onupdate=datetime.now
    )
    found_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    done_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)

    unit: Mapped[Unit] = relationship(back_populates="orders")
    events: Mapped[list["OrderEvent"]] = relationship(
        back_populates="order", cascade="all, delete-orphan", order_by="OrderEvent.id"
    )
    historical_studies: Mapped[list["HistoricalStudy"]] = relationship(
        back_populates="order", cascade="all, delete-orphan"
    )


class OrderEvent(Base):
    __tablename__ = "order_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    level: Mapped[str] = mapped_column(String(16), default="info")
    message: Mapped[str] = mapped_column(String(500), nullable=False)
    detail: Mapped[str] = mapped_column(Text, default="")

    order: Mapped[Order] = relationship(back_populates="events")


class HistoricalStudy(Base):
    __tablename__ = "historical_studies"
    __table_args__ = (
        UniqueConstraint("order_id", "study_uid", name="uq_historical_order_study"),
        Index("ix_historical_study_order", "order_id", "study_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=False)
    unit_id: Mapped[int] = mapped_column(ForeignKey("units.id"), nullable=False)
    study_uid: Mapped[str] = mapped_column(String(128), nullable=False)
    accession: Mapped[str] = mapped_column(String(64), default="")
    study_date: Mapped[str] = mapped_column(String(16), default="")
    modality: Mapped[str] = mapped_column(String(32), default="")
    body_part: Mapped[str] = mapped_column(String(64), default="")
    description: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)

    order: Mapped[Order] = relationship(back_populates="historical_studies")
    image_links: Mapped[list["HistoricalImageLink"]] = relationship(
        back_populates="study", cascade="all, delete-orphan"
    )


class HistoricalImageLink(Base):
    __tablename__ = "historical_image_links"
    __table_args__ = (
        UniqueConstraint(
            "historical_study_id",
            "transfer_id",
            name="uq_historical_study_transfer",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    historical_study_id: Mapped[int] = mapped_column(
        ForeignKey("historical_studies.id"), nullable=False
    )
    transfer_id: Mapped[int] = mapped_column(
        ForeignKey("image_transfers.id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)

    study: Mapped[HistoricalStudy] = relationship(back_populates="image_links")
    transfer: Mapped["ImageTransfer"] = relationship(back_populates="historical_links")


class ImageTransfer(Base):
    """Persistent delivery state for a DICOM file, independent from C-MOVE state."""

    __tablename__ = "image_transfers"
    __table_args__ = (
        UniqueConstraint("unit_id", "filename", name="uq_transfer_unit_filename"),
        Index("ix_transfer_unit_status_retry", "unit_id", "status", "next_attempt_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    unit_id: Mapped[int] = mapped_column(ForeignKey("units.id"), nullable=False)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=True)
    filename: Mapped[str] = mapped_column(String(500), nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    study_uid: Mapped[str] = mapped_column(String(128), default="")
    status: Mapped[str] = mapped_column(String(32), default="received")
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    last_http_status: Mapped[int] = mapped_column(Integer, nullable=True)
    last_error: Mapped[str] = mapped_column(String(500), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now, onupdate=datetime.now
    )
    historical_links: Mapped[list[HistoricalImageLink]] = relationship(
        back_populates="transfer", cascade="all, delete-orphan"
    )


STATUSES = {
    "watching": "Aguardando PACS",
    "wait_retrieve": "Na fila do 1º retrieve",
    "retrieving": "1º retrieve em andamento",
    "wait_second": "Na fila do 2º retrieve",
    "retrieving_second": "2º retrieve em andamento",
    "receiving": "Recebendo imagens",
    "done": "Retrieve concluído",
    "error": "Erro",
    "cancelled": "Cancelado",
}
