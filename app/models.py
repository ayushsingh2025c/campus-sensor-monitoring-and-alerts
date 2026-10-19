from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import Uuid


class Base(DeclarativeBase):
    pass


class Reading(Base):
    __tablename__ = "readings"
    __table_args__ = (
        Index("ix_readings_sensor_type_observed", "sensor_id", "metric_type", "observed_at"),
        Index("ix_readings_type_observed", "metric_type", "observed_at"),
    )

    event_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    sensor_id: Mapped[str] = mapped_column(String(128), nullable=False)
    metric_type: Mapped[str] = mapped_column(String(64), nullable=False)
    value: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSON().with_variant(JSONB, "postgresql"))
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )


class AlertRule(Base):
    __tablename__ = "alert_rules"
    __table_args__ = (
        CheckConstraint("operator IN ('>', '>=', '<', '<=', '==')", name="ck_alert_rules_operator"),
        CheckConstraint("window_seconds IS NULL OR window_seconds > 0", name="ck_alert_rules_window_positive"),
        CheckConstraint(
            "(window_seconds IS NULL AND aggregation IS NULL) OR "
            "(window_seconds IS NOT NULL AND aggregation = 'average')",
            name="ck_alert_rules_aggregation",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    sensor_id: Mapped[str | None] = mapped_column(String(128))
    metric_type: Mapped[str] = mapped_column(String(64), nullable=False)
    operator: Mapped[str] = mapped_column(String(2), nullable=False)
    threshold: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    window_seconds: Mapped[int | None] = mapped_column(Integer)
    aggregation: Mapped[str | None] = mapped_column(String(16))
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )


class Alert(Base):
    __tablename__ = "alerts"
    __table_args__ = (
        CheckConstraint("status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')", name="ck_alerts_status"),
        Index(
            "uq_active_alert_rule_sensor",
            "rule_id",
            "sensor_id",
            unique=True,
            postgresql_where=text("status IN ('OPEN', 'ACKNOWLEDGED')"),
        ),
        Index("ix_alerts_status_last_triggered", "status", "last_triggered_at"),
        Index("ix_alerts_sensor_status", "sensor_id", "status"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    rule_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("alert_rules.id", ondelete="RESTRICT"), nullable=False
    )
    sensor_id: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="OPEN")
    first_triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AlertHistory(Base):
    __tablename__ = "alert_history"
    __table_args__ = (
        CheckConstraint(
            "from_status IS NULL OR from_status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')",
            name="ck_alert_history_from_status",
        ),
        CheckConstraint(
            "to_status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')",
            name="ck_alert_history_to_status",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    alert_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("alerts.id", ondelete="CASCADE"), nullable=False
    )
    from_status: Mapped[str | None] = mapped_column(String(16))
    to_status: Mapped[str] = mapped_column(String(16), nullable=False)
    changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
