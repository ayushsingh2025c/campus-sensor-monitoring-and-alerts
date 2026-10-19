"""Create readings, alert rules, alerts, and alert history.

Revision ID: 0001_initial
Revises:
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "readings",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("sensor_id", sa.String(length=128), nullable=False),
        sa.Column("metric_type", sa.String(length=64), nullable=False),
        sa.Column("value", sa.Numeric(20, 6), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("received_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index(
        "ix_readings_sensor_type_observed",
        "readings",
        ["sensor_id", "metric_type", "observed_at"],
    )
    op.create_index("ix_readings_type_observed", "readings", ["metric_type", "observed_at"])

    op.create_table(
        "alert_rules",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("sensor_id", sa.String(length=128)),
        sa.Column("metric_type", sa.String(length=64), nullable=False),
        sa.Column("operator", sa.String(length=2), nullable=False),
        sa.Column("threshold", sa.Numeric(20, 6), nullable=False),
        sa.Column("window_seconds", sa.Integer()),
        sa.Column("aggregation", sa.String(length=16)),
        sa.Column("enabled", sa.Boolean(), server_default=sa.true(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("operator IN ('>', '>=', '<', '<=', '==')", name="ck_alert_rules_operator"),
        sa.CheckConstraint("window_seconds IS NULL OR window_seconds > 0", name="ck_alert_rules_window_positive"),
        sa.CheckConstraint(
            "(window_seconds IS NULL AND aggregation IS NULL) OR "
            "(window_seconds IS NOT NULL AND aggregation = 'average')",
            name="ck_alert_rules_aggregation",
        ),
    )

    op.create_table(
        "alerts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("rule_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sensor_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=16), server_default="OPEN", nullable=False),
        sa.Column("first_triggered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_triggered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True)),
        sa.Column("resolved_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')", name="ck_alerts_status"),
        sa.ForeignKeyConstraint(["rule_id"], ["alert_rules.id"], ondelete="RESTRICT"),
    )
    op.create_index(
        "uq_active_alert_rule_sensor",
        "alerts",
        ["rule_id", "sensor_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('OPEN', 'ACKNOWLEDGED')"),
    )
    op.create_index("ix_alerts_status_last_triggered", "alerts", ["status", "last_triggered_at"])
    op.create_index("ix_alerts_sensor_status", "alerts", ["sensor_id", "status"])

    op.create_table(
        "alert_history",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("alert_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("from_status", sa.String(length=16)),
        sa.Column("to_status", sa.String(length=16), nullable=False),
        sa.Column("changed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "from_status IS NULL OR from_status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')",
            name="ck_alert_history_from_status",
        ),
        sa.CheckConstraint(
            "to_status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')",
            name="ck_alert_history_to_status",
        ),
        sa.ForeignKeyConstraint(["alert_id"], ["alerts.id"], ondelete="CASCADE"),
    )


def downgrade() -> None:
    op.drop_table("alert_history")
    op.drop_index("ix_alerts_sensor_status", table_name="alerts")
    op.drop_index("ix_alerts_status_last_triggered", table_name="alerts")
    op.drop_index("uq_active_alert_rule_sensor", table_name="alerts")
    op.drop_table("alerts")
    op.drop_table("alert_rules")
    op.drop_index("ix_readings_type_observed", table_name="readings")
    op.drop_index("ix_readings_sensor_type_observed", table_name="readings")
    op.drop_table("readings")
