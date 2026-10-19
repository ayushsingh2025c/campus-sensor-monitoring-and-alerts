from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.events import ReadingEvent
from app.models import Alert, AlertHistory
from app.worker import persist_reading


def create_rule(client, cleanup_rows, *, operator=">=", threshold=30, enabled=True, **extra):
    response = client.post(
        "/api/v1/rules",
        json={
            "name": "Worker rule test",
            "sensorId": "worker-test-sensor",
            "type": "worker-test-metric",
            "operator": operator,
            "threshold": threshold,
            "enabled": enabled,
            **extra,
        },
    )
    assert response.status_code == 201
    rule_id = response.json()["id"]
    cleanup_rows(rule=rule_id)
    return rule_id


def reading_event(value, sensor_id="worker-test-sensor", metric_type="worker-test-metric"):
    return ReadingEvent(
        eventId=uuid4(),
        sensorId=sensor_id,
        type=metric_type,
        value=value,
        timestamp=datetime.now(UTC),
    )


def test_single_reading_threshold_creates_one_active_alert_and_is_idempotent(
    client, db_session, cleanup_rows
):
    rule_id = create_rule(client, cleanup_rows)
    first_event = reading_event(30)
    cleanup_rows(reading=first_event.event_id)

    assert persist_reading(first_event, db_session) is True
    assert persist_reading(first_event, db_session) is False

    second_event = reading_event(35)
    cleanup_rows(reading=second_event.event_id)
    assert persist_reading(second_event, db_session) is True
    alerts = db_session.scalars(select(Alert).where(Alert.rule_id == rule_id)).all()
    assert len(alerts) == 1
    assert alerts[0].status == "OPEN"
    cleanup_rows(alert=alerts[0].id)
    history = db_session.scalars(
        select(AlertHistory).where(AlertHistory.alert_id == alerts[0].id)
    ).all()
    assert [entry.to_status for entry in history] == ["OPEN"]


@pytest.mark.parametrize(
    ("operator", "threshold", "reading", "expected"),
    [
        (">", 20, 20, False),
        (">", 20, 21, True),
        (">=", 20, 20, True),
        ("<", 20, 20, False),
        ("<", 20, 19, True),
        ("<=", 20, 20, True),
        ("==", 20, 20, True),
    ],
)
def test_single_reading_comparison_operators(
    client, db_session, cleanup_rows, operator, threshold, reading, expected
):
    rule_id = create_rule(client, cleanup_rows, operator=operator, threshold=threshold)
    event = reading_event(reading)
    cleanup_rows(reading=event.event_id)

    persist_reading(event, db_session)

    alert = db_session.scalar(select(Alert).where(Alert.rule_id == rule_id))
    assert (alert is not None) is expected
    if alert:
        cleanup_rows(alert=alert.id)


def test_disabled_rule_does_not_create_alert(client, db_session, cleanup_rows):
    rule_id = create_rule(client, cleanup_rows, enabled=False)
    event = reading_event(100)
    cleanup_rows(reading=event.event_id)

    persist_reading(event, db_session)

    assert db_session.scalar(select(Alert.id).where(Alert.rule_id == rule_id)) is None
