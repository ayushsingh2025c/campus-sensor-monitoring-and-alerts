from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import select

from app.events import ReadingEvent
from app.models import Alert, AlertRule, Reading
from app.worker import persist_reading


def create_window_rule(
    client,
    cleanup_rows,
    *,
    window_seconds=120,
    threshold=30,
    sensor_id="window-test-sensor",
    metric_type="window-test-metric",
):
    response = client.post(
        "/api/v1/rules",
        json={
            "name": "Rolling average test",
            "sensorId": sensor_id,
            "type": metric_type,
            "operator": ">=",
            "threshold": threshold,
            "windowSeconds": window_seconds,
            "aggregation": "average",
        },
    )
    assert response.status_code == 201
    rule_id = response.json()["id"]
    cleanup_rows(rule=rule_id)
    return rule_id


def make_event(value, observed_at):
    return ReadingEvent(
        eventId=uuid4(),
        sensorId="window-test-sensor",
        type="window-test-metric",
        value=value,
        timestamp=observed_at,
    )


def persist_tracked(event, session, cleanup_rows):
    cleanup_rows(reading=event.event_id)
    return persist_reading(event, session)


def test_rolling_average_ignores_readings_from_other_sensor(client, db_session, cleanup_rows):
    rule_id = create_window_rule(client, cleanup_rows, threshold=50)
    other_sensor = ReadingEvent(
        eventId=uuid4(),
        sensorId="different-sensor",
        type="window-test-metric",
        value=100,
        timestamp=datetime.now(UTC),
    )
    cleanup_rows(reading=other_sensor.event_id)

    persist_reading(other_sensor, db_session)

    assert db_session.scalar(select(Alert.id).where(Alert.rule_id == rule_id)) is None


def test_window_rule_uses_average_of_readings_in_rolling_window(
    client, db_session, cleanup_rows
):
    rule_id = create_window_rule(client, cleanup_rows, threshold=30)
    now = datetime.now(UTC)
    first = make_event(20, now - timedelta(seconds=20))
    second = make_event(40, now)

    persist_tracked(first, db_session, cleanup_rows)
    persist_tracked(second, db_session, cleanup_rows)

    alert = db_session.scalar(select(Alert).where(Alert.rule_id == rule_id))
    assert alert is not None
    cleanup_rows(alert=alert.id)


def test_one_second_window_triggers_for_delayed_temperature_reading(
    client, db_session, cleanup_rows
):
    rule_id = create_window_rule(
        client,
        cleanup_rows,
        window_seconds=1,
        threshold=30,
        sensor_id="demo-temp-01",
        metric_type="temperature",
    )
    event = ReadingEvent(
        eventId=uuid4(),
        sensorId="demo-temp-01",
        type="temperature",
        value=35,
        timestamp=datetime.now(UTC) - timedelta(minutes=30),
    )
    cleanup_rows(reading=event.event_id)

    class IsolatedRuleSession:
        def scalars(self, statement):
            return db_session.scalars(statement.where(AlertRule.id == rule_id))

        def __getattr__(self, name):
            return getattr(db_session, name)

    persist_reading(event, IsolatedRuleSession())

    alert = db_session.scalar(select(Alert).where(Alert.rule_id == rule_id))
    assert alert is not None
    assert alert.sensor_id == event.sensor_id
    cleanup_rows(alert=alert.id)


def test_late_out_of_order_reading_inside_window_contributes_to_average(
    client, db_session, cleanup_rows
):
    rule_id = create_window_rule(client, cleanup_rows, window_seconds=120, threshold=50)
    now = datetime.now(UTC)
    earlier = make_event(20, now - timedelta(minutes=4))
    newer = make_event(20, now - timedelta(minutes=2))
    late = make_event(80, now - timedelta(minutes=3))

    persist_tracked(earlier, db_session, cleanup_rows)
    persist_tracked(newer, db_session, cleanup_rows)
    assert db_session.scalar(select(Alert.id).where(Alert.rule_id == rule_id)) is None

    other_rule_id = create_window_rule(
        client,
        cleanup_rows,
        window_seconds=120,
        threshold=60,
    )
    persist_tracked(late, db_session, cleanup_rows)

    assert db_session.scalar(select(Alert.id).where(Alert.rule_id == other_rule_id)) is None
    alerts = db_session.scalars(select(Alert).where(Alert.rule_id == rule_id)).all()
    assert len(alerts) == 1
    for alert in alerts:
        cleanup_rows(alert=alert.id)


def test_readings_older_than_window_do_not_affect_rolling_average(
    client, db_session, cleanup_rows
):
    rule_id = create_window_rule(client, cleanup_rows, window_seconds=30, threshold=50)
    now = datetime.now(UTC)
    old = make_event(100, now - timedelta(minutes=2))
    current = make_event(0, now)

    cleanup_rows(reading=old.event_id)
    db_session.add(
        Reading(
            event_id=old.event_id,
            sensor_id=old.sensor_id,
            metric_type=old.metric_type,
            value=old.value,
            observed_at=old.timestamp,
        )
    )
    db_session.commit()
    persist_tracked(current, db_session, cleanup_rows)

    assert db_session.scalar(select(Alert.id).where(Alert.rule_id == rule_id)) is None
