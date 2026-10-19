from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select

from app.events import ReadingEvent
from app.models import Alert, AlertHistory
from app.worker import persist_reading


def trigger_alert(client, db_session, cleanup_rows):
    rule_response = client.post(
        "/api/v1/rules",
        json={
            "name": "Alert lifecycle test",
            "sensorId": "alert-api-test",
            "type": "alert-api-test",
            "operator": ">",
            "threshold": 10,
        },
    )
    assert rule_response.status_code == 201
    rule_id = rule_response.json()["id"]
    cleanup_rows(rule=rule_id)
    event = ReadingEvent(
        eventId=uuid4(),
        sensorId="alert-api-test",
        type="alert-api-test",
        value=11,
        timestamp=datetime.now(UTC),
    )
    cleanup_rows(reading=event.event_id)
    persist_reading(event, db_session)
    alert = db_session.scalar(select(Alert).where(Alert.rule_id == rule_id))
    assert alert is not None
    cleanup_rows(alert=alert.id)
    return alert


def test_alert_lifecycle_acknowledge_then_resolve(client, db_session, cleanup_rows):
    alert = trigger_alert(client, db_session, cleanup_rows)

    acknowledged = client.post(f"/api/v1/alerts/{alert.id}/acknowledge")
    assert acknowledged.status_code == 200
    assert acknowledged.json()["status"] == "ACKNOWLEDGED"
    assert acknowledged.json()["acknowledgedAt"] is not None

    resolved = client.post(f"/api/v1/alerts/{alert.id}/resolve")
    assert resolved.status_code == 200
    assert resolved.json()["status"] == "RESOLVED"
    assert resolved.json()["resolvedAt"] is not None
    assert [item["toStatus"] for item in resolved.json()["history"]] == [
        "OPEN",
        "ACKNOWLEDGED",
        "RESOLVED",
    ]


def test_alert_lifecycle_rejects_invalid_transitions(client, db_session, cleanup_rows):
    alert = trigger_alert(client, db_session, cleanup_rows)

    assert client.post(f"/api/v1/alerts/{alert.id}/resolve").status_code == 409
    assert client.post(f"/api/v1/alerts/{alert.id}/acknowledge").status_code == 200
    assert client.post(f"/api/v1/alerts/{alert.id}/acknowledge").status_code == 409
    assert client.post(f"/api/v1/alerts/{alert.id}/resolve").status_code == 200
    assert client.post(f"/api/v1/alerts/{alert.id}/resolve").status_code == 409


def test_list_and_get_alert_return_history_and_filters(client, db_session, cleanup_rows):
    alert = trigger_alert(client, db_session, cleanup_rows)
    client.post(f"/api/v1/alerts/{alert.id}/acknowledge")

    response = client.get(
        "/api/v1/alerts",
        params={"status": "ACKNOWLEDGED", "sensorId": alert.sensor_id, "ruleId": str(alert.rule_id)},
    )
    detail = client.get(f"/api/v1/alerts/{alert.id}")

    assert response.status_code == 200
    assert any(item["id"] == str(alert.id) for item in response.json())
    assert detail.status_code == 200
    assert [item["toStatus"] for item in detail.json()["history"]] == ["OPEN", "ACKNOWLEDGED"]


def test_alert_transition_returns_not_found(client):
    missing = uuid4()
    response = client.post(f"/api/v1/alerts/{missing}/acknowledge")
    assert response.status_code == 404
