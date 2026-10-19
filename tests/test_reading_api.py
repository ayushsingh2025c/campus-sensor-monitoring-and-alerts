from datetime import UTC, datetime
from uuid import UUID, uuid4

from redis.exceptions import ConnectionError as RedisConnectionError

from app.api import readings as reading_routes
from app.models import Reading


def payload(event_id=None):
    return {
        "eventId": str(event_id or uuid4()),
        "sensorId": "api-test-sensor",
        "type": "temperature",
        "value": 21.5,
        "timestamp": datetime.now(UTC).isoformat(),
        "metadata": {"test": True},
    }


def test_single_reading_is_accepted_only_after_stream_publish(client, monkeypatch, cleanup_rows):
    published = []
    monkeypatch.setattr(reading_routes, "publish_reading", lambda event: published.append(event.event_id) or "1-0")
    event = payload()

    response = client.post("/api/v1/readings", json=event)

    assert response.status_code == 202
    assert response.json() == {"eventId": event["eventId"], "status": "accepted"}
    assert published == [UUID(event["eventId"])]


def test_single_reading_returns_503_when_stream_publish_fails(client, monkeypatch):
    def fail_publish(_event):
        raise RedisConnectionError("unavailable")

    monkeypatch.setattr(reading_routes, "publish_reading", fail_publish)
    response = client.post("/api/v1/readings", json=payload())
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "stream_unavailable"


def test_batch_ingestion_returns_per_item_validation_and_acceptance(client, monkeypatch):
    monkeypatch.setattr(
        reading_routes,
        "publish_readings",
        lambda events: [("accepted", f"{index}-0") for index, _ in enumerate(events)],
    )
    valid = payload()
    invalid = {**payload(), "timestamp": "not-a-timestamp"}

    response = client.post("/api/v1/readings/batch", json={"readings": [valid, invalid]})

    assert response.status_code == 207
    result = response.json()["results"]
    assert result[0]["status"] == "accepted"
    assert result[0]["eventId"] == valid["eventId"]
    assert result[1]["status"] == "rejected"
    assert result[1]["error"]["code"] == "invalid_timestamp"


def test_batch_non_object_item_is_rejected_without_rejecting_batch(client, monkeypatch):
    monkeypatch.setattr(
        reading_routes,
        "publish_readings",
        lambda events: [("accepted", "1-0") for _ in events],
    )
    valid = payload()

    response = client.post("/api/v1/readings/batch", json={"readings": [valid, 42]})

    assert response.status_code == 207
    outcomes = response.json()["results"]
    assert outcomes[0]["status"] == "accepted"
    assert outcomes[1]["status"] == "rejected"


def test_batch_rejects_more_than_500_readings(client):
    response = client.post("/api/v1/readings/batch", json={"readings": [payload() for _ in range(501)]})
    assert response.status_code == 413


def test_duplicate_persisted_event_is_not_republished(client, monkeypatch, db_session, cleanup_rows):
    event = payload()
    event_id = event["eventId"]
    reading = Reading(
        event_id=UUID(event_id),
        sensor_id=event["sensorId"],
        metric_type=event["type"],
        value=event["value"],
        observed_at=datetime.fromisoformat(event["timestamp"]),
        metadata_json=event["metadata"],
    )
    db_session.add(reading)
    db_session.commit()
    assert db_session.get(Reading, UUID(event_id)) is not None
    cleanup_rows(reading=reading.event_id)
    published = []
    monkeypatch.setattr(reading_routes, "publish_reading", lambda value: published.append(value))

    response = client.post("/api/v1/readings", json=event)

    assert response.status_code == 200
    assert response.json()["status"] == "duplicate"
    assert not published
