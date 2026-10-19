import json
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select

from app.config import get_settings
from app.db import SessionLocal
from app.events import ReadingEvent
from app.models import Alert, Reading
from app.worker import process_one


def test_worker_persists_event_evaluates_rule_and_acks_after_commit(
    client, cleanup_rows
):
    rule_response = client.post(
        "/api/v1/rules",
        json={
            "name": "Worker stream smoke",
            "sensorId": "worker-stream-test",
            "type": "worker-stream-test",
            "operator": ">",
            "threshold": 10,
        },
    )
    assert rule_response.status_code == 201
    rule_id = rule_response.json()["id"]
    cleanup_rows(rule=rule_id)
    event = ReadingEvent(
        eventId=uuid4(),
        sensorId="worker-stream-test",
        type="worker-stream-test",
        value=11,
        timestamp=datetime.now(UTC),
    )
    cleanup_rows(reading=event.event_id)
    settings = get_settings()
    message_id = "100-0"

    class RedisStub:
        acknowledged = False

        def xgroup_create(self, *args, **kwargs):
            return True

        def xpending_range(self, *args, **kwargs):
            return []

        def xreadgroup(self, *args, **kwargs):
            return [
                (
                    settings.redis_stream,
                    [
                        (
                            message_id,
                            {
                                "event_id": str(event.event_id),
                                "payload": json.dumps(event.model_dump(mode="json", by_alias=True)),
                            },
                        )
                    ],
                )
            ]

        def xack(self, stream, group, acknowledged_id):
            assert stream == settings.redis_stream
            assert group == settings.redis_consumer_group
            assert acknowledged_id == message_id
            with SessionLocal() as session:
                assert session.get(Reading, event.event_id) is not None
                assert session.scalar(select(Alert.id).where(Alert.rule_id == rule_id)) is not None
            self.acknowledged = True
            return 1

    redis = RedisStub()
    assert process_one(client=redis) is True
    assert redis.acknowledged is True
    with SessionLocal() as session:
        alerts = session.scalars(select(Alert).where(Alert.rule_id == rule_id)).all()
        assert len(alerts) == 1
        cleanup_rows(alert=alerts[0].id)


def test_worker_reclaims_pending_message_after_idle_timeout(cleanup_rows):
    settings = get_settings()
    event = ReadingEvent(
        eventId=uuid4(),
        sensorId="worker-reclaim-test",
        type="worker-reclaim-test",
        value=1,
        timestamp=datetime.now(UTC),
    )
    cleanup_rows(reading=event.event_id)
    message_id = "200-0"
    fields = {
        "event_id": str(event.event_id),
        "payload": json.dumps(event.model_dump(mode="json", by_alias=True)),
    }

    class RedisStub:
        claimed = False

        def xgroup_create(self, *args, **kwargs):
            return True

        def xpending_range(self, *args, **kwargs):
            return [{"message_id": message_id, "time_since_delivered": 30_000}]

        def xclaim(self, stream, group, consumer, min_idle_time, message_ids):
            assert stream == settings.redis_stream
            assert group == settings.redis_consumer_group
            assert consumer == settings.redis_consumer_name
            assert min_idle_time <= 30_000
            assert message_ids == [message_id]
            self.claimed = True
            return [(message_id, fields)]

        def xreadgroup(self, *args, **kwargs):
            raise AssertionError("new messages should not be read before pending messages")

        def xack(self, _stream, _group, acknowledged_id):
            assert acknowledged_id == message_id
            with SessionLocal() as session:
                assert session.get(Reading, event.event_id) is not None
            return 1

    redis = RedisStub()
    assert process_one(client=redis) is True
    assert redis.claimed is True
