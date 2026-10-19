from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.events import ReadingEvent
from app.simulator import generate_readings
from app.streams import EventIdentityConflict, publish_reading, redis_streams_supported


def test_simulator_is_reproducible_and_generates_requested_breaches():
    readings = generate_readings(
        6,
        sensor_count=2,
        metric_types=["temperature"],
        breach_every=3,
        seed=7,
    )

    assert [reading["value"] == 100 for reading in readings] == [False, False, True, False, False, True]
    assert [reading["sensorId"] for reading in readings] == [
        "sim-00000",
        "sim-00001",
        "sim-00000",
        "sim-00001",
        "sim-00000",
        "sim-00001",
    ]
    assert all(reading["type"] == "temperature" for reading in readings)


@pytest.mark.parametrize(("version", "supported"), [("3.0.504", False), ("5.0.14", True), ("7.2.4", True)])
def test_stream_health_requires_redis_streams(version, supported):
    class RedisStub:
        def info(self, section):
            assert section == "server"
            return {"redis_version": version}

    assert redis_streams_supported(RedisStub()) is supported


def test_publish_reading_uses_atomic_stream_idempotency_result():
    event = ReadingEvent(
        eventId=uuid4(),
        sensorId="stream-test",
        type="temperature",
        value=20,
        timestamp=datetime.now(UTC),
    )

    class RedisStub:
        result = ["accepted", "1-0"]

        def eval(self, script, key_count, *args):
            assert "XADD" in script
            assert key_count == 2
            assert args[-2] == str(event.event_id)
            return self.result

    client = RedisStub()
    assert publish_reading(event, client) == "1-0"
    client.result = ["duplicate", ""]
    assert publish_reading(event, client) == "duplicate"
    client.result = ["conflict", ""]
    with pytest.raises(EventIdentityConflict, match="already used"):
        publish_reading(event, client)
