from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.events import ReadingEvent
from app import simulator
from app.simulator import _percentile, _scheduled_times, _submit, generate_readings
from app.streams import EventIdentityConflict, publish_reading, publish_readings, redis_streams_supported


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


def test_simulator_reports_per_item_batch_outcomes_and_request_latency(monkeypatch):
    class Response:
        status = 207

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'{"results":[{"status":"accepted"},{"status":"rejected"}]}'

    ticks = iter([10.0, 10.01, 10.06, 10.061])
    monkeypatch.setattr(simulator.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(simulator, "urlopen", lambda *_args, **_kwargs: Response())

    result = _submit(
        "http://example.test",
        [{"eventId": "one"}, {"eventId": "two"}],
        "batch",
        scheduled_at=0,
    )

    assert result["accepted"] == 1
    assert result["rejected"] == 1
    assert result["http_failures"] == 0
    assert result["failed_readings"] == 0
    assert result["latency_ms"] == pytest.approx(50)


def test_simulator_percentiles_interpolate_between_samples():
    assert _percentile([10, 20, 30, 40], 0.50) == pytest.approx(25)
    assert _percentile([10, 20, 30, 40], 0.95) == pytest.approx(38.5)
    assert _percentile([], 0.95) is None


def test_simulator_burst_schedules_all_batches_immediately():
    assert _scheduled_times(10, 4, 500, 10_000, burst=True) == [10, 10, 10, 10]
    assert _scheduled_times(10, 4, 500, 10_000, burst=False) == [10, 10.05, 10.1, 10.15]


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


def test_publish_readings_batches_atomic_idempotency_outcomes():
    events = [
        ReadingEvent(
            eventId=uuid4(),
            sensorId="stream-batch-test",
            type="temperature",
            value=value,
            timestamp=datetime.now(UTC),
        )
        for value in (20, 21, 22)
    ]

    class RedisStub:
        def eval(self, script, key_count, *_args):
            assert "for index = 1, #ARGV, 2 do" in script
            assert "XADD" in script
            assert key_count == 2
            return [["accepted", "1-0"], ["duplicate", ""], ["conflict", ""]]

    assert publish_readings(events, RedisStub()) == [
        ("accepted", "1-0"),
        ("duplicate", ""),
        ("conflict", ""),
    ]
