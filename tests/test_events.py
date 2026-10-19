from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.events import ReadingEvent
from app.models import Reading
from app.worker import EventIdentityConflict, persist_reading


def test_reading_event_normalizes_timestamp_and_round_trips_aliases() -> None:
    event = ReadingEvent(
        eventId=uuid4(),
        sensorId="sensor-1",
        type="temperature",
        value="20.5",
        timestamp="2026-10-19T12:00:00+02:00",
        metadata={"floor": 2},
    )

    assert event.timestamp == datetime(2026, 10, 19, 10, 0, tzinfo=UTC)
    assert event.model_dump(mode="json", by_alias=True)["sensorId"] == "sensor-1"


def test_reading_event_requires_timezone_aware_timestamp() -> None:
    with pytest.raises(ValidationError, match="timestamp must include a timezone"):
        ReadingEvent(
            eventId=uuid4(),
            sensorId="sensor-1",
            type="temperature",
            value=20,
            timestamp="2026-10-19T12:00:00",
        )


def test_reading_event_rejects_non_finite_values() -> None:
    with pytest.raises(ValidationError, match="finite number"):
        ReadingEvent(
            eventId=uuid4(),
            sensorId="sensor-1",
            type="temperature",
            value="NaN",
            timestamp="2026-10-19T12:00:00Z",
        )


def test_reading_event_rejects_timestamp_too_far_in_future() -> None:
    with pytest.raises(ValidationError, match="more than 5 minutes in the future"):
        ReadingEvent(
            eventId=uuid4(),
            sensorId="sensor-1",
            type="temperature",
            value=20,
            timestamp="2100-01-01T00:00:00Z",
        )


def test_persist_reading_commits_once_and_accepts_identical_replay() -> None:
    event = ReadingEvent(
        eventId=uuid4(),
        sensorId="sensor-1",
        type="temperature",
        value="20.5",
        timestamp="2026-10-19T12:00:00Z",
        metadata={"floor": 2},
    )
    stored = Reading(
        event_id=event.event_id,
        sensor_id=event.sensor_id,
        metric_type=event.metric_type,
        value=event.value,
        observed_at=event.timestamp,
        metadata_json=event.metadata,
    )

    class SessionStub:
        commits = 0

        def __init__(self, rowcount: int) -> None:
            self.rowcount = rowcount

        def scalar(self, _statement: object) -> object | None:
            return event.event_id if self.rowcount else None

        def scalars(self, _statement: object) -> SimpleNamespace:
            return SimpleNamespace(all=lambda: [])

        def get(self, _model: type[Reading], _event_id: object) -> Reading:
            return stored

        def commit(self) -> None:
            self.commits += 1

    new_session = SessionStub(rowcount=1)
    assert persist_reading(event, new_session) is True
    assert new_session.commits == 1

    replay_session = SessionStub(rowcount=0)
    assert persist_reading(event, replay_session) is False
    assert replay_session.commits == 1


def test_persist_reading_rejects_event_id_reused_with_different_data() -> None:
    event = ReadingEvent(
        eventId=uuid4(),
        sensorId="sensor-1",
        type="temperature",
        value=Decimal("20.5"),
        timestamp="2026-10-19T12:00:00Z",
    )

    class SessionStub:
        commits = 0

        def scalar(self, _statement: object) -> None:
            return None

        def get(self, _model: type[Reading], _event_id: object) -> Reading:
            return Reading(
                event_id=event.event_id,
                sensor_id="different-sensor",
                metric_type=event.metric_type,
                value=event.value,
                observed_at=event.timestamp,
            )

        def commit(self) -> None:
            self.commits += 1

    session = SessionStub()
    with pytest.raises(EventIdentityConflict, match="reused with different content"):
        persist_reading(event, session)
    assert session.commits == 0
