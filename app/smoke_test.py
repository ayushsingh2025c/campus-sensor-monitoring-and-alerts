import time
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import select

from app.db import SessionLocal
from app.events import ReadingEvent
from app.models import Reading
from app.streams import ensure_consumer_group, get_redis, publish_reading


def main() -> None:
    event = ReadingEvent(
        eventId=uuid4(),
        sensorId="foundation-smoke-test",
        type="smoke-test",
        value=Decimal("1"),
        timestamp=datetime.now(UTC),
    )
    redis_client = get_redis()
    ensure_consumer_group(redis_client)
    stream_id = publish_reading(event, redis_client)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        with SessionLocal() as session:
            persisted = session.scalar(select(Reading).where(Reading.event_id == event.event_id))
        if persisted is not None:
            print(f"PASS event_id={event.event_id} stream_id={stream_id} persisted=true")
            return
        time.sleep(0.25)
    raise TimeoutError(f"Worker did not persist event {event.event_id} within 15 seconds")


if __name__ == "__main__":
    main()
