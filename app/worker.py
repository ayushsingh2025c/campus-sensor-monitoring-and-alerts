import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from redis import Redis
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import SessionLocal
from app.events import ReadingEvent
from app.logging_config import configure_logging, log_event
from app.models import Alert, AlertHistory, AlertRule, Reading
from app.streams import ensure_consumer_group, get_redis

logger = logging.getLogger("campus.worker")


class EventIdentityConflict(ValueError):
    pass


def _matches(value: Decimal, operator: str, threshold: Decimal) -> bool:
    return {
        ">": value > threshold,
        ">=": value >= threshold,
        "<": value < threshold,
        "<=": value <= threshold,
        "==": value == threshold,
    }[operator]


def evaluate_reading(event: ReadingEvent, session: Session) -> list[UUID]:
    now = datetime.now(UTC)
    rules = session.scalars(
        select(AlertRule).where(
            AlertRule.enabled.is_(True),
            AlertRule.metric_type == event.metric_type,
            (AlertRule.sensor_id == event.sensor_id) | (AlertRule.sensor_id.is_(None)),
        )
    ).all()
    created: list[UUID] = []
    for rule in rules:
        value = event.value
        if rule.window_seconds is not None:
            window_end = event.timestamp
            window_start = window_end - timedelta(seconds=rule.window_seconds)
            value = session.scalar(
                select(func.avg(Reading.value)).where(
                    Reading.sensor_id == event.sensor_id,
                    Reading.metric_type == event.metric_type,
                    Reading.observed_at > window_start,
                    Reading.observed_at <= window_end,
                )
            )
            if value is None:
                continue
        if not _matches(value, rule.operator, rule.threshold):
            continue

        insert_alert = (
            insert(Alert)
            .values(
                rule_id=rule.id,
                sensor_id=event.sensor_id,
                status="OPEN",
                first_triggered_at=now,
                last_triggered_at=now,
            )
            .on_conflict_do_nothing(
                index_elements=[Alert.rule_id, Alert.sensor_id],
                index_where=text("status IN ('OPEN', 'ACKNOWLEDGED')"),
            )
            .returning(Alert.id)
        )
        alert_id = session.scalar(insert_alert)
        if alert_id is not None:
            session.add(
                AlertHistory(
                    alert_id=alert_id,
                    from_status=None,
                    to_status="OPEN",
                    changed_at=now,
                )
            )
            created.append(alert_id)
            continue

        active_alert = session.scalar(
            select(Alert)
            .where(
                Alert.rule_id == rule.id,
                Alert.sensor_id == event.sensor_id,
                Alert.status.in_(("OPEN", "ACKNOWLEDGED")),
            )
            .with_for_update()
        )
        if active_alert is not None:
            active_alert.last_triggered_at = now
    return created


def persist_reading(event: ReadingEvent, session: Session) -> bool:
    statement = (
        insert(Reading)
        .values(
            event_id=event.event_id,
            sensor_id=event.sensor_id,
            metric_type=event.metric_type,
            value=event.value,
            observed_at=event.timestamp,
            metadata_json=event.metadata,
        )
        .on_conflict_do_nothing(index_elements=[Reading.event_id])
        .returning(Reading.event_id)
    )
    inserted_event_id = session.scalar(statement)
    if inserted_event_id is None:
        existing = session.get(Reading, event.event_id)
        if existing is None or (
            existing.sensor_id != event.sensor_id
            or existing.metric_type != event.metric_type
            or existing.value != event.value
            or existing.observed_at != event.timestamp
            or existing.metadata_json != event.metadata
        ):
            raise EventIdentityConflict(f"event ID {event.event_id} was reused with different content")
        session.commit()
        return False
    evaluate_reading(event, session)
    session.commit()
    return True


def process_one(
    client: Redis | None = None,
    session_factory=SessionLocal,
    block_ms: int | None = None,
) -> bool:
    settings = get_settings()
    redis_client = client or get_redis()
    ensure_consumer_group(redis_client)
    pending = redis_client.xpending_range(
        settings.redis_stream,
        settings.redis_consumer_group,
        min="-",
        max="+",
        count=1,
    )
    if pending:
        entry = pending[0]
        idle_time = entry.get("time_since_delivered", 0)
        claimed = (
            redis_client.xclaim(
                settings.redis_stream,
                settings.redis_consumer_group,
                settings.redis_consumer_name,
                min_idle_time=settings.redis_claim_idle_ms,
                message_ids=[entry["message_id"]],
            )
            if idle_time >= settings.redis_claim_idle_ms
            else []
        )
    else:
        claimed = []
    if claimed:
        stream = settings.redis_stream
        message_id, fields = claimed[0]
    else:
        messages = redis_client.xreadgroup(
            settings.redis_consumer_group,
            settings.redis_consumer_name,
            {settings.redis_stream: ">"},
            count=1,
            block=settings.redis_block_ms if block_ms is None else block_ms,
        )
        if not messages:
            return False
        stream, entries = messages[0]
        message_id, fields = entries[0]
    event = ReadingEvent.model_validate_json(fields["payload"])
    with session_factory() as session:
        inserted = persist_reading(event, session)
    redis_client.xack(stream, settings.redis_consumer_group, message_id)
    log_event(
        logger,
        logging.INFO,
        "reading_persisted",
        event_id=event.event_id,
        stream_message_id=message_id,
        inserted=inserted,
    )
    return True


def main() -> None:
    configure_logging()
    client = get_redis()
    ensure_consumer_group(client)
    log_event(logger, logging.INFO, "worker_started", stream=get_settings().redis_stream)
    while True:
        process_one(client)


if __name__ == "__main__":
    main()
