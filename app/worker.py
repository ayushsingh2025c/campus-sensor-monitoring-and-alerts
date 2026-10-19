import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from redis import Redis
from sqlalchemy import func, select, text, update, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import SessionLocal
from app.events import ReadingEvent
from app.logging_config import configure_logging, log_event
from app.models import Alert, AlertHistory, AlertRule, Reading
from app.streams import ensure_consumer_group, get_redis

logger = logging.getLogger("campus.worker")
BATCH_SIZE = 100


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


def persist_readings(events: list[ReadingEvent], session: Session) -> list[bool]:
    unique_events: dict[UUID, ReadingEvent] = {}
    for event in events:
        previous = unique_events.get(event.event_id)
        if previous is not None and previous != event:
            raise EventIdentityConflict(f"event ID {event.event_id} was reused with different content")
        unique_events[event.event_id] = event

    candidates = list(unique_events.values())
    if not candidates:
        session.commit()
        return [False] * len(events)

    rules = session.scalars(
        select(AlertRule).where(
            AlertRule.enabled.is_(True),
            AlertRule.metric_type.in_({event.metric_type for event in candidates}),
            (AlertRule.sensor_id.is_(None))
            | AlertRule.sensor_id.in_({event.sensor_id for event in candidates}),
        )
    ).all()
    rules_by_metric: dict[str, list[AlertRule]] = {}
    for rule in rules:
        rules_by_metric.setdefault(rule.metric_type, []).append(rule)

    window_keys = {
        (event.sensor_id, event.metric_type, rule.window_seconds, event.timestamp)
        for event in candidates
        for rule in rules_by_metric.get(event.metric_type, ())
        if rule.window_seconds is not None
        and rule.sensor_id in (None, event.sensor_id)
    }
    window_baselines: dict[tuple[str, str, int, datetime], tuple[Decimal, int]] = {}
    for sensor_id, metric_type, window_seconds, window_end in window_keys:
        window_start = window_end - timedelta(seconds=window_seconds)
        total, count = session.execute(
            select(func.coalesce(func.sum(Reading.value), 0), func.count(Reading.event_id)).where(
                Reading.sensor_id == sensor_id,
                Reading.metric_type == metric_type,
                Reading.observed_at > window_start,
                Reading.observed_at <= window_end,
            )
        ).one()
        window_baselines[(sensor_id, metric_type, window_seconds, window_end)] = (Decimal(total), count)

    statement = (
        insert(Reading)
        .values(
            [
                {
                    "event_id": event.event_id,
                    "sensor_id": event.sensor_id,
                    "metric_type": event.metric_type,
                    "value": event.value,
                    "observed_at": event.timestamp,
                    "metadata_json": event.metadata,
                }
                for event in candidates
            ]
        )
        .on_conflict_do_nothing(index_elements=[Reading.event_id])
        .returning(Reading.event_id)
    )
    inserted_ids = set(session.scalars(statement).all())
    if len(inserted_ids) != len(candidates):
        conflicting_rows = session.scalars(
            select(Reading).where(
                Reading.event_id.in_([event.event_id for event in candidates if event.event_id not in inserted_ids])
            )
        ).all()
        conflicting_by_id = {reading.event_id: reading for reading in conflicting_rows}
        for event in candidates:
            if event.event_id in inserted_ids:
                continue
            reading = conflicting_by_id.get(event.event_id)
            if reading is None or (
                reading.sensor_id != event.sensor_id
                or reading.metric_type != event.metric_type
                or reading.value != event.value
                or reading.observed_at != event.timestamp
                or reading.metadata_json != event.metadata
            ):
                raise EventIdentityConflict(f"event ID {event.event_id} was reused with different content")

    inserted_events = [event for event in candidates if event.event_id in inserted_ids]
    seen: dict[tuple[str, str], list[tuple[datetime, Decimal]]] = {}
    triggered: dict[tuple[UUID, str], tuple[AlertRule, str]] = {}
    now = datetime.now(UTC)
    for event in inserted_events:
        key = (event.sensor_id, event.metric_type)
        prior = seen.setdefault(key, [])
        for rule in rules_by_metric.get(event.metric_type, ()):
            if rule.sensor_id not in (None, event.sensor_id):
                continue
            value = event.value
            if rule.window_seconds is not None:
                window_start = event.timestamp - timedelta(seconds=rule.window_seconds)
                baseline_total, baseline_count = window_baselines[
                    (event.sensor_id, event.metric_type, rule.window_seconds, event.timestamp)
                ]
                prior_values = [
                    prior_value
                    for observed_at, prior_value in prior
                    if window_start < observed_at <= event.timestamp
                ]
                count = baseline_count + len(prior_values) + 1
                total = baseline_total + sum(prior_values, event.value)
                value = total / count
            if _matches(value, rule.operator, rule.threshold):
                triggered[(rule.id, event.sensor_id)] = (rule, event.sensor_id)
        prior.append((event.timestamp, event.value))

    if triggered:
        alert_keys = list(triggered)
        new_alerts = session.execute(
            insert(Alert)
            .values(
                [
                    {
                        "rule_id": rule_id,
                        "sensor_id": sensor_id,
                        "status": "OPEN",
                        "first_triggered_at": now,
                        "last_triggered_at": now,
                    }
                    for rule_id, sensor_id in alert_keys
                ]
            )
            .on_conflict_do_nothing(
                index_elements=[Alert.rule_id, Alert.sensor_id],
                index_where=text("status IN ('OPEN', 'ACKNOWLEDGED')"),
            )
            .returning(Alert.id, Alert.rule_id, Alert.sensor_id)
        ).all()
        inserted_alert_keys = {(row.rule_id, row.sensor_id) for row in new_alerts}
        if new_alerts:
            session.add_all(
                [
                    AlertHistory(
                        alert_id=row.id,
                        from_status=None,
                        to_status="OPEN",
                        changed_at=now,
                    )
                    for row in new_alerts
                ]
            )
        existing_alert_keys = [key for key in alert_keys if key not in inserted_alert_keys]
        if existing_alert_keys:
            session.execute(
                update(Alert)
                .where(
                    tuple_(Alert.rule_id, Alert.sensor_id).in_(existing_alert_keys),
                    Alert.status.in_(("OPEN", "ACKNOWLEDGED")),
                )
                .values(last_triggered_at=now)
            )

    session.commit()
    reported: set[UUID] = set()
    results: list[bool] = []
    for event in events:
        results.append(event.event_id in inserted_ids and event.event_id not in reported)
        reported.add(event.event_id)
    return results


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


def process_batch(
    client: Redis | None = None,
    session_factory=SessionLocal,
    block_ms: int | None = None,
    batch_size: int = BATCH_SIZE,
) -> bool:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    settings = get_settings()
    redis_client = client or get_redis()
    ensure_consumer_group(redis_client)
    pending = redis_client.xpending_range(
        settings.redis_stream,
        settings.redis_consumer_group,
        min="-",
        max="+",
        count=batch_size,
    )
    claimable_ids = [
        entry["message_id"]
        for entry in pending
        if entry.get("time_since_delivered", 0) >= settings.redis_claim_idle_ms
    ]
    claimed = (
        redis_client.xclaim(
            settings.redis_stream,
            settings.redis_consumer_group,
            settings.redis_consumer_name,
            min_idle_time=settings.redis_claim_idle_ms,
            message_ids=claimable_ids,
        )
        if claimable_ids
        else []
    )
    if claimed:
        stream = settings.redis_stream
        messages = claimed
    else:
        messages = redis_client.xreadgroup(
            settings.redis_consumer_group,
            settings.redis_consumer_name,
            {settings.redis_stream: ">"},
            count=batch_size,
            block=settings.redis_block_ms if block_ms is None else block_ms,
        )
        if not messages:
            return False
        stream, messages = messages[0]
    message_ids = [message_id for message_id, _ in messages]
    events = [ReadingEvent.model_validate_json(fields["payload"]) for _, fields in messages]
    with session_factory() as session:
        inserted = persist_readings(events, session)
    redis_client.xack(stream, settings.redis_consumer_group, *message_ids)
    log_event(
        logger,
        logging.INFO,
        "reading_batch_persisted",
        stream=stream,
        first_stream_message_id=message_ids[0],
        last_stream_message_id=message_ids[-1],
        event_count=len(events),
        inserted_count=sum(inserted),
        duplicate_count=len(events) - sum(inserted),
    )
    return True


def process_one(
    client: Redis | None = None,
    session_factory=SessionLocal,
    block_ms: int | None = None,
) -> bool:
    return process_batch(client, session_factory, block_ms, batch_size=1)


def main() -> None:
    configure_logging()
    client = get_redis()
    ensure_consumer_group(client)
    log_event(logger, logging.INFO, "worker_started", stream=get_settings().redis_stream)
    while True:
        process_batch(client)


if __name__ == "__main__":
    main()
