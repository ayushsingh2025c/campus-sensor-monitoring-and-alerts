import json
from functools import lru_cache

from redis import Redis
from redis.exceptions import RedisError, ResponseError

from app.config import get_settings
from app.events import ReadingEvent


class EventIdentityConflict(ValueError):
    pass


@lru_cache
def get_redis() -> Redis:
    return Redis.from_url(
        get_settings().redis_url,
        decode_responses=True,
        protocol=2,
        socket_connect_timeout=2,
        health_check_interval=30,
    )


def redis_streams_supported(client: Redis | None = None) -> bool:
    info = (client or get_redis()).info("server")
    version = str(info.get("redis_version", "0.0.0"))
    parsed = [int(part) for part in version.split(".")[:3] if part.isdigit()]
    parts = tuple((parsed + [0, 0, 0])[:3])
    return parts >= (5, 0, 0)


def ensure_consumer_group(client: Redis | None = None) -> None:
    settings = get_settings()
    redis_client = client or get_redis()
    try:
        redis_client.xgroup_create(settings.redis_stream, settings.redis_consumer_group, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


_PUBLISH_SCRIPT = """
local existing = redis.call('HGET', KEYS[1], ARGV[1])
if existing then
    if existing == ARGV[2] then
        return {'duplicate', ''}
    end
    return {'conflict', ''}
end
local stream_id = redis.call('XADD', KEYS[2], '*', 'event_id', ARGV[1], 'payload', ARGV[2])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return {'accepted', stream_id}
"""


def publish_reading(event: ReadingEvent, client: Redis | None = None) -> str:
    settings = get_settings()
    redis_client = client or get_redis()
    payload = json.dumps(event.model_dump(mode="json", by_alias=True), separators=(",", ":"))
    outcome, message_id = redis_client.eval(
        _PUBLISH_SCRIPT,
        2,
        settings.redis_event_index,
        settings.redis_stream,
        str(event.event_id),
        payload,
    )
    if outcome == "conflict":
        raise EventIdentityConflict(f"event ID {event.event_id} was already used with different content")
    return outcome if outcome == "duplicate" else message_id
