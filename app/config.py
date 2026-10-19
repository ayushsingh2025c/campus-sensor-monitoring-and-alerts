from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+psycopg://sensor_app:replace-me@127.0.0.1:5432/sensor_monitoring"
    redis_url: str = "redis://127.0.0.1:6380/0"
    redis_stream: str = "campus:sensor-readings"
    redis_event_index: str = "campus:sensor-event-ids"
    redis_consumer_group: str = "campus-workers"
    redis_consumer_name: str = "worker-1"
    redis_block_ms: int = 1000
    redis_claim_idle_ms: int = 30_000


@lru_cache
def get_settings() -> Settings:
    return Settings()
