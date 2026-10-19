from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ReadingEvent(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    event_id: UUID = Field(alias="eventId")
    sensor_id: str = Field(alias="sensorId", min_length=1, max_length=128)
    metric_type: str = Field(alias="type", min_length=1, max_length=64)
    value: Decimal = Field(max_digits=20, decimal_places=6)
    timestamp: datetime
    metadata: dict[str, object] | None = None

    @field_validator("timestamp")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        normalized = value.astimezone(UTC)
        if normalized > datetime.now(UTC) + timedelta(minutes=5):
            raise ValueError("timestamp cannot be more than 5 minutes in the future")
        return normalized

    @field_validator("value")
    @classmethod
    def require_finite_value(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("value must be finite")
        return value
