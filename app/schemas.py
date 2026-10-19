from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.events import ReadingEvent

RuleOperator = Literal[">", ">=", "<", "<=", "=="]
AlertStatus = Literal["OPEN", "ACKNOWLEDGED", "RESOLVED"]


class BatchReadings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    readings: Annotated[list[object], Field(min_length=1)]


class ErrorDetail(BaseModel):
    code: str
    message: str


class ReadingOutcome(BaseModel):
    index: int
    eventId: UUID | None
    status: Literal["accepted", "duplicate", "rejected"]
    error: ErrorDetail | None = None


class BatchReadingResponse(BaseModel):
    results: list[ReadingOutcome]


class AlertRuleCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    sensorId: str | None = Field(default=None, min_length=1, max_length=128)
    type: str = Field(min_length=1, max_length=64)
    operator: RuleOperator
    threshold: Decimal = Field(max_digits=20, decimal_places=6, allow_inf_nan=False)
    windowSeconds: int | None = Field(default=None, gt=0, le=31_536_000)
    aggregation: Literal["average"] | None = None
    enabled: bool = True

    @model_validator(mode="after")
    def require_aggregation_for_window(self) -> "AlertRuleCreate":
        if self.windowSeconds is not None and self.aggregation != "average":
            raise ValueError("aggregation must be 'average' when windowSeconds is set")
        if self.windowSeconds is None and self.aggregation is not None:
            raise ValueError("aggregation requires windowSeconds")
        return self


class AlertRuleResponse(BaseModel):
    id: UUID
    name: str
    sensorId: str | None
    type: str
    operator: RuleOperator
    threshold: Decimal
    windowSeconds: int | None
    aggregation: str | None
    enabled: bool
    createdAt: datetime
    updatedAt: datetime

    @classmethod
    def from_model(cls, rule: object) -> "AlertRuleResponse":
        return cls(
            id=rule.id,
            name=rule.name,
            sensorId=rule.sensor_id,
            type=rule.metric_type,
            operator=rule.operator,
            threshold=rule.threshold,
            windowSeconds=rule.window_seconds,
            aggregation=rule.aggregation,
            enabled=rule.enabled,
            createdAt=rule.created_at,
            updatedAt=rule.updated_at,
        )


class AlertHistoryResponse(BaseModel):
    fromStatus: AlertStatus | None
    toStatus: AlertStatus
    changedAt: datetime


class AlertResponse(BaseModel):
    id: UUID
    ruleId: UUID
    sensorId: str
    status: AlertStatus
    firstTriggeredAt: datetime
    lastTriggeredAt: datetime
    acknowledgedAt: datetime | None
    resolvedAt: datetime | None
    history: list[AlertHistoryResponse] = Field(default_factory=list)


class ReadingResponse(BaseModel):
    eventId: UUID
    sensorId: str
    type: str
    value: Decimal
    timestamp: datetime
    metadata: dict[str, object] | None
    receivedAt: datetime


def reading_response(reading: object) -> ReadingResponse:
    return ReadingResponse(
        eventId=reading.event_id,
        sensorId=reading.sensor_id,
        type=reading.metric_type,
        value=reading.value,
        timestamp=reading.observed_at,
        metadata=reading.metadata_json,
        receivedAt=reading.received_at,
    )


__all__ = [
    "AlertHistoryResponse",
    "AlertResponse",
    "AlertRuleCreate",
    "AlertRuleResponse",
    "AlertStatus",
    "BatchReadingResponse",
    "BatchReadings",
    "ErrorDetail",
    "ReadingEvent",
    "ReadingOutcome",
    "ReadingResponse",
    "reading_response",
]
