import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import ValidationError
from redis.exceptions import RedisError
from sqlalchemy.orm import Session

from app.db import get_db
from app.events import ReadingEvent
from app.models import Reading
from app.schemas import BatchReadingResponse, BatchReadings, ReadingOutcome
from app.streams import EventIdentityConflict, publish_reading

logger = logging.getLogger("campus.api.readings")
router = APIRouter(prefix="/readings", tags=["readings"])
MAX_BATCH_SIZE = 500


def _error_code(error: dict[str, object]) -> str:
    error_type = str(error.get("type", "invalid_input"))
    if error_type.startswith("datetime_"):
        return "invalid_timestamp"
    if error_type == "uuid_parsing":
        return "invalid_event_id"
    if error_type == "missing":
        return "missing_field"
    if error_type == "extra_forbidden":
        return "unexpected_field"
    if error_type == "value_error" and "timezone" in str(error.get("msg", "")).lower():
        return "invalid_timestamp"
    return error_type.replace(".", "_")


def _validation_outcome(index: int, exc: ValidationError) -> ReadingOutcome:
    error = exc.errors(include_input=False)[0]
    location = ".".join(str(part) for part in error.get("loc", ()))
    message = str(error.get("msg", "Invalid reading"))
    if location:
        message = f"{location}: {message}"
    return ReadingOutcome(
        index=index,
        eventId=None,
        status="rejected",
        error={"code": _error_code(error), "message": message},
    )


def _same_reading(existing: Reading, event: ReadingEvent) -> bool:
    return (
        existing.sensor_id == event.sensor_id
        and existing.metric_type == event.metric_type
        and existing.value == event.value
        and existing.observed_at == event.timestamp
        and existing.metadata_json == event.metadata
    )


def _enqueue(
    event: ReadingEvent,
    index: int,
    db: Session,
    *,
    fail_http: bool,
) -> ReadingOutcome:
    existing = db.get(Reading, event.event_id)
    if existing is not None:
        if _same_reading(existing, event):
            return ReadingOutcome(index=index, eventId=event.event_id, status="duplicate")
        if fail_http:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail={
                "code": "event_id_conflict",
                "message": "eventId has already been used for a different reading",
            })
        return ReadingOutcome(
            index=index,
            eventId=event.event_id,
            status="rejected",
            error={"code": "event_id_conflict", "message": "eventId was used for different reading data"},
        )
    try:
        result = publish_reading(event)
        if result == "duplicate":
            return ReadingOutcome(index=index, eventId=event.event_id, status="duplicate")
    except EventIdentityConflict as exc:
        if fail_http:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": "event_id_conflict", "message": "eventId has already been used for a different reading"},
            ) from exc
        return ReadingOutcome(
            index=index,
            eventId=event.event_id,
            status="rejected",
            error={"code": "event_id_conflict", "message": "eventId was used for different reading data"},
        )
    except RedisError as exc:
        logger.warning("Redis Streams publish failed: %s", type(exc).__name__)
        if fail_http:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"code": "stream_unavailable", "message": "Reading was not accepted because the event stream is unavailable"},
            ) from exc
        return ReadingOutcome(
            index=index,
            eventId=event.event_id,
            status="rejected",
            error={"code": "stream_unavailable", "message": "Reading was not accepted because the event stream is unavailable"},
        )
    return ReadingOutcome(index=index, eventId=event.event_id, status="accepted")


@router.post("", status_code=status.HTTP_202_ACCEPTED)
def ingest_reading(
    event: ReadingEvent,
    response: Response,
    db: Session = Depends(get_db),
) -> dict[str, object]:
    outcome = _enqueue(event, 0, db, fail_http=True)
    if outcome.status == "duplicate":
        response.status_code = status.HTTP_200_OK
        return {"eventId": str(event.event_id), "status": "duplicate"}
    return {"eventId": str(event.event_id), "status": "accepted"}


@router.post("/batch", response_model=BatchReadingResponse, status_code=status.HTTP_207_MULTI_STATUS)
def ingest_batch(batch: BatchReadings, db: Session = Depends(get_db)) -> BatchReadingResponse:
    if len(batch.readings) > MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=413,
            detail={"code": "batch_too_large", "message": f"Batch cannot exceed {MAX_BATCH_SIZE} readings"},
        )

    outcomes: list[ReadingOutcome] = []
    for index, item in enumerate(batch.readings):
        try:
            event = ReadingEvent.model_validate(item)
        except ValidationError as exc:
            outcomes.append(_validation_outcome(index, exc))
            continue
        outcomes.append(_enqueue(event, index, db, fail_http=False))
    return BatchReadingResponse(results=outcomes)
