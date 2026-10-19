import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.db import engine
from app.api.readings import router as readings_router
from app.api.rules import router as rules_router
from app.api.alerts import router as alerts_router
from app.logging_config import configure_logging, log_event
from app.streams import get_redis, redis_streams_supported
from redis.exceptions import RedisError

configure_logging()
app = FastAPI(title="Campus Sensor Monitoring", version="0.1.0")
logger = logging.getLogger("campus.api")
app.include_router(readings_router, prefix="/api/v1")
app.include_router(rules_router, prefix="/api/v1")
app.include_router(alerts_router, prefix="/api/v1")


@app.get("/health")
def health() -> JSONResponse:
    checks = {"postgres": "ok", "redis": "ok"}
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        checks["postgres"] = "unavailable"
        log_event(logger, logging.ERROR, "postgres_health_check_failed", error_type=type(exc).__name__)
    try:
        redis_client = get_redis()
        if not redis_streams_supported(redis_client):
            checks["redis"] = "streams_unsupported"
        else:
            redis_client.ping()
    except RedisError as exc:
        checks["redis"] = "unavailable"
        log_event(logger, logging.ERROR, "redis_health_check_failed", error_type=type(exc).__name__)

    ready = all(value == "ok" for value in checks.values())
    return JSONResponse(status_code=200 if ready else 503, content={"status": "ok" if ready else "unavailable", "checks": checks})


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
    errors = [
        {
            "code": error.get("type", "invalid_input"),
            "field": ".".join(str(part) for part in error.get("loc", ())),
            "message": error.get("msg", "Invalid input"),
        }
        for error in exc.errors()
    ]
    return JSONResponse(
        status_code=422,
        content={
            "detail": {
                "code": "validation_error",
                "message": "Request validation failed",
                "errors": errors,
            }
        },
    )
