import json
import logging
from datetime import UTC, datetime


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


def log_event(logger: logging.Logger, level: int, event: str, **fields: object) -> None:
    logger.log(
        level,
        json.dumps(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "event": event,
                **fields,
            },
            default=str,
            sort_keys=True,
        ),
    )
