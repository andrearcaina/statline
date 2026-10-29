import logging
import sys
from typing import Any

import structlog


def order_log_fields(_, __, event_dict: dict[str, Any]) -> dict[str, Any]:
    ordered: dict[str, Any] = {}

    for field in ("timestamp", "level", "event"):
        if field in event_dict:
            ordered[field] = event_dict.pop(field)

    ordered.update(event_dict)
    return ordered


class Logger:
    def __init__(self, name: str = "statline", level: str = "INFO") -> None:
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.processors.add_log_level,
                order_log_fields,
                structlog.processors.JSONRenderer(),
            ],
            logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
            wrapper_class=structlog.make_filtering_bound_logger(
                getattr(logging, level.upper(), logging.INFO)
            ),
            cache_logger_on_first_use=True,
        )
        self._logger = structlog.get_logger(name)

    def info(self, message: str, **fields: Any) -> None:
        self._logger.info(message, **fields)

    def warning(self, message: str, **fields: Any) -> None:
        self._logger.warning(message, **fields)

    def error(self, message: str, **fields: Any) -> None:
        self._logger.error(message, **fields)

    def debug(self, message: str, **fields: Any) -> None:
        self._logger.debug(message, **fields)
