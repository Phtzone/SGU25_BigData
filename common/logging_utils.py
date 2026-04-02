from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

_LOG_RECORD_STANDARD_KEYS = set(logging.makeLogRecord({}).__dict__.keys())


def _is_json_logging_enabled() -> bool:
    value = os.getenv("LOG_JSON", "1").strip().lower()
    return value not in {"0", "false", "no", "off"}


class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "service": getattr(record, "service", record.name),
            "event": getattr(record, "event", record.getMessage()),
        }

        extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _LOG_RECORD_STANDARD_KEYS and not key.startswith("_")
        }
        payload.update({key: value for key, value in extras.items() if value is not None})
        if "message" not in payload:
            payload["message"] = record.getMessage()

        return json.dumps(payload, ensure_ascii=False, default=str)


class KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        parts = [
            f"timestamp={datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()}",
            f"level={record.levelname}",
            f"logger={record.name}",
            f"service={getattr(record, 'service', record.name)}",
            f"event={getattr(record, 'event', record.getMessage())}",
        ]

        extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _LOG_RECORD_STANDARD_KEYS and not key.startswith("_")
        }
        for key, value in extras.items():
            if value is None:
                continue
            parts.append(f"{key}={value}")

        parts.append(f"message={record.getMessage()}")
        return " ".join(parts)


def configure_logging(service: str) -> logging.Logger:
    root_logger = logging.getLogger()
    if not getattr(root_logger, "_sgu25_configured", False):
        handler = logging.StreamHandler(sys.stdout)
        formatter: logging.Formatter
        if _is_json_logging_enabled():
            formatter = StructuredFormatter()
        else:
            formatter = KeyValueFormatter()

        handler.setFormatter(formatter)
        root_logger.handlers.clear()
        root_logger.addHandler(handler)
        root_logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
        root_logger._sgu25_configured = True  # type: ignore[attr-defined]

    logger = logging.getLogger(service)
    logger = logging.LoggerAdapter(logger, {"service": service})  # type: ignore[assignment]
    return logger  # type: ignore[return-value]


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    **fields: Any,
) -> None:
    logger.log(level, event, extra={"event": event, **fields})
