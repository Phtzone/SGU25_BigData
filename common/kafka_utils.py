from __future__ import annotations

import os
import time
from typing import Any, Callable, TypeVar

from common.logging_utils import log_event

KafkaClient = TypeVar("KafkaClient")


def _resolve_timeout_seconds(timeout_seconds: float | None) -> float:
    if timeout_seconds is not None:
        return max(float(timeout_seconds), 0.0)
    return max(float(os.getenv("KAFKA_STARTUP_TIMEOUT_SECONDS", "60")), 0.0)


def _resolve_check_interval_seconds(check_interval_seconds: float | None) -> float:
    if check_interval_seconds is not None:
        return max(float(check_interval_seconds), 0.0)
    return max(float(os.getenv("KAFKA_STARTUP_CHECK_INTERVAL_SECONDS", "2")), 0.0)


def create_kafka_client_with_retry(
    *,
    client_name: str,
    bootstrap_servers: str,
    factory: Callable[[], KafkaClient],
    logger: Any | None = None,
    timeout_seconds: float | None = None,
    check_interval_seconds: float | None = None,
) -> KafkaClient:
    timeout_seconds = _resolve_timeout_seconds(timeout_seconds)
    check_interval_seconds = _resolve_check_interval_seconds(check_interval_seconds)
    deadline = time.monotonic() + timeout_seconds
    attempt = 0
    last_error: Exception | None = None

    while True:
        attempt += 1
        try:
            client = factory()
            if logger is not None and attempt > 1:
                log_event(
                    logger,
                    20,
                    "kafka_client_ready_after_retry",
                    client_name=client_name,
                    bootstrap_servers=bootstrap_servers,
                    attempt=attempt,
                    status="success",
                )
            return client
        except Exception as exc:  # pragma: no cover - exercised via tests with fake clients
            last_error = exc
            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                break

            sleep_seconds = min(check_interval_seconds, remaining_seconds)
            if logger is not None:
                log_event(
                    logger,
                    30,
                    "kafka_client_waiting_for_broker",
                    client_name=client_name,
                    bootstrap_servers=bootstrap_servers,
                    attempt=attempt,
                    retry_in_seconds=round(sleep_seconds, 2),
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    status="warning",
                )
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    message = (
        f"Timed out after {timeout_seconds} seconds waiting for Kafka client "
        f"'{client_name}' on {bootstrap_servers}."
    )
    raise TimeoutError(message) from last_error
