import argparse
import base64
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, TYPE_CHECKING

from common.data_quality import summarize_article_quality
from common.article_schema import (
    normalize_article_record,
    normalize_text,
    validate_article_record,
    validate_original_article_record,
)
from common.hdfs_utils import upload_hdfs_bytes
from common.kafka_utils import create_kafka_client_with_retry
from common.logging_utils import configure_logging, log_event

if TYPE_CHECKING:
    from hdfs import InsecureClient
    from kafka import KafkaConsumer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume news messages from Kafka and store them in HDFS."
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "news_raw"))
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"),
    )
    parser.add_argument(
        "--group-id",
        default=os.getenv("KAFKA_CONSUMER_GROUP", "news-raw-to-hdfs-v1"),
    )
    parser.add_argument(
        "--auto-offset-reset",
        choices=("earliest", "latest"),
        default=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        help="Where to start consuming when no committed offset exists for the group.",
    )
    parser.add_argument("--hdfs-url", default=os.getenv("HDFS_URL", "http://localhost:9870"))
    parser.add_argument("--hdfs-user", default=os.getenv("HDFS_USER", "root"))
    parser.add_argument(
        "--base-path",
        default=os.getenv("HDFS_RAW_PATH", os.getenv("HDFS_BASE_PATH", "/news/raw")),
    )
    parser.add_argument(
        "--webhdfs-redirect-host",
        default=os.getenv("WEBHDFS_REDIRECT_HOST", ""),
        help="Override the hostname returned by WebHDFS redirects when running outside Docker.",
    )
    parser.add_argument("--max-messages", type=int, default=100)
    parser.add_argument("--poll-timeout-ms", type=int, default=5000)
    parser.add_argument(
        "--dead-letter-topic",
        default=os.getenv("KAFKA_DEAD_LETTER_TOPIC", "news_dead_letter"),
    )
    parser.add_argument(
        "--write-output-path-file",
        default="",
        help="Optional local file used to persist the exact raw HDFS output path for downstream tasks.",
    )
    return parser.parse_args()


def create_consumer(args: argparse.Namespace, logger: Any | None = None):
    from kafka import KafkaConsumer

    return create_kafka_client_with_retry(
        client_name="consumer",
        bootstrap_servers=args.bootstrap_servers,
        logger=logger,
        factory=lambda: KafkaConsumer(
            args.topic,
            bootstrap_servers=args.bootstrap_servers,
            auto_offset_reset=args.auto_offset_reset,
            enable_auto_commit=False,
            group_id=args.group_id,
        ),
    )


def collect_messages(
    consumer: Any,
    max_messages: int,
    poll_timeout_ms: int,
) -> list[Any]:
    collected: list[Any] = []

    while len(collected) < max_messages:
        remaining = max_messages - len(collected)
        records = consumer.poll(timeout_ms=poll_timeout_ms, max_records=remaining)
        if not records:
            break

        for batch in records.values():
            for message in batch:
                collected.append(message)

    return collected


def serialize_raw_payload(value: Any, *, max_bytes: int = 4096) -> dict[str, Any]:
    if isinstance(value, bytes):
        is_truncated = len(value) > max_bytes
        raw_slice = value[:max_bytes]
        return {
            "encoding": "base64",
            "is_truncated": is_truncated,
            "payload": base64.b64encode(raw_slice).decode("ascii"),
        }

    text_value = str(value)
    text_slice = text_value[:max_bytes]
    return {
        "encoding": "text",
        "is_truncated": len(text_value) > max_bytes,
        "payload": text_slice,
    }


def decode_message_value(message_value: Any) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if isinstance(message_value, dict):
        return message_value, None

    if isinstance(message_value, bytes):
        try:
            raw_text = message_value.decode("utf-8")
        except UnicodeDecodeError as exc:
            return None, {
                "error_type": "utf8_decode_error",
                "error_message": str(exc),
                "raw_payload": serialize_raw_payload(message_value),
            }
    elif isinstance(message_value, str):
        raw_text = message_value
    else:
        return None, {
            "error_type": "unsupported_message_type",
            "error_message": f"Unsupported Kafka message value type: {type(message_value).__name__}",
            "raw_payload": serialize_raw_payload(message_value),
        }

    try:
        parsed_value = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        return None, {
            "error_type": "json_decode_error",
            "error_message": str(exc),
            "raw_payload": serialize_raw_payload(message_value),
        }

    if not isinstance(parsed_value, dict):
        return None, {
            "error_type": "json_payload_not_object",
            "error_message": f"Expected JSON object but got {type(parsed_value).__name__}",
            "raw_payload": serialize_raw_payload(message_value),
        }

    return parsed_value, None


def normalize_message_key(message_key: Any) -> str:
    if message_key is None:
        return ""
    if isinstance(message_key, bytes):
        try:
            return normalize_text(message_key.decode("utf-8"))
        except UnicodeDecodeError:
            return base64.b64encode(message_key).decode("ascii")
    return normalize_text(message_key)


def split_rows_by_deserialization(
    rows: list[Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    parsed_rows: list[dict[str, Any]] = []
    failed_rows: list[dict[str, Any]] = []

    for row in rows:
        parsed_value, decode_error = decode_message_value(row.value)
        if decode_error:
            failed_rows.append(
                {
                    "error_type": decode_error["error_type"],
                    "error_message": decode_error["error_message"],
                    "raw_payload": decode_error["raw_payload"],
                    "partition": getattr(row, "partition", None),
                    "offset": getattr(row, "offset", None),
                    "timestamp": getattr(row, "timestamp", None),
                    "message_key": normalize_message_key(getattr(row, "key", None)),
                }
            )
            continue
        parsed_rows.append(parsed_value)

    return parsed_rows, failed_rows


def split_rows_by_validity(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    valid_rows: list[dict[str, str]] = []
    invalid_rows: list[dict[str, Any]] = []

    for row in rows:
        original_errors = validate_original_article_record(row)
        normalized = normalize_article_record(row)
        normalized_errors = validate_article_record(normalized)
        errors = list(dict.fromkeys(original_errors + normalized_errors))
        if errors:
            invalid_rows.append(
                {
                    "original_payload": row,
                    "normalized_payload": normalized,
                    "errors": errors,
                }
            )
            continue
        valid_rows.append(normalized)

    return valid_rows, invalid_rows


def normalize_rows_for_storage(rows: list[dict[str, Any]]) -> tuple[list[dict[str, str]], int]:
    valid_rows, invalid_rows = split_rows_by_validity(rows)
    return valid_rows, len(invalid_rows)


def build_output_path(base_path: str, collected_at: datetime) -> str:
    output_dir = PurePosixPath(
        base_path,
        collected_at.strftime("%Y"),
        collected_at.strftime("%m"),
        collected_at.strftime("%d"),
    )
    return str(output_dir / f"news_{collected_at.strftime('%H%M%S%f')}.jsonl")


def write_output_path_file(path_file: str, output_path: str) -> None:
    if not path_file.strip():
        return

    path = Path(path_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(output_path + "\n", encoding="utf-8")


def commit_processed_offsets(consumer: Any) -> None:
    consumer.commit()


def write_jsonl_to_hdfs(
    args: argparse.Namespace,
    client: Any,
    output_path: str,
    rows: list[dict[str, Any]],
) -> None:
    directory = str(PurePosixPath(output_path).parent)
    client.makedirs(directory)

    payload = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows).encode("utf-8")
    upload_hdfs_bytes(
        hdfs_url=args.hdfs_url,
        hdfs_user=args.hdfs_user,
        path=output_path,
        data=payload,
        redirect_host=args.webhdfs_redirect_host,
    )


def create_dead_letter_producer(args: argparse.Namespace, logger: Any | None = None):
    if not args.dead_letter_topic:
        return None

    from kafka import KafkaProducer

    return create_kafka_client_with_retry(
        client_name="dead_letter_producer",
        bootstrap_servers=args.bootstrap_servers,
        logger=logger,
        factory=lambda: KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            acks="all",
            retries=3,
            retry_backoff_ms=1000,
            linger_ms=50,
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        ),
    )


def build_dead_letter_message(
    *,
    invalid_row: dict[str, Any],
    topic: str,
    group_id: str,
) -> dict[str, Any]:
    return {
        "reason": "article_validation_failed",
        "source_topic": topic,
        "consumer_group": group_id,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "errors": invalid_row["errors"],
        "original_payload": invalid_row["original_payload"],
        "normalized_payload": invalid_row["normalized_payload"],
    }


def build_deserialization_dead_letter_message(
    *,
    failed_row: dict[str, Any],
    topic: str,
    group_id: str,
) -> dict[str, Any]:
    return {
        "reason": "message_deserialization_failed",
        "source_topic": topic,
        "consumer_group": group_id,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "error_type": failed_row["error_type"],
        "error_message": failed_row["error_message"],
        "raw_payload": failed_row["raw_payload"],
        "message_key": failed_row["message_key"],
        "partition": failed_row["partition"],
        "offset": failed_row["offset"],
        "timestamp": failed_row["timestamp"],
    }


def publish_dead_letters(
    *,
    producer: Any,
    topic: str,
    group_id: str,
    invalid_rows: list[dict[str, Any]],
    deserialization_failures: list[dict[str, Any]] | None = None,
) -> int:
    if producer is None or not topic:
        return 0

    published_count = 0
    for invalid_row in invalid_rows:
        payload = build_dead_letter_message(
            invalid_row=invalid_row,
            topic=topic,
            group_id=group_id,
        )
        key = (
            invalid_row["normalized_payload"].get("link")
            or invalid_row["normalized_payload"].get("ingestion_id")
            or "news_dead_letter"
        )
        producer.send(topic, key=key, value=payload)
        published_count += 1

    for failed_row in deserialization_failures or []:
        payload = build_deserialization_dead_letter_message(
            failed_row=failed_row,
            topic=topic,
            group_id=group_id,
        )
        key = failed_row["message_key"] or f"dead_letter_{failed_row['partition']}_{failed_row['offset']}"
        producer.send(topic, key=key, value=payload)
        published_count += 1

    producer.flush()
    return published_count


def main() -> None:
    from hdfs import InsecureClient

    logger = configure_logging("consumer")
    started_at = time.perf_counter()
    args = parse_args()
    consumer = create_consumer(args, logger=logger)
    client = InsecureClient(args.hdfs_url, user=args.hdfs_user)
    dead_letter_producer = create_dead_letter_producer(args, logger=logger)

    try:
        rows = collect_messages(
            consumer=consumer,
            max_messages=args.max_messages,
            poll_timeout_ms=args.poll_timeout_ms,
        )
        if not rows:
            log_event(
                logger,
                20,
                "kafka_consume_empty",
                topic=args.topic,
                group_id=args.group_id,
                status="success",
                duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
            )
            return

        rows, deserialization_failures = split_rows_by_deserialization(rows)
        deserialization_error_count = len(deserialization_failures)
        consumed_quality = summarize_article_quality(rows)
        rows, invalid_rows = split_rows_by_validity(rows)
        invalid_count = len(invalid_rows)
        if invalid_count or deserialization_error_count:
            dead_letter_count = publish_dead_letters(
                producer=dead_letter_producer,
                topic=args.dead_letter_topic,
                group_id=args.group_id,
                invalid_rows=invalid_rows,
                deserialization_failures=deserialization_failures,
            )
            log_event(
                logger,
                30,
                "invalid_messages_routed_to_dead_letter",
                topic=args.topic,
                group_id=args.group_id,
                dead_letter_topic=args.dead_letter_topic,
                deserialization_error_count=deserialization_error_count,
                invalid_count=invalid_count,
                dead_letter_count=dead_letter_count,
                status="warning",
            )
        if not rows:
            commit_processed_offsets(consumer)
            log_event(
                logger,
                30,
                "no_valid_messages_after_validation",
                topic=args.topic,
                group_id=args.group_id,
                deserialization_error_count=deserialization_error_count,
                invalid_count=invalid_count,
                status="warning",
                offset_commit="success",
                duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
            )
            return

        collected_at = datetime.now(timezone.utc)
        output_path = build_output_path(args.base_path, collected_at)
        write_jsonl_to_hdfs(args=args, client=client, output_path=output_path, rows=rows)
        write_output_path_file(args.write_output_path_file, output_path)
        valid_quality = summarize_article_quality(rows)
        commit_processed_offsets(consumer)
        log_event(
            logger,
            20,
            "hdfs_raw_write_completed",
            topic=args.topic,
            group_id=args.group_id,
            row_count=len(rows),
            deserialization_error_count=deserialization_error_count,
            invalid_count=invalid_count,
            duplicate_count=consumed_quality["duplicate_count"],
            missing_title_count=consumed_quality["missing_title_count"],
            missing_title_rate=consumed_quality["missing_title_rate"],
            missing_link_count=consumed_quality["missing_link_count"],
            missing_link_rate=consumed_quality["missing_link_rate"],
            articles_by_source=valid_quality["articles_by_source"],
            output_path=output_path,
            offset_commit="success",
            duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
            status="success",
        )
    finally:
        consumer.close()
        if dead_letter_producer is not None:
            dead_letter_producer.close()


if __name__ == "__main__":
    main()
