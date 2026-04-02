import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, TYPE_CHECKING

from common.data_quality import summarize_article_quality
from common.article_schema import normalize_article_record, validate_article_record
from common.hdfs_utils import upload_hdfs_bytes
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
    return parser.parse_args()


def create_consumer(args: argparse.Namespace):
    from kafka import KafkaConsumer

    return KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=args.group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def collect_messages(
    consumer: Any,
    max_messages: int,
    poll_timeout_ms: int,
) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []

    while len(collected) < max_messages:
        remaining = max_messages - len(collected)
        records = consumer.poll(timeout_ms=poll_timeout_ms, max_records=remaining)
        if not records:
            break

        for batch in records.values():
            for message in batch:
                collected.append(message.value)

    return collected


def split_rows_by_validity(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    valid_rows: list[dict[str, str]] = []
    invalid_rows: list[dict[str, Any]] = []

    for row in rows:
        normalized = normalize_article_record(row)
        errors = validate_article_record(normalized)
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
    return str(output_dir / f"news_{collected_at.strftime('%H%M%S')}.jsonl")


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


def create_dead_letter_producer(args: argparse.Namespace):
    if not args.dead_letter_topic:
        return None

    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        acks="all",
        retries=3,
        retry_backoff_ms=1000,
        linger_ms=50,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
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


def publish_dead_letters(
    *,
    producer: Any,
    topic: str,
    group_id: str,
    invalid_rows: list[dict[str, Any]],
) -> int:
    if producer is None or not topic or not invalid_rows:
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

    producer.flush()
    return published_count


def main() -> None:
    from hdfs import InsecureClient

    logger = configure_logging("consumer")
    started_at = time.perf_counter()
    args = parse_args()
    consumer = create_consumer(args)
    client = InsecureClient(args.hdfs_url, user=args.hdfs_user)
    dead_letter_producer = create_dead_letter_producer(args)

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

        consumed_quality = summarize_article_quality(rows)
        rows, invalid_rows = split_rows_by_validity(rows)
        invalid_count = len(invalid_rows)
        if invalid_count:
            dead_letter_count = publish_dead_letters(
                producer=dead_letter_producer,
                topic=args.dead_letter_topic,
                group_id=args.group_id,
                invalid_rows=invalid_rows,
            )
            log_event(
                logger,
                30,
                "invalid_messages_routed_to_dead_letter",
                topic=args.topic,
                group_id=args.group_id,
                dead_letter_topic=args.dead_letter_topic,
                invalid_count=invalid_count,
                dead_letter_count=dead_letter_count,
                status="warning",
            )
        if not rows:
            log_event(
                logger,
                30,
                "no_valid_messages_after_validation",
                topic=args.topic,
                group_id=args.group_id,
                invalid_count=invalid_count,
                status="warning",
                duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
            )
            return

        collected_at = datetime.now(timezone.utc)
        output_path = build_output_path(args.base_path, collected_at)
        write_jsonl_to_hdfs(args=args, client=client, output_path=output_path, rows=rows)
        valid_quality = summarize_article_quality(rows)
        log_event(
            logger,
            20,
            "hdfs_raw_write_completed",
            topic=args.topic,
            group_id=args.group_id,
            row_count=len(rows),
            invalid_count=invalid_count,
            duplicate_count=consumed_quality["duplicate_count"],
            missing_title_count=consumed_quality["missing_title_count"],
            missing_title_rate=consumed_quality["missing_title_rate"],
            missing_link_count=consumed_quality["missing_link_count"],
            missing_link_rate=consumed_quality["missing_link_rate"],
            articles_by_source=valid_quality["articles_by_source"],
            output_path=output_path,
            duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
            status="success",
        )
    finally:
        consumer.close()
        if dead_letter_producer is not None:
            dead_letter_producer.close()


if __name__ == "__main__":
    main()
