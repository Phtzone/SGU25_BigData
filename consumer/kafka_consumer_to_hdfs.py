import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, TYPE_CHECKING

from common.article_schema import normalize_article_record, validate_article_record
from common.hdfs_utils import upload_hdfs_bytes

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
        default=os.getenv("KAFKA_CONSUMER_GROUP", "news-hdfs-consumer"),
    )
    parser.add_argument("--hdfs-url", default=os.getenv("HDFS_URL", "http://localhost:9870"))
    parser.add_argument("--hdfs-user", default=os.getenv("HDFS_USER", "root"))
    parser.add_argument("--base-path", default=os.getenv("HDFS_BASE_PATH", "/news/raw"))
    parser.add_argument(
        "--webhdfs-redirect-host",
        default=os.getenv("WEBHDFS_REDIRECT_HOST", ""),
        help="Override the hostname returned by WebHDFS redirects when running outside Docker.",
    )
    parser.add_argument("--max-messages", type=int, default=100)
    parser.add_argument("--poll-timeout-ms", type=int, default=5000)
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


def normalize_rows_for_storage(rows: list[dict[str, Any]]) -> tuple[list[dict[str, str]], int]:
    valid_rows: list[dict[str, str]] = []
    invalid_count = 0

    for row in rows:
        normalized = normalize_article_record(row)
        errors = validate_article_record(normalized)
        if errors:
            invalid_count += 1
            continue
        valid_rows.append(normalized)

    return valid_rows, invalid_count


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


def main() -> None:
    from hdfs import InsecureClient

    args = parse_args()
    consumer = create_consumer(args)
    client = InsecureClient(args.hdfs_url, user=args.hdfs_user)

    try:
        rows = collect_messages(
            consumer=consumer,
            max_messages=args.max_messages,
            poll_timeout_ms=args.poll_timeout_ms,
        )
        if not rows:
            print("No messages consumed from Kafka.")
            return

        rows, invalid_count = normalize_rows_for_storage(rows)
        if invalid_count:
            print(f"Skipped {invalid_count} invalid message(s) before writing to HDFS.")
        if not rows:
            print("No valid messages remained after validation.")
            return

        collected_at = datetime.now(timezone.utc)
        output_path = build_output_path(args.base_path, collected_at)
        write_jsonl_to_hdfs(args=args, client=client, output_path=output_path, rows=rows)
        print(f"Wrote {len(rows)} messages to HDFS path: {output_path}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
