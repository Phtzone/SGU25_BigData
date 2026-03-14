import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from hdfs import InsecureClient
from kafka import KafkaConsumer
import requests


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


def create_consumer(args: argparse.Namespace) -> KafkaConsumer:
    return KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=args.group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def collect_messages(
    consumer: KafkaConsumer,
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
    client: InsecureClient,
    output_path: str,
    rows: list[dict[str, Any]],
) -> None:
    directory = str(PurePosixPath(output_path).parent)
    client.makedirs(directory)

    payload = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows).encode("utf-8")
    create_url = f"{args.hdfs_url.rstrip('/')}/webhdfs/v1{output_path}"
    create_response = requests.put(
        create_url,
        params={
            "op": "CREATE",
            "overwrite": "true",
            "user.name": args.hdfs_user,
        },
        allow_redirects=False,
        timeout=30,
    )

    if create_response.status_code in (307, 308):
        redirect_url = rewrite_webhdfs_redirect(
            location=create_response.headers["Location"],
            requested_hdfs_url=args.hdfs_url,
            redirect_host=args.webhdfs_redirect_host,
        )
        upload_response = requests.put(
            redirect_url,
            data=payload,
            headers={"Content-Type": "application/octet-stream"},
            timeout=60,
        )
        upload_response.raise_for_status()
        return

    create_response.raise_for_status()


def rewrite_webhdfs_redirect(
    location: str,
    requested_hdfs_url: str,
    redirect_host: str,
) -> str:
    parts = urlsplit(location)
    effective_host = redirect_host.strip()

    if not effective_host:
        requested_host = urlsplit(requested_hdfs_url).hostname
        if requested_host in {"localhost", "127.0.0.1"}:
            effective_host = "localhost"

    if not effective_host:
        return location

    netloc = effective_host
    if parts.port:
        netloc = f"{effective_host}:{parts.port}"

    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def main() -> None:
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

        collected_at = datetime.now(timezone.utc)
        output_path = build_output_path(args.base_path, collected_at)
        write_jsonl_to_hdfs(args=args, client=client, output_path=output_path, rows=rows)
        print(f"Wrote {len(rows)} messages to HDFS path: {output_path}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
