from __future__ import annotations

import argparse
import importlib
import json
import time

from common.kafka_utils import create_kafka_client_with_retry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reset a Kafka consumer group's offsets for a topic to latest.",
    )
    parser.add_argument("--topic", default="news_raw")
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:9093")
    parser.add_argument("--assign-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=0.5)
    return parser.parse_args()


def reset_group_offsets_to_latest(
    *,
    topic: str,
    group_id: str,
    bootstrap_servers: str,
    assign_timeout_seconds: float,
    poll_interval_seconds: float,
) -> dict[str, object]:
    kafka_module = importlib.import_module("kafka")
    kafka_structs_module = importlib.import_module("kafka.structs")
    KafkaConsumer = getattr(kafka_module, "KafkaConsumer")
    OffsetAndMetadata = getattr(kafka_structs_module, "OffsetAndMetadata")

    consumer = create_kafka_client_with_retry(
        client_name="offset_reset_consumer",
        bootstrap_servers=bootstrap_servers,
        logger=None,
        factory=lambda: KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="latest",
        ),
    )

    try:
        consumer.subscribe([topic])
        deadline = time.monotonic() + max(assign_timeout_seconds, 1.0)

        while time.monotonic() < deadline and not consumer.assignment():
            consumer.poll(timeout_ms=max(int(poll_interval_seconds * 1000), 100))

        assignment = set(consumer.assignment())
        if not assignment:
            raise SystemExit(
                f"Unable to obtain partition assignment for topic={topic!r}, group_id={group_id!r}."
            )

        ordered_assignment = sorted(assignment, key=lambda tp: (tp.topic, tp.partition))
        consumer.seek_to_end(*ordered_assignment)

        offsets = {}
        committed_offsets: dict[str, int] = {}
        for tp in ordered_assignment:
            offset = int(consumer.position(tp))
            offsets[tp] = OffsetAndMetadata(offset, "", -1)
            committed_offsets[f"{tp.topic}:{tp.partition}"] = offset

        consumer.commit(offsets=offsets)
        return {
            "topic": topic,
            "group_id": group_id,
            "bootstrap_servers": bootstrap_servers,
            "committed_offsets": committed_offsets,
        }
    finally:
        consumer.close()


def main() -> None:
    args = parse_args()
    result = reset_group_offsets_to_latest(
        topic=args.topic,
        group_id=args.group_id,
        bootstrap_servers=args.bootstrap_servers,
        assign_timeout_seconds=args.assign_timeout_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
