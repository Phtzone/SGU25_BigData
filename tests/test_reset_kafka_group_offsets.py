import types
import subprocess
import sys
import unittest
from unittest.mock import patch
from pathlib import Path

import scripts.reset_kafka_group_offsets as offset_reset_module


class FakeTopicPartition:
    def __init__(self, topic: str, partition: int) -> None:
        self.topic = topic
        self.partition = partition

    def __hash__(self) -> int:
        return hash((self.topic, self.partition))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FakeTopicPartition):
            return False
        return (self.topic, self.partition) == (other.topic, other.partition)


class FakeConsumer:
    def __init__(self, assignment: set[FakeTopicPartition], positions: dict[FakeTopicPartition, int]) -> None:
        self._assignment = assignment
        self._positions = positions
        self.subscribed_topics: list[list[str]] = []
        self.seek_calls: list[tuple[FakeTopicPartition, ...]] = []
        self.commit_calls: list[dict[FakeTopicPartition, object]] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed_topics.append(topics)

    def assignment(self) -> set[FakeTopicPartition]:
        return self._assignment

    def poll(self, timeout_ms: int) -> dict[object, object]:  # noqa: ARG002
        return {}

    def seek_to_end(self, *partitions: FakeTopicPartition) -> None:
        self.seek_calls.append(partitions)

    def position(self, partition: FakeTopicPartition) -> int:
        return self._positions[partition]

    def commit(self, offsets: dict[FakeTopicPartition, object]) -> None:
        self.commit_calls.append(offsets)

    def close(self) -> None:
        self.closed = True


class ResetKafkaGroupOffsetsTests(unittest.TestCase):
    def test_script_help_runs_when_invoked_by_path(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        script_path = project_root / "scripts" / "reset_kafka_group_offsets.py"

        result = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("--group-id", result.stdout)

    def test_reset_group_offsets_supports_two_field_offset_and_metadata(self) -> None:
        assignment = {FakeTopicPartition("news_raw", 0)}
        topic_partition = next(iter(assignment))
        consumer = FakeConsumer(assignment, {topic_partition: 12})

        class TwoFieldOffsetAndMetadata:
            def __init__(self, offset: int, metadata: str) -> None:
                self.offset = offset
                self.metadata = metadata

        fake_kafka_module = types.SimpleNamespace(KafkaConsumer=object)
        fake_kafka_structs_module = types.SimpleNamespace(OffsetAndMetadata=TwoFieldOffsetAndMetadata)

        with patch(
            "scripts.reset_kafka_group_offsets.importlib.import_module",
            side_effect=lambda name: fake_kafka_module if name == "kafka" else fake_kafka_structs_module,
        ):
            with patch.object(
                offset_reset_module,
                "create_kafka_client_with_retry",
                return_value=consumer,
            ):
                result = offset_reset_module.reset_group_offsets_to_latest(
                    topic="news_raw",
                    group_id="demo-group",
                    bootstrap_servers="localhost:9093",
                    assign_timeout_seconds=1,
                    poll_interval_seconds=0,
                )

        self.assertEqual(result["committed_offsets"], {"news_raw:0": 12})
        committed_offset = consumer.commit_calls[0][topic_partition]
        self.assertEqual(committed_offset.offset, 12)
        self.assertEqual(committed_offset.metadata, "")
        self.assertTrue(consumer.closed)

    def test_reset_group_offsets_supports_three_field_offset_and_metadata(self) -> None:
        assignment = {FakeTopicPartition("news_raw", 1)}
        topic_partition = next(iter(assignment))
        consumer = FakeConsumer(assignment, {topic_partition: 24})

        class ThreeFieldOffsetAndMetadata:
            def __init__(self, offset: int, metadata: str, leader_epoch: int) -> None:
                self.offset = offset
                self.metadata = metadata
                self.leader_epoch = leader_epoch

        fake_kafka_module = types.SimpleNamespace(KafkaConsumer=object)
        fake_kafka_structs_module = types.SimpleNamespace(OffsetAndMetadata=ThreeFieldOffsetAndMetadata)

        with patch(
            "scripts.reset_kafka_group_offsets.importlib.import_module",
            side_effect=lambda name: fake_kafka_module if name == "kafka" else fake_kafka_structs_module,
        ):
            with patch.object(
                offset_reset_module,
                "create_kafka_client_with_retry",
                return_value=consumer,
            ):
                result = offset_reset_module.reset_group_offsets_to_latest(
                    topic="news_raw",
                    group_id="demo-group",
                    bootstrap_servers="localhost:9093",
                    assign_timeout_seconds=1,
                    poll_interval_seconds=0,
                )

        self.assertEqual(result["committed_offsets"], {"news_raw:1": 24})
        committed_offset = consumer.commit_calls[0][topic_partition]
        self.assertEqual(committed_offset.offset, 24)
        self.assertEqual(committed_offset.metadata, "")
        self.assertEqual(committed_offset.leader_epoch, -1)
        self.assertTrue(consumer.closed)


if __name__ == "__main__":
    unittest.main()
