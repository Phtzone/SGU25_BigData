import unittest
from unittest.mock import patch

from consumer.kafka_consumer_to_hdfs import (
    build_output_path,
    commit_processed_offsets,
    normalize_rows_for_storage,
    parse_args,
)


class ConsumerUtilsTests(unittest.TestCase):
    def test_build_output_path_uses_date_hierarchy(self) -> None:
        from datetime import datetime, timezone

        output_path = build_output_path(
            "/news/raw",
            datetime(2026, 3, 28, 8, 15, 1, 123456, tzinfo=timezone.utc),
        )
        self.assertEqual(output_path, "/news/raw/2026/03/28/news_081501123456.jsonl")

    def test_build_output_path_is_unique_within_same_second(self) -> None:
        from datetime import datetime, timezone

        first = build_output_path(
            "/news/raw",
            datetime(2026, 3, 28, 8, 15, 1, 100000, tzinfo=timezone.utc),
        )
        second = build_output_path(
            "/news/raw",
            datetime(2026, 3, 28, 8, 15, 1, 200000, tzinfo=timezone.utc),
        )

        self.assertNotEqual(first, second)

    def test_normalize_rows_for_storage_filters_invalid_rows(self) -> None:
        rows, invalid_count = normalize_rows_for_storage(
            [
                {
                    "title": "Good",
                    "link": "https://example.com/1",
                    "summary": "One",
                    "published_at": "2026-03-28T08:00:00+00:00",
                    "source": "VNExpress",
                    "fetched_at": "2026-03-28T08:05:00+00:00",
                    "ingestion_id": "ing-001",
                },
                {
                    "title": "",
                    "link": "https://example.com/2",
                    "summary": "Two",
                    "published_at": "invalid-date",
                    "source": "VTV",
                    "fetched_at": "2026-03-28T08:05:00+00:00",
                    "ingestion_id": "ing-002",
                },
            ]
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(invalid_count, 1)
        self.assertEqual(rows[0]["title"], "Good")

    def test_commit_processed_offsets_calls_consumer_commit(self) -> None:
        class FakeConsumer:
            def __init__(self) -> None:
                self.committed = False

            def commit(self) -> None:
                self.committed = True

        consumer = FakeConsumer()
        commit_processed_offsets(consumer)
        self.assertTrue(consumer.committed)

    def test_parse_args_reads_auto_offset_reset(self) -> None:
        with patch.dict("os.environ", {"KAFKA_AUTO_OFFSET_RESET": "latest"}, clear=False):
            with patch("sys.argv", ["consumer.kafka_consumer_to_hdfs"]):
                args = parse_args()

        self.assertEqual(args.auto_offset_reset, "latest")


if __name__ == "__main__":
    unittest.main()
