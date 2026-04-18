import unittest
from argparse import Namespace
from unittest.mock import call
from unittest.mock import patch

from consumer.kafka_consumer_to_hdfs import (
    build_output_path,
    commit_processed_offsets,
    normalize_rows_for_storage,
    parse_args,
    write_jsonl_to_hdfs,
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


class WriteJsonlToHdfsTests(unittest.TestCase):
    def _build_args(self, retries: int = 2, base_delay_seconds: float = 0.1) -> Namespace:
        return Namespace(
            hdfs_url="http://localhost:9870",
            hdfs_user="root",
            webhdfs_redirect_host="",
            hdfs_safe_mode_retries=retries,
            hdfs_safe_mode_retry_delay_seconds=base_delay_seconds,
        )

    @patch("consumer.kafka_consumer_to_hdfs.upload_hdfs_bytes")
    @patch("consumer.kafka_consumer_to_hdfs.time.sleep")
    def test_write_jsonl_to_hdfs_retries_when_hdfs_in_safe_mode(
        self, mock_sleep, mock_upload
    ) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.calls = 0

            def makedirs(self, _directory: str) -> None:
                self.calls += 1
                if self.calls < 3:
                    raise RuntimeError("Name node is in safe mode.")

        args = self._build_args(retries=3, base_delay_seconds=0.1)
        client = FakeClient()

        write_jsonl_to_hdfs(
            args=args,
            client=client,
            output_path="/news/raw/2026/04/18/news_000000000000.jsonl",
            rows=[{"title": "A"}],
        )

        self.assertEqual(client.calls, 3)
        self.assertEqual(mock_sleep.call_args_list, [call(0.1), call(0.2)])
        mock_upload.assert_called_once()

    @patch("consumer.kafka_consumer_to_hdfs.upload_hdfs_bytes")
    @patch("consumer.kafka_consumer_to_hdfs.time.sleep")
    def test_write_jsonl_to_hdfs_does_not_retry_non_safe_mode_errors(
        self, mock_sleep, mock_upload
    ) -> None:
        class FakeClient:
            def makedirs(self, _directory: str) -> None:
                raise RuntimeError("Permission denied.")

        args = self._build_args(retries=3, base_delay_seconds=0.1)

        with self.assertRaisesRegex(RuntimeError, "Permission denied"):
            write_jsonl_to_hdfs(
                args=args,
                client=FakeClient(),
                output_path="/news/raw/2026/04/18/news_000000000000.jsonl",
                rows=[{"title": "A"}],
            )

        mock_sleep.assert_not_called()
        mock_upload.assert_not_called()


if __name__ == "__main__":
    unittest.main()
