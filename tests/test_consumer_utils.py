import unittest

from consumer.kafka_consumer_to_hdfs import build_output_path, normalize_rows_for_storage


class ConsumerUtilsTests(unittest.TestCase):
    def test_build_output_path_uses_date_hierarchy(self) -> None:
        from datetime import datetime, timezone

        output_path = build_output_path("/news/raw", datetime(2026, 3, 28, 8, 15, 1, tzinfo=timezone.utc))
        self.assertEqual(output_path, "/news/raw/2026/03/28/news_081501.jsonl")

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


if __name__ == "__main__":
    unittest.main()
