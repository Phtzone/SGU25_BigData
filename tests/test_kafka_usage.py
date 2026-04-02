import unittest

from consumer.kafka_consumer_to_hdfs import build_dead_letter_message, split_rows_by_validity
from producer.kafka_producer import build_message_key


class KafkaUsageTests(unittest.TestCase):
    def test_build_message_key_uses_normalized_link(self) -> None:
        message_key = build_message_key({"link": " https://example.com/story "})
        self.assertEqual(message_key, "https://example.com/story")

    def test_split_rows_by_validity_collects_invalid_rows_for_dead_letter(self) -> None:
        valid_rows, invalid_rows = split_rows_by_validity(
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
                    "published_at": "bad-date",
                    "source": "VTV",
                    "fetched_at": "2026-03-28T08:05:00+00:00",
                    "ingestion_id": "ing-002",
                },
            ]
        )

        self.assertEqual(len(valid_rows), 1)
        self.assertEqual(len(invalid_rows), 1)
        self.assertIn("title is required", invalid_rows[0]["errors"])

    def test_build_dead_letter_message_keeps_error_context(self) -> None:
        payload = build_dead_letter_message(
            invalid_row={
                "original_payload": {"link": "https://example.com/2"},
                "normalized_payload": {"link": "https://example.com/2"},
                "errors": ["title is required"],
            },
            topic="news_raw",
            group_id="news-raw-to-hdfs-v1",
        )

        self.assertEqual(payload["reason"], "article_validation_failed")
        self.assertEqual(payload["source_topic"], "news_raw")
        self.assertEqual(payload["consumer_group"], "news-raw-to-hdfs-v1")
        self.assertEqual(payload["errors"], ["title is required"])


if __name__ == "__main__":
    unittest.main()
