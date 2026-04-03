import unittest

from consumer.kafka_consumer_to_hdfs import (
    build_dead_letter_message,
    build_deserialization_dead_letter_message,
    decode_message_value,
    split_rows_by_validity,
)
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

    def test_decode_message_value_reports_utf8_error(self) -> None:
        parsed, error = decode_message_value(b"\xff\xfe")

        self.assertIsNone(parsed)
        self.assertIsNotNone(error)
        self.assertEqual(error["error_type"], "utf8_decode_error")

    def test_decode_message_value_reports_json_error(self) -> None:
        parsed, error = decode_message_value(b'{"title":')

        self.assertIsNone(parsed)
        self.assertIsNotNone(error)
        self.assertEqual(error["error_type"], "json_decode_error")

    def test_decode_message_value_requires_json_object(self) -> None:
        parsed, error = decode_message_value(b'["not-an-object"]')

        self.assertIsNone(parsed)
        self.assertIsNotNone(error)
        self.assertEqual(error["error_type"], "json_payload_not_object")

    def test_build_deserialization_dead_letter_message_keeps_decode_context(self) -> None:
        payload = build_deserialization_dead_letter_message(
            failed_row={
                "error_type": "json_decode_error",
                "error_message": "Expecting value",
                "raw_payload": {"encoding": "base64", "is_truncated": False, "payload": "eyJ0aXRsZSI6"},
                "message_key": "article-1",
                "partition": 0,
                "offset": 10,
                "timestamp": 1711785600000,
            },
            topic="news_raw",
            group_id="news-raw-to-hdfs-v1",
        )

        self.assertEqual(payload["reason"], "message_deserialization_failed")
        self.assertEqual(payload["source_topic"], "news_raw")
        self.assertEqual(payload["consumer_group"], "news-raw-to-hdfs-v1")
        self.assertEqual(payload["error_type"], "json_decode_error")
        self.assertEqual(payload["message_key"], "article-1")
        self.assertEqual(payload["partition"], 0)
        self.assertEqual(payload["offset"], 10)


if __name__ == "__main__":
    unittest.main()
