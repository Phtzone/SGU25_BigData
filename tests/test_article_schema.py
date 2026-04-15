import unittest

import common.article_schema as article_schema
from common.article_schema import build_article_record, normalize_article_record, validate_article_record


class ArticleSchemaTests(unittest.TestCase):
    def test_build_article_record_normalizes_fields(self) -> None:
        article = build_article_record(
            title="  Example title  ",
            link=" https://example.com/story ",
            summary=" Summary   with   spaces ",
            published_at="Sat, 28 Mar 2026 12:34:56 GMT",
            source=" Example Source ",
            fetched_at="2026-03-28T12:35:00+00:00",
            ingestion_id="abc123",
        )

        self.assertEqual(article["title"], "Example title")
        self.assertEqual(article["link"], "https://example.com/story")
        self.assertEqual(article["summary"], "Summary with spaces")
        self.assertEqual(article["source"], "Example Source")
        self.assertEqual(article["published_at"], "2026-03-28T12:34:56+00:00")
        self.assertEqual(article["fetched_at"], "2026-03-28T12:35:00+00:00")
        self.assertEqual(article["ingestion_id"], "abc123")
        self.assertEqual(
            tuple(article.keys()),
            (
                "title",
                "link",
                "summary",
                "published_at",
                "source",
                "fetched_at",
                "ingestion_id",
            ),
        )

    def test_normalize_article_record_normalizes_to_unified_contract(self) -> None:
        normalized = normalize_article_record(
            {
                "title": "Hello",
                "link": "https://example.com",
                "summary": "World",
                "published_at": "Sat, 28 Mar 2026 12:34:56 GMT",
                "source": "VNExpress",
                "fetched_at": "2026-03-28T12:35:00+00:00",
                "ingestion_id": "ing-001",
                "ignored_field": "should-not-survive",
            }
        )

        self.assertEqual(normalized["published_at"], "2026-03-28T12:34:56+00:00")
        self.assertEqual(normalized["ingestion_id"], "ing-001")
        self.assertNotIn("ignored_field", normalized)

    def test_validate_article_record_requires_unified_contract_fields(self) -> None:
        errors = validate_article_record(
            {
                "title": "",
                "link": "",
                "summary": "",
                "published_at": "",
                "source": "",
                "fetched_at": "",
                "ingestion_id": "",
            }
        )

        self.assertIn("title is required", errors)
        self.assertIn("link is required", errors)
        self.assertIn("published_at is required", errors)
        self.assertIn("source is required", errors)
        self.assertIn("fetched_at is required", errors)
        self.assertIn("ingestion_id is required", errors)

    def test_validate_original_article_record_rejects_unexpected_fields(self) -> None:
        self.assertTrue(
            hasattr(article_schema, "validate_original_article_record"),
            "validate_original_article_record should exist",
        )
        errors = article_schema.validate_original_article_record(  # type: ignore[attr-defined]
            {
                "title": "Hello",
                "link": "https://example.com",
                "summary": "World",
                "published_at": "2026-03-28T12:34:56+00:00",
                "source": "VNExpress",
                "fetched_at": "2026-03-28T12:35:00+00:00",
                "ingestion_id": "ing-001",
                "extra_field": "should-fail",
            }
        )

        self.assertIn("unexpected fields: extra_field", errors)


if __name__ == "__main__":
    unittest.main()
