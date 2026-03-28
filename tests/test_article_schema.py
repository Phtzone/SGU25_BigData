import unittest

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
        self.assertEqual(article["published_at_raw"], "Sat, 28 Mar 2026 12:34:56 GMT")
        self.assertEqual(article["fetched_at"], "2026-03-28T12:35:00+00:00")
        self.assertEqual(article["ingestion_id"], "abc123")

    def test_normalize_article_record_preserves_raw_published_value(self) -> None:
        normalized = normalize_article_record(
            {
                "title": "Hello",
                "link": "https://example.com",
                "summary": "World",
                "published_at_raw": "Sat, 28 Mar 2026 12:34:56 GMT",
                "source": "VNExpress",
                "fetched_at": "2026-03-28T12:35:00+00:00",
            }
        )

        self.assertEqual(normalized["published_at_raw"], "Sat, 28 Mar 2026 12:34:56 GMT")
        self.assertEqual(normalized["published_at"], "2026-03-28T12:34:56+00:00")

    def test_validate_article_record_requires_core_fields(self) -> None:
        errors = validate_article_record({"title": "", "link": "", "source": ""})

        self.assertIn("title is required", errors)
        self.assertIn("link is required", errors)
        self.assertIn("source is required", errors)


if __name__ == "__main__":
    unittest.main()
