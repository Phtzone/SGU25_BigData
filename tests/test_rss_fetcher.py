import unittest

from producer.rss_fetcher import deduplicate_articles, normalize_entry


class RssFetcherTests(unittest.TestCase):
    def test_normalize_entry_builds_unified_article_contract(self) -> None:
        article = normalize_entry(
            {
                "title": "  Example title  ",
                "link": " https://example.com/story ",
                "description": " Summary text ",
                "updated": "Sat, 28 Mar 2026 12:34:56 GMT",
            },
            " Example Source ",
        )

        self.assertEqual(article["title"], "Example title")
        self.assertEqual(article["link"], "https://example.com/story")
        self.assertEqual(article["summary"], "Summary text")
        self.assertEqual(article["published_at"], "2026-03-28T12:34:56+00:00")
        self.assertEqual(article["source"], "Example Source")
        self.assertTrue(article["fetched_at"])
        self.assertTrue(article["ingestion_id"])

    def test_deduplicate_articles_keeps_first_article_for_each_link(self) -> None:
        unique_articles = deduplicate_articles(
            [
                {"title": "One", "link": "https://example.com/1"},
                {"title": "Duplicate", "link": "https://example.com/1"},
                {"title": "Two", "link": "https://example.com/2"},
            ]
        )

        self.assertEqual(len(unique_articles), 2)
        self.assertEqual(unique_articles[0]["title"], "One")
        self.assertEqual(unique_articles[1]["title"], "Two")


if __name__ == "__main__":
    unittest.main()
