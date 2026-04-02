import unittest

from common.data_quality import summarize_article_quality


class DataQualityTests(unittest.TestCase):
    def test_summarize_article_quality_reports_missing_duplicate_and_zero_source_metrics(self) -> None:
        summary = summarize_article_quality(
            [
                {"title": "One", "link": "https://example.com/1", "source": "VNExpress"},
                {"title": "", "link": "https://example.com/1", "source": "VNExpress"},
                {"title": "Three", "link": "", "source": "VTV"},
            ],
            expected_sources=["VNExpress", "VTV", "Tuoi Tre"],
        )

        self.assertEqual(summary["total_count"], 3)
        self.assertEqual(summary["missing_title_count"], 1)
        self.assertEqual(summary["missing_link_count"], 1)
        self.assertEqual(summary["duplicate_count"], 1)
        self.assertEqual(summary["articles_by_source"]["VNExpress"], 2)
        self.assertEqual(summary["articles_by_source"]["VTV"], 1)
        self.assertEqual(summary["zero_article_sources"], ["Tuoi Tre"])


if __name__ == "__main__":
    unittest.main()
