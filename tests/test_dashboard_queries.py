import unittest
from datetime import date

from dashboard.query_builders import (
    build_article_keywords_query,
    build_breakout_keywords_query,
    build_keyword_metrics_query,
    build_keyword_timeseries_query,
    build_overall_keyword_trends_query,
    build_source_keyword_trends_query,
    build_source_options_query,
    clamp_limit,
)


class DashboardQueryBuilderTests(unittest.TestCase):
    def test_clamp_limit_caps_high_values(self) -> None:
        self.assertEqual(clamp_limit(999), 500)
        self.assertEqual(clamp_limit(0), 1)

    def test_source_options_query_adds_date_filters(self) -> None:
        query, params = build_source_options_query(
            date_from=date(2026, 4, 1),
            date_to=date(2026, 4, 7),
        )

        self.assertIn("FROM vw_streamlit_keyword_daily_source_latest", query)
        self.assertIn("event_date >= %s", query)
        self.assertIn("event_date <= %s", query)
        self.assertEqual(params, [date(2026, 4, 1), date(2026, 4, 7)])

    def test_overall_query_uses_filters_and_limit(self) -> None:
        query, params = build_overall_keyword_trends_query(
            date_from=date(2026, 4, 1),
            date_to=date(2026, 4, 7),
            sources=["VNExpress"],
            ngram_sizes=[2, 3],
            keyword_search="seo",
            limit=50,
        )

        self.assertIn("FROM vw_streamlit_keyword_daily_source_latest", query)
        self.assertIn("source IN (%s)", query)
        self.assertIn("ngram_size IN (%s, %s)", query)
        self.assertIn("keyword_normalized ILIKE %s", query)
        self.assertEqual(
            params,
            [date(2026, 4, 1), date(2026, 4, 7), "VNExpress", 2, 3, "%seo%", 50],
        )

    def test_source_query_skips_empty_sources(self) -> None:
        query, params = build_source_keyword_trends_query(
            date_from=None,
            date_to=None,
            sources=[],
            ngram_sizes=[1],
            keyword_search="",
            limit=20,
        )

        self.assertIn("FROM vw_streamlit_keyword_daily_source_latest", query)
        self.assertNotIn("source IN", query)
        self.assertEqual(params, [1, 20])

    def test_article_query_adds_title_and_source_filters(self) -> None:
        query, params = build_article_keywords_query(
            date_from=date(2026, 4, 2),
            date_to=date(2026, 4, 7),
            sources=["VNExpress"],
            ngram_sizes=[2],
            keyword_search="ai",
            title_search="seo",
            limit=25,
        )

        self.assertIn("FROM vw_streamlit_article_keywords_latest", query)
        self.assertIn("source IN (%s)", query)
        self.assertIn("title ILIKE %s", query)
        self.assertEqual(
            params,
            [
                date(2026, 4, 2),
                date(2026, 4, 7),
                "VNExpress",
                2,
                "%ai%",
                "%seo%",
                25,
            ],
        )

    def test_metrics_query_targets_latest_source_view(self) -> None:
        query, params = build_keyword_metrics_query(
            date_from=None,
            date_to=None,
            sources=["Tuoi Tre"],
            ngram_sizes=[2, 3],
            keyword_search="keyword",
        )

        self.assertIn("COUNT(DISTINCT keyword_normalized)", query)
        self.assertIn("FROM vw_streamlit_keyword_daily_source_latest", query)
        self.assertEqual(params, ["Tuoi Tre", 2, 3, "%keyword%"])

    def test_timeseries_query_builds_top_keyword_cte(self) -> None:
        query, params = build_keyword_timeseries_query(
            date_from=date(2026, 4, 1),
            date_to=date(2026, 4, 7),
            sources=["VNExpress"],
            ngram_sizes=[2],
            keyword_search="ai",
            limit_keywords=5,
        )

        self.assertIn("WITH filtered_source_keywords AS", query)
        self.assertIn("LIMIT %s", query)
        self.assertIn("source IN (%s)", query)
        self.assertEqual(
            params,
            [date(2026, 4, 1), date(2026, 4, 7), "VNExpress", 2, "%ai%", 5],
        )

    def test_breakout_query_uses_latest_date_history_comparison(self) -> None:
        query, params = build_breakout_keywords_query(
            date_from=date(2026, 4, 1),
            date_to=date(2026, 4, 7),
            sources=["Tuoi Tre"],
            ngram_sizes=[2, 3],
            keyword_search="seo",
            limit=20,
        )

        self.assertIn("latest_date AS", query)
        self.assertIn("history AS", query)
        self.assertIn("breakout_score", query)
        self.assertIn("FROM vw_streamlit_keyword_daily_source_latest", query)
        self.assertEqual(
            params,
            [date(2026, 4, 1), date(2026, 4, 7), "Tuoi Tre", 2, 3, "%seo%", 20],
        )


if __name__ == "__main__":
    unittest.main()
