import json
import unittest
from pathlib import Path

from Spark_jobs.extract_news_keywords import (
    DEFAULT_KEYWORD_SETTINGS,
    build_keyword_output_path,
    filter_tokens,
    generate_ngrams,
    is_valid_ngram,
    load_keyword_settings,
    normalize_keyword_text,
    score_keywords_for_article,
    split_candidate_token_segments,
    tokenize_keyword_text,
)


class KeywordExtractionTests(unittest.TestCase):
    def test_build_keyword_output_path_mirrors_curated_date(self) -> None:
        output_path = build_keyword_output_path(
            "/news/curated/2026/04/04/news_120000000000",
            "/news/keywords",
        )

        self.assertEqual(output_path, "/news/keywords/2026/04/04/news_120000000000")

    def test_normalize_keyword_text_lowercases_and_removes_urls(self) -> None:
        normalized = normalize_keyword_text("AI cho SEO! Xem https://example.com/ngay")
        self.assertEqual(normalized, "ai cho seo xem")

    def test_generate_ngrams_builds_all_sizes_up_to_max(self) -> None:
        ngrams = generate_ngrams(["tri", "tue", "nhan", "tao"], max_ngram_size=3)

        self.assertIn(("tri", 1), ngrams)
        self.assertIn(("tri tue", 2), ngrams)
        self.assertIn(("tri tue nhan", 3), ngrams)
        self.assertIn(("tue nhan tao", 3), ngrams)

    def test_filter_tokens_removes_stopwords_short_tokens_and_numbers(self) -> None:
        filtered = filter_tokens(
            tokenize_keyword_text("ai va 2026 seo du lieu"),
            stopwords={"va"},
            blocked_terms={"xem"},
            min_token_length=2,
        )

        self.assertEqual(filtered, ["ai", "seo", "du", "lieu"])

    def test_split_candidate_token_segments_breaks_ngrams_on_stopwords(self) -> None:
        segments = split_candidate_token_segments(
            tokenize_keyword_text("AI cho SEO và dữ liệu"),
            stopwords={"cho", "và"},
            blocked_terms={"xem"},
            min_token_length=2,
        )

        self.assertEqual(segments, [["AI"], ["SEO"], ["dữ", "liệu"]])

    def test_is_valid_ngram_rejects_repeated_tokens(self) -> None:
        self.assertFalse(is_valid_ngram(["ai", "ai"]))
        self.assertTrue(is_valid_ngram(["du", "lieu"]))

    def test_score_keywords_for_article_prefers_repeated_title_terms(self) -> None:
        settings = dict(DEFAULT_KEYWORD_SETTINGS)
        settings["top_keywords_per_article"] = 10

        ranked = score_keywords_for_article(
            title="AI AI cho SEO",
            summary="AI cho doanh nghiep va seo",
            settings=settings,
            stopwords={"cho", "va"},
        )

        self.assertTrue(ranked)
        self.assertEqual(ranked[0]["keyword"], "ai")
        self.assertEqual(ranked[0]["article_score"], 5.0)
        self.assertEqual(ranked[0]["rank_in_article"], 1)
        self.assertIn("doanh nghiep", {item["keyword"] for item in ranked})
        self.assertNotIn("ai seo", {item["keyword"] for item in ranked})

    def test_score_keywords_for_article_removes_blocked_noise_terms(self) -> None:
        settings = dict(DEFAULT_KEYWORD_SETTINGS)
        settings["top_keywords_per_article"] = 10

        ranked = score_keywords_for_article(
            title="Xem video ngay về AI",
            summary="Clip AI cho doanh nghiệp",
            settings=settings,
            stopwords={"cho", "về"},
        )

        keywords = {item["keyword"] for item in ranked}

        self.assertNotIn("xem", keywords)
        self.assertNotIn("video", keywords)
        self.assertIn("ai", keywords)

    def test_load_keyword_settings_merges_with_defaults(self) -> None:
        config_dir = Path(".tmp")
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "test_keyword_settings.json"
        try:
            config_path.write_text(
                json.dumps({"top_keywords_per_article": 7, "title_weight": 3.0}),
                encoding="utf-8",
            )

            settings = load_keyword_settings(str(config_path))
        finally:
            config_path.unlink(missing_ok=True)

        self.assertEqual(settings["top_keywords_per_article"], 7)
        self.assertEqual(settings["title_weight"], 3.0)
        self.assertEqual(
            settings["top_keywords_per_day_source"],
            DEFAULT_KEYWORD_SETTINGS["top_keywords_per_day_source"],
        )


if __name__ == "__main__":
    unittest.main()
