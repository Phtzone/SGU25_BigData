import json
import unittest
from pathlib import Path
from unittest.mock import patch

from Spark_jobs.extract_news_keywords import (
    DEFAULT_KEYWORD_SETTINGS,
    build_keyword_config_hash,
    build_keyword_output_path,
    filter_tokens,
    generate_ngrams,
    is_valid_ngram,
    load_keyword_settings,
    load_source_keyword_blocklist,
    normalize_keyword_text,
    score_keywords_for_article,
    split_candidate_token_segments,
    tokenize_keyword_text,
    write_keyword_metadata,
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
        ngrams = generate_ngrams(
            ["tri", "tue", "nhan", "tao"],
            min_ngram_size=1,
            max_ngram_size=3,
        )

        self.assertIn(("tri", 1), ngrams)
        self.assertIn(("tri tue", 2), ngrams)
        self.assertIn(("tri tue nhan", 3), ngrams)
        self.assertIn(("tue nhan tao", 3), ngrams)

    def test_generate_ngrams_respects_min_ngram_size(self) -> None:
        ngrams = generate_ngrams(
            ["tri", "tue", "nhan", "tao"],
            min_ngram_size=2,
            max_ngram_size=3,
        )

        self.assertNotIn(("tri", 1), ngrams)
        self.assertIn(("tri tue", 2), ngrams)
        self.assertIn(("tri tue nhan", 3), ngrams)

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
        settings["min_keyword_score"] = 0.5
        settings["min_ngram_size"] = 1
        settings["summary_only_penalty"] = 0.0

        ranked = score_keywords_for_article(
            source="VNExpress",
            title="AI AI cho SEO",
            summary="AI cho doanh nghiep va seo",
            settings=settings,
            stopwords={"cho", "va"},
            source_blocklist={},
            keyword_config_hash="cfg12345",
        )

        self.assertTrue(ranked)
        self.assertEqual(ranked[0]["keyword"], "ai")
        self.assertEqual(ranked[0]["article_score"], 5.0)
        self.assertEqual(ranked[0]["rank_in_article"], 1)
        self.assertEqual(ranked[0]["keyword_score_version"], settings["keyword_score_version"])
        self.assertEqual(ranked[0]["keyword_config_hash"], "cfg12345")
        self.assertIn("doanh nghiep", {item["keyword"] for item in ranked})
        self.assertNotIn("ai seo", {item["keyword"] for item in ranked})

    def test_score_keywords_for_article_removes_blocked_noise_terms(self) -> None:
        settings = dict(DEFAULT_KEYWORD_SETTINGS)
        settings["top_keywords_per_article"] = 10
        settings["min_ngram_size"] = 1

        ranked = score_keywords_for_article(
            source="VTV",
            title="Xem video ngay về AI",
            summary="Clip AI cho doanh nghiệp",
            settings=settings,
            stopwords={"cho", "về"},
            source_blocklist={},
            keyword_config_hash="cfg12345",
        )

        keywords = {item["keyword"] for item in ranked}

        self.assertNotIn("xem", keywords)
        self.assertNotIn("video", keywords)
        self.assertIn("ai", keywords)

    def test_score_keywords_for_article_applies_summary_only_penalty(self) -> None:
        settings = dict(DEFAULT_KEYWORD_SETTINGS)
        settings["min_keyword_score"] = 0.5
        settings["min_ngram_size"] = 1
        settings["top_keywords_per_article"] = 20

        ranked = score_keywords_for_article(
            source="VNExpress",
            title="Kinh tế hôm nay",
            summary="AI doanh nghiệp tăng tốc",
            settings=settings,
            stopwords={"hôm", "nay"},
            source_blocklist={},
            keyword_config_hash="cfg12345",
        )

        ai_row = next(item for item in ranked if item["keyword"] == "ai")
        self.assertIn("summary_only", ai_row["quality_flags"])
        self.assertGreater(ai_row["quality_penalty"], 0.0)

    def test_score_keywords_for_article_filters_source_blocked_phrase(self) -> None:
        settings = dict(DEFAULT_KEYWORD_SETTINGS)
        settings["blocked_phrases"] = []

        ranked = score_keywords_for_article(
            source="VNExpress",
            title="Bình luận AI doanh nghiệp",
            summary="Bình luận AI tiếp tục nóng",
            settings=settings,
            stopwords=set(),
            source_blocklist={"vnexpress": {"bình luận"}},
            keyword_config_hash="cfg12345",
        )

        self.assertNotIn("bình luận", {item["keyword"] for item in ranked})

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

    def test_load_source_keyword_blocklist_normalizes_terms(self) -> None:
        config_dir = Path(".tmp")
        config_dir.mkdir(parents=True, exist_ok=True)
        blocklist_path = config_dir / "test_source_keyword_blocklist.json"
        try:
            blocklist_path.write_text(
                json.dumps({"VNExpress": ["Bình luận", "Xem thêm"]}),
                encoding="utf-8",
            )

            blocklist = load_source_keyword_blocklist(str(blocklist_path))
        finally:
            blocklist_path.unlink(missing_ok=True)

        self.assertEqual(blocklist["vnexpress"], {"bình luận", "xem thêm"})

    def test_build_keyword_config_hash_is_stable_for_same_inputs(self) -> None:
        settings = dict(DEFAULT_KEYWORD_SETTINGS)
        hash_1 = build_keyword_config_hash(
            settings=settings,
            stopwords={"va", "cho"},
            source_blocklist={"default": {"xem thêm"}},
        )
        hash_2 = build_keyword_config_hash(
            settings=settings,
            stopwords={"cho", "va"},
            source_blocklist={"default": {"xem thêm"}},
        )

        self.assertEqual(hash_1, hash_2)

    def test_write_keyword_metadata_uses_redirect_aware_upload(self) -> None:
        metadata_payload = {
            "batch_path": "/news/curated/2026/04/14/news_073417975178",
            "keyword_output_path": "/news/keywords/2026/04/14/news_073417975178",
            "keyword_score_version": "v2",
            "keyword_config_hash": "abc12345",
        }

        with patch("Spark_jobs.extract_news_keywords.upload_hdfs_bytes") as upload_mock:
            write_keyword_metadata(
                hdfs_url="http://namenode:9870",
                hdfs_user="root",
                output_path="/news/keywords/2026/04/14/news_073417975178",
                metadata_payload=metadata_payload,
                redirect_host="datanode",
            )

        upload_mock.assert_called_once_with(
            hdfs_url="http://namenode:9870",
            hdfs_user="root",
            path="/news/keywords/2026/04/14/news_073417975178/_keyword_metadata.json",
            data=json.dumps(metadata_payload, ensure_ascii=False, indent=2).encode("utf-8"),
            redirect_host="datanode",
            overwrite=True,
            content_type="application/json",
        )


if __name__ == "__main__":
    unittest.main()
