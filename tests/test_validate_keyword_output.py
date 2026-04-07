import unittest
from unittest.mock import patch

from scripts.validate_keyword_output import (
    resolve_keyword_batch_from_parquet,
    resolve_latest_keyword_batch,
)


class ValidateKeywordOutputTests(unittest.TestCase):
    def test_resolve_keyword_batch_from_article_keywords_parquet(self) -> None:
        batch_path = resolve_keyword_batch_from_parquet(
            "/news/keywords/2026/04/07/news_120000000000/article_keywords/part-0000.parquet"
        )

        self.assertEqual(batch_path, "/news/keywords/2026/04/07/news_120000000000")

    def test_resolve_latest_keyword_batch_uses_latest_parquet_file(self) -> None:
        class FakeClient:
            @staticmethod
            def status(path: str, strict: bool = False):  # noqa: ARG004
                return {"type": "DIRECTORY"}

        with patch(
            "scripts.validate_keyword_output.list_hdfs_files",
            return_value=[
                (
                    "/news/keywords/2026/04/06/news_100000000000/article_keywords/part-0000.parquet",
                    {"modificationTime": 100},
                ),
                (
                    "/news/keywords/2026/04/07/news_120000000000/keyword_daily_source/part-0000.parquet",
                    {"modificationTime": 200},
                ),
            ],
        ):
            batch_path = resolve_latest_keyword_batch(FakeClient(), "/news/keywords")

        self.assertEqual(batch_path, "/news/keywords/2026/04/07/news_120000000000")


if __name__ == "__main__":
    unittest.main()
