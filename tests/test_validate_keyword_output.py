import unittest
from unittest.mock import patch

from scripts.validate_keyword_output import (
    read_keyword_metadata,
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
            "common.pipeline_paths.list_hdfs_files",
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

    def test_read_keyword_metadata_uses_redirect_aware_reader(self) -> None:
        with patch(
            "scripts.validate_keyword_output.read_hdfs_bytes",
            return_value=(
                b'{\n'
                b'  "keyword_score_version": "v2",\n'
                b'  "keyword_config_hash": "abc12345"\n'
                b'}'
            ),
        ) as read_mock:
            payload = read_keyword_metadata(
                hdfs_url="http://namenode:9870",
                hdfs_user="root",
                metadata_path="/news/keywords/2026/04/14/news_073417975178/_keyword_metadata.json",
                redirect_host="datanode",
            )

        self.assertEqual(payload["keyword_score_version"], "v2")
        self.assertEqual(payload["keyword_config_hash"], "abc12345")
        read_mock.assert_called_once_with(
            hdfs_url="http://namenode:9870",
            hdfs_user="root",
            path="/news/keywords/2026/04/14/news_073417975178/_keyword_metadata.json",
            redirect_host="datanode",
        )


if __name__ == "__main__":
    unittest.main()
