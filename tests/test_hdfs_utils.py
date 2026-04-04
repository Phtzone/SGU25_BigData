import unittest

from common.hdfs_utils import build_hdfs_uri, derive_hdfs_default_fs, rewrite_webhdfs_redirect


class HdfsUtilsTests(unittest.TestCase):
    def test_derive_hdfs_default_fs_from_webhdfs_url(self) -> None:
        self.assertEqual(
            derive_hdfs_default_fs("http://namenode:9870"),
            "hdfs://namenode:9000",
        )

    def test_build_hdfs_uri_uses_default_fs_for_absolute_path(self) -> None:
        self.assertEqual(
            build_hdfs_uri("/news/raw/2026/03/28/news.jsonl", "hdfs://namenode:9000"),
            "hdfs://namenode:9000/news/raw/2026/03/28/news.jsonl",
        )

    def test_build_hdfs_uri_keeps_existing_uri(self) -> None:
        self.assertEqual(
            build_hdfs_uri("hdfs://namenode:9000/news/raw/file.jsonl", "hdfs://other:9000"),
            "hdfs://namenode:9000/news/raw/file.jsonl",
        )

    def test_rewrite_webhdfs_redirect_to_localhost(self) -> None:
        rewritten = rewrite_webhdfs_redirect(
            location="http://datanode:9864/webhdfs/v1/news/raw/file.jsonl?op=OPEN",
            requested_hdfs_url="http://localhost:9870",
            redirect_host="",
        )

        self.assertEqual(
            rewritten,
            "http://localhost:9864/webhdfs/v1/news/raw/file.jsonl?op=OPEN",
        )

    def test_rewrite_webhdfs_redirect_respects_override(self) -> None:
        rewritten = rewrite_webhdfs_redirect(
            location="http://datanode:9864/webhdfs/v1/news/raw/file.jsonl?op=OPEN",
            requested_hdfs_url="http://namenode:9870",
            redirect_host="custom-host",
        )

        self.assertEqual(
            rewritten,
            "http://custom-host:9864/webhdfs/v1/news/raw/file.jsonl?op=OPEN",
        )


if __name__ == "__main__":
    unittest.main()
