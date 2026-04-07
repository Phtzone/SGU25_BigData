import unittest

from common.hdfs_utils import (
    build_hdfs_uri,
    derive_hdfs_default_fs,
    resolve_explicit_or_latest_path,
    rewrite_webhdfs_redirect,
)


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

    def test_resolve_explicit_or_latest_path_prefers_explicit_existing_path(self) -> None:
        class FakeClient:
            @staticmethod
            def status(path: str, strict: bool = False):  # noqa: ARG004
                return {"type": "FILE"} if path == "/news/raw/2026/03/28/news.jsonl" else None

        resolved = resolve_explicit_or_latest_path(
            FakeClient(),
            explicit_path="/news/raw/2026/03/28/news.jsonl",
            fallback_path="/news/raw",
            latest_resolver=lambda _client, _path: "/news/raw/latest.jsonl",
        )

        self.assertEqual(resolved, "/news/raw/2026/03/28/news.jsonl")

    def test_resolve_explicit_or_latest_path_uses_latest_resolver_when_explicit_missing(self) -> None:
        class FakeClient:
            @staticmethod
            def status(path: str, strict: bool = False):  # noqa: ARG004
                return None

        resolved = resolve_explicit_or_latest_path(
            FakeClient(),
            explicit_path="",
            fallback_path="/news/raw",
            latest_resolver=lambda _client, _path: "/news/raw/latest.jsonl",
        )

        self.assertEqual(resolved, "/news/raw/latest.jsonl")


if __name__ == "__main__":
    unittest.main()
