import unittest

from common.hdfs_utils import rewrite_webhdfs_redirect


class HdfsUtilsTests(unittest.TestCase):
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
