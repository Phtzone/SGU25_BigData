import unittest
from unittest.mock import patch

from scripts.validate_hdfs_output import resolve_raw_files


class ValidateHdfsOutputTests(unittest.TestCase):
    def test_resolve_raw_files_accepts_file_path(self) -> None:
        class FakeClient:
            @staticmethod
            def status(path: str, strict: bool = False):  # noqa: ARG004
                if path == "/news/raw/2026/04/07/news_124043106401.jsonl":
                    return {"type": "FILE", "modificationTime": 123}
                return None

        files = resolve_raw_files(FakeClient(), "/news/raw/2026/04/07/news_124043106401.jsonl")

        self.assertEqual(
            files,
            [("/news/raw/2026/04/07/news_124043106401.jsonl", {"type": "FILE", "modificationTime": 123})],
        )

    def test_resolve_raw_files_uses_recursive_listing_for_directory(self) -> None:
        class FakeClient:
            @staticmethod
            def status(path: str, strict: bool = False):  # noqa: ARG004
                return {"type": "DIRECTORY"} if path == "/news/raw" else None

        with patch(
            "scripts.validate_hdfs_output.list_hdfs_files",
            return_value=[
                ("/news/raw/2026/04/07/news_1.jsonl", {"type": "FILE", "modificationTime": 100}),
                ("/news/raw/2026/04/07/news_2.jsonl", {"type": "FILE", "modificationTime": 200}),
            ],
        ):
            files = resolve_raw_files(FakeClient(), "/news/raw")

        self.assertEqual(len(files), 2)
        self.assertEqual(files[1][0], "/news/raw/2026/04/07/news_2.jsonl")


if __name__ == "__main__":
    unittest.main()
