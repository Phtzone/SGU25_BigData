import unittest
from datetime import date
from io import StringIO

from scripts.load_keywords_to_postgres import _ensure_date, iter_dataframe_chunks, read_keyword_batch_metadata


class KeywordLoaderTests(unittest.TestCase):
    def test_ensure_date_accepts_date(self) -> None:
        value = date(2026, 4, 7)
        self.assertEqual(_ensure_date(value), value)

    def test_ensure_date_rejects_non_date(self) -> None:
        with self.assertRaises(ValueError):
            _ensure_date("2026-04-07")

    def test_iter_dataframe_chunks_splits_rows_by_chunk_size(self) -> None:
        class FakeRow:
            def __init__(self, payload):
                self.payload = payload

            def asDict(self, recursive: bool = True):  # noqa: ARG002
                return self.payload

        class FakeDataFrame:
            @staticmethod
            def toLocalIterator():
                return iter(
                    [
                        FakeRow({"id": 1}),
                        FakeRow({"id": 2}),
                        FakeRow({"id": 3}),
                    ]
                )

        chunks = list(iter_dataframe_chunks(FakeDataFrame(), 2))

        self.assertEqual(chunks, [[{"id": 1}, {"id": 2}], [{"id": 3}]])

    def test_read_keyword_batch_metadata_reads_required_fields(self) -> None:
        class FakeClient:
            @staticmethod
            def status(path: str, strict: bool = False):  # noqa: ARG004
                return {"type": "FILE"}

            @staticmethod
            def read(path: str, encoding: str = "utf-8"):  # noqa: ARG004
                return StringIO(
                    """
                    {
                      "batch_path": "/news/keywords/2026/04/08/news_120000000000",
                      "keyword_score_version": "v2",
                      "keyword_config_hash": "abc12345"
                    }
                    """
                )

        metadata = read_keyword_batch_metadata(
            hdfs_client=FakeClient(),
            batch_path="/news/keywords/2026/04/08/news_120000000000",
        )

        self.assertEqual(metadata["keyword_score_version"], "v2")
        self.assertEqual(metadata["keyword_config_hash"], "abc12345")


if __name__ == "__main__":
    unittest.main()
