import unittest
from datetime import date
from unittest.mock import patch

from scripts.load_keywords_to_postgres import (
    _ensure_date,
    iter_dataframe_chunks,
    read_keyword_batch_metadata,
    reset_streamlit_keyword_views,
)


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

        with patch(
            "scripts.load_keywords_to_postgres.read_hdfs_bytes",
            return_value=(
                b'{\n'
                b'  "batch_path": "/news/keywords/2026/04/08/news_120000000000",\n'
                b'  "keyword_output_path": "/news/keywords/2026/04/08/news_120000000000",\n'
                b'  "keyword_score_version": "v2",\n'
                b'  "keyword_config_hash": "abc12345"\n'
                b'}'
            ),
        ) as read_mock:
            metadata = read_keyword_batch_metadata(
                hdfs_client=FakeClient(),
                batch_path="/news/keywords/2026/04/08/news_120000000000",
                hdfs_url="http://namenode:9870",
                hdfs_user="root",
                redirect_host="datanode",
            )

        self.assertEqual(metadata["keyword_score_version"], "v2")
        self.assertEqual(metadata["keyword_config_hash"], "abc12345")
        read_mock.assert_called_once_with(
            hdfs_url="http://namenode:9870",
            hdfs_user="root",
            path="/news/keywords/2026/04/08/news_120000000000/_keyword_metadata.json",
            redirect_host="datanode",
        )

    def test_reset_streamlit_keyword_views_drops_views_in_dependency_order(self) -> None:
        executed_queries: list[str] = []

        class FakeCursor:
            def execute(self, query, params=None):  # noqa: ARG002
                executed_queries.append(str(query).strip())

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeConnection:
            @staticmethod
            def cursor():
                return FakeCursor()

        reset_streamlit_keyword_views(FakeConnection())

        self.assertEqual(
            executed_queries,
            [
                "DROP VIEW IF EXISTS vw_streamlit_keyword_daily_overall_latest",
                "DROP VIEW IF EXISTS vw_streamlit_keyword_daily_source_latest",
                "DROP VIEW IF EXISTS vw_streamlit_article_keywords_latest",
            ],
        )


if __name__ == "__main__":
    unittest.main()
