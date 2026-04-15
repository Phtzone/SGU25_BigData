import unittest
from datetime import date
from unittest.mock import patch

import scripts.load_curated_to_postgres as curated_loader
from scripts.load_curated_to_postgres import resolve_curated_batch_from_parquet, resolve_latest_curated_batch


class LoadCuratedToPostgresTests(unittest.TestCase):
    def test_resolve_curated_batch_from_partitioned_parquet_path(self) -> None:
        batch_path = resolve_curated_batch_from_parquet(
            "/news/curated/2026/04/04/news_120000000000/event_date=2026-04-04/source=VNExpress/part-0000.parquet"
        )
        self.assertEqual(batch_path, "/news/curated/2026/04/04/news_120000000000")

    def test_resolve_curated_batch_from_non_partitioned_parquet_path(self) -> None:
        batch_path = resolve_curated_batch_from_parquet("/news/curated/2026/04/04/news_120000000000/part-0000.parquet")
        self.assertEqual(batch_path, "/news/curated/2026/04/04/news_120000000000")

    def test_resolve_latest_curated_batch_uses_latest_parquet_file(self) -> None:
        class FakeClient:
            @staticmethod
            def status(path: str, strict: bool = False):  # noqa: ARG004
                return {"type": "DIRECTORY"}

        with patch(
            "common.pipeline_paths.list_hdfs_files",
            return_value=[
                (
                    "/news/curated/2026/04/03/news_100000000000/event_date=2026-04-03/source=VNExpress/part-0000.parquet",
                    {"modificationTime": 100},
                ),
                (
                    "/news/curated/2026/04/04/news_120000000000/event_date=2026-04-04/source=VTV/part-0000.parquet",
                    {"modificationTime": 200},
                ),
            ],
        ):
            batch_path = resolve_latest_curated_batch(FakeClient(), "/news/curated")

        self.assertEqual(batch_path, "/news/curated/2026/04/04/news_120000000000")

    def test_build_curated_batch_fingerprint_uses_sorted_file_metadata(self) -> None:
        self.assertTrue(
            hasattr(curated_loader, "build_curated_batch_fingerprint"),
            "build_curated_batch_fingerprint should exist",
        )
        fingerprint = curated_loader.build_curated_batch_fingerprint(  # type: ignore[attr-defined]
            [
                (
                    "/news/curated/2026/04/04/news_120000000000/part-0001.parquet",
                    {"length": 20, "modificationTime": 200},
                ),
                (
                    "/news/curated/2026/04/04/news_120000000000/part-0000.parquet",
                    {"length": 10, "modificationTime": 100},
                ),
            ],
            batch_path="/news/curated/2026/04/04/news_120000000000",
        )

        self.assertTrue(fingerprint)

    def test_should_skip_curated_batch_load_only_when_path_and_fingerprint_match(self) -> None:
        self.assertTrue(
            hasattr(curated_loader, "should_skip_curated_batch_load"),
            "should_skip_curated_batch_load should exist",
        )
        should_skip_curated_batch_load = curated_loader.should_skip_curated_batch_load  # type: ignore[attr-defined]

        self.assertTrue(
            should_skip_curated_batch_load(
                loaded_batch_metadata={"batch_path": "/news/curated/a", "batch_fingerprint": "abc123"},
                batch_path="/news/curated/a",
                batch_fingerprint="abc123",
            )
        )
        self.assertFalse(
            should_skip_curated_batch_load(
                loaded_batch_metadata={"batch_path": "/news/curated/a", "batch_fingerprint": "abc123"},
                batch_path="/news/curated/a",
                batch_fingerprint="zzz999",
            )
        )

    def test_iter_dataframe_chunks_splits_rows_by_chunk_size(self) -> None:
        self.assertTrue(
            hasattr(curated_loader, "iter_dataframe_chunks"),
            "iter_dataframe_chunks should exist",
        )

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

        chunks = list(curated_loader.iter_dataframe_chunks(FakeDataFrame(), 2))  # type: ignore[attr-defined]
        self.assertEqual(chunks, [[{"id": 1}, {"id": 2}], [{"id": 3}]])

    def test_delete_existing_ods_batch_rows_returns_deleted_event_dates(self) -> None:
        self.assertTrue(
            hasattr(curated_loader, "delete_existing_ods_batch_rows"),
            "delete_existing_ods_batch_rows should exist",
        )

        executed_params = []

        class FakeCursor:
            def execute(self, query, params=None):
                executed_params.append(params)

            @staticmethod
            def fetchall():
                return [(date(2026, 4, 4),), (date(2026, 4, 5),)]

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeConnection:
            @staticmethod
            def cursor():
                return FakeCursor()

        deleted_event_dates = curated_loader.delete_existing_ods_batch_rows(  # type: ignore[attr-defined]
            connection=FakeConnection(),
            ods_table="ods_news_articles",
            batch_path="/news/curated/2026/04/04/news_120000000000",
        )

        self.assertEqual(deleted_event_dates, [date(2026, 4, 4), date(2026, 4, 5)])
        self.assertEqual(
            executed_params,
            [("/news/curated/2026/04/04/news_120000000000",)],
        )

    def test_ensure_analytics_tables_adds_batch_path_column_to_ods_table(self) -> None:
        executed_queries = []

        class FakeCursor:
            def execute(self, query, params=None):  # noqa: ARG002
                executed_queries.append(str(query))

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeConnection:
            committed = False

            @staticmethod
            def cursor():
                return FakeCursor()

            @classmethod
            def commit(cls):
                cls.committed = True

        curated_loader.ensure_analytics_tables(
            connection=FakeConnection(),
            history_table="analytics_load_history",
            ods_table="ods_news_articles",
            mart_table="mart_news_daily_source",
        )

        joined_queries = "\n".join(executed_queries)
        self.assertIn("batch_path TEXT", joined_queries)
        self.assertTrue(FakeConnection.committed)


if __name__ == "__main__":
    unittest.main()
