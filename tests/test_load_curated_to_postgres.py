import unittest
from unittest.mock import patch

from scripts.load_curated_to_postgres import (
    resolve_curated_batch_from_parquet,
    resolve_latest_curated_batch,
)


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
            "scripts.load_curated_to_postgres.list_hdfs_files",
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


if __name__ == "__main__":
    unittest.main()
