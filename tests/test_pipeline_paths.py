import unittest
from pathlib import Path
from unittest.mock import patch

from common.pipeline_paths import (
    resolve_batch_from_parquet_path,
    resolve_latest_parquet_batch,
    write_output_path_file,
)


class PipelinePathTests(unittest.TestCase):
    def test_write_output_path_file_writes_trailing_newline(self) -> None:
        workspace_temp_dir = Path(".tmp") / "pipeline-path-tests"
        path_file = workspace_temp_dir / "artifacts" / "path.txt"
        try:
            write_output_path_file(str(path_file), "/news/raw/2026/04/15/news_120000.jsonl")

            self.assertEqual(
                path_file.read_text(encoding="utf-8"),
                "/news/raw/2026/04/15/news_120000.jsonl\n",
            )
        finally:
            path_file.unlink(missing_ok=True)
            path_file.parent.rmdir()
            workspace_temp_dir.rmdir()

    def test_resolve_batch_from_parquet_path_uses_partition_prefix_when_present(self) -> None:
        batch_path = resolve_batch_from_parquet_path(
            "/news/curated/2026/04/04/news_120000000000/event_date=2026-04-04/source=VNExpress/part-0000.parquet",
            partition_prefixes=("event_date=",),
            parents_up_if_unpartitioned=1,
        )

        self.assertEqual(batch_path, "/news/curated/2026/04/04/news_120000000000")

    def test_resolve_batch_from_parquet_path_uses_parent_depth_when_unpartitioned(self) -> None:
        batch_path = resolve_batch_from_parquet_path(
            "/news/keywords/2026/04/07/news_120000000000/article_keywords/part-0000.parquet",
            parents_up_if_unpartitioned=2,
        )

        self.assertEqual(batch_path, "/news/keywords/2026/04/07/news_120000000000")

    def test_resolve_latest_parquet_batch_uses_explicit_parquet_file(self) -> None:
        class FakeClient:
            @staticmethod
            def status(path: str, strict: bool = False):  # noqa: ARG004
                return {"type": "FILE"}

        batch_path = resolve_latest_parquet_batch(
            FakeClient(),
            "/news/keywords/2026/04/07/news_120000000000/article_keywords/part-0000.parquet",
            batch_from_parquet=lambda parquet_path: resolve_batch_from_parquet_path(
                parquet_path,
                parents_up_if_unpartitioned=2,
            ),
            missing_status_message="HDFS path does not exist: {path}",
            missing_parquet_message="No keyword Parquet files found under {path}",
        )

        self.assertEqual(batch_path, "/news/keywords/2026/04/07/news_120000000000")

    def test_resolve_latest_parquet_batch_uses_latest_listing_entry(self) -> None:
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
            batch_path = resolve_latest_parquet_batch(
                FakeClient(),
                "/news/curated",
                batch_from_parquet=lambda parquet_path: resolve_batch_from_parquet_path(
                    parquet_path,
                    partition_prefixes=("event_date=",),
                    parents_up_if_unpartitioned=1,
                ),
                missing_status_message="HDFS path does not exist: {path}",
                missing_parquet_message="No curated Parquet files found under {path}",
            )

        self.assertEqual(batch_path, "/news/curated/2026/04/04/news_120000000000")


if __name__ == "__main__":
    unittest.main()
