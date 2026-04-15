import io
import json
import sys
import types
import unittest
from unittest.mock import patch

import scripts.validate_curated_output as validate_curated_output
import scripts.validate_processed_output as validate_processed_output


class SubmissionReadinessValidatorTests(unittest.TestCase):
    def test_resolve_processed_batch_path_uses_parent_directory(self) -> None:
        self.assertTrue(
            hasattr(validate_processed_output, "resolve_batch_path"),
            "validate_processed_output.resolve_batch_path should exist",
        )
        self.assertEqual(
            validate_processed_output.resolve_batch_path(  # type: ignore[attr-defined]
                "/news/processed/2026/04/04/news_120000000000/part-0000.parquet"
            ),
            "/news/processed/2026/04/04/news_120000000000",
        )

    def test_validate_curated_output_rejects_unpartitioned_batch_layout(self) -> None:
        class FakeClient:
            def __init__(self, *args, **kwargs) -> None:
                pass

            @staticmethod
            def status(path: str, strict: bool = False):  # noqa: ARG004
                return {"type": "DIRECTORY"}

        fake_hdfs = types.SimpleNamespace(InsecureClient=FakeClient)
        files = [
            (
                "/news/curated/2026/04/04/news_120000000000/part-0000.parquet",
                {"modificationTime": 1},
            )
        ]

        with patch.dict(sys.modules, {"hdfs": fake_hdfs}):
            with patch("scripts.validate_curated_output.list_hdfs_files", return_value=files):
                with patch.object(
                    sys,
                    "argv",
                    ["prog", "--path", "/news/curated", "--json"],
                ):
                    with self.assertRaises(SystemExit) as error:
                        validate_curated_output.main()

        self.assertIn("partition", str(error.exception).lower())


if __name__ == "__main__":
    unittest.main()
