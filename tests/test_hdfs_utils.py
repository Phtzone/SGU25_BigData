import unittest
from pathlib import Path, PurePosixPath
from unittest.mock import patch

from common.hdfs_utils import (
    build_hdfs_uri,
    derive_hdfs_default_fs,
    prepare_spark_input_output_paths,
    prepare_spark_input_path,
    resolve_explicit_or_latest_path,
    resolve_local_staging_root,
    rewrite_webhdfs_redirect,
    should_stage_spark_via_webhdfs,
    stage_hdfs_path_for_spark,
    sync_local_output_directory_to_hdfs,
)


class HdfsUtilsTests(unittest.TestCase):
    def _make_repo_temp_dir(self, name: str) -> Path:
        temp_dir = Path(".tmp") / name
        temp_dir.mkdir(parents=True, exist_ok=True)
        return temp_dir

    def _cleanup_repo_temp_dir(self, path: Path) -> None:
        for child in sorted(path.rglob("*"), reverse=True):
            if child.is_file():
                child.unlink(missing_ok=True)
            elif child.is_dir():
                child.rmdir()
        path.rmdir()

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

    def test_should_stage_spark_via_webhdfs_for_localhost_endpoints(self) -> None:
        self.assertTrue(
            should_stage_spark_via_webhdfs(
                hdfs_url="http://localhost:9870",
                hdfs_default_fs="hdfs://localhost:9000",
            )
        )

    def test_should_not_stage_spark_via_webhdfs_for_container_endpoints(self) -> None:
        self.assertFalse(
            should_stage_spark_via_webhdfs(
                hdfs_url="http://namenode:9870",
                hdfs_default_fs="hdfs://namenode:9000",
            )
        )

    def test_resolve_local_staging_root_defaults_to_repo_tmp_on_windows(self) -> None:
        with patch.dict("os.environ", {"LOCAL_STAGING_ROOT": ""}, clear=False):
            with patch("common.hdfs_utils.os.name", "nt"):
                staging_root = resolve_local_staging_root()

        self.assertEqual(
            staging_root,
            Path.cwd() / ".tmp" / "spark-staging",
        )

    def test_resolve_local_staging_root_defaults_to_system_temp_on_posix(self) -> None:
        with patch.dict("os.environ", {"LOCAL_STAGING_ROOT": ""}, clear=False):
            with patch("common.hdfs_utils.os.name", "posix"):
                with patch("common.hdfs_utils.Path", PurePosixPath):
                    with patch("common.hdfs_utils.tempfile.gettempdir", return_value="/tmp"):
                        staging_root = resolve_local_staging_root()

        self.assertEqual(
            staging_root,
            PurePosixPath("/tmp") / "sgu25-bigdata-spark-staging",
        )

    def test_stage_hdfs_path_for_spark_downloads_single_file(self) -> None:
        class FakeClient:
            @staticmethod
            def status(path: str, strict: bool = False):  # noqa: ARG004
                return {"type": "FILE"} if path == "/news/raw/2026/03/28/news.jsonl" else None

        temp_dir = self._make_repo_temp_dir("test_stage_hdfs_path_for_spark")
        try:
            with patch(
                "common.hdfs_utils.read_hdfs_bytes",
                return_value=b'{"title":"Example"}\n',
            ) as read_mock:
                staged_path = stage_hdfs_path_for_spark(
                    client=FakeClient(),
                    hdfs_path="/news/raw/2026/03/28/news.jsonl",
                    local_dir=str(temp_dir),
                    hdfs_url="http://localhost:9870",
                    hdfs_user="root",
                    redirect_host="localhost",
                )
            self.assertEqual(Path(staged_path).read_text(encoding="utf-8"), '{"title":"Example"}\n')
            read_mock.assert_called_once_with(
                hdfs_url="http://localhost:9870",
                hdfs_user="root",
                path="/news/raw/2026/03/28/news.jsonl",
                redirect_host="localhost",
            )
        finally:
            self._cleanup_repo_temp_dir(temp_dir)

    def test_sync_local_output_directory_to_hdfs_replaces_existing_target(self) -> None:
        deleted_targets: list[tuple[str, bool]] = []

        class FakeClient:
            @staticmethod
            def status(path: str, strict: bool = False):  # noqa: ARG004
                return {"type": "DIRECTORY"} if path == "/news/processed/2026/03/28/news_081501123456" else None

            @staticmethod
            def delete(path: str, recursive: bool = False):
                deleted_targets.append((path, recursive))

        client = FakeClient()

        temp_dir = self._make_repo_temp_dir("test_sync_local_output_directory_to_hdfs")
        try:
            output_dir = temp_dir / "news_081501123456"
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / "_SUCCESS").write_text("", encoding="utf-8")

            with patch("common.hdfs_utils.upload_directory_to_hdfs") as upload_mock:
                sync_local_output_directory_to_hdfs(
                    client=client,
                    local_dir=str(output_dir),
                    hdfs_dir="/news/processed/2026/03/28/news_081501123456",
                    hdfs_url="http://localhost:9870",
                    hdfs_user="root",
                    redirect_host="localhost",
                )
        finally:
            self._cleanup_repo_temp_dir(temp_dir)

        self.assertEqual(
            deleted_targets,
            [("/news/processed/2026/03/28/news_081501123456", True)],
        )
        upload_mock.assert_called_once_with(
            client=client,
            local_dir=str(output_dir),
            hdfs_dir="/news/processed/2026/03/28/news_081501123456",
            hdfs_url="http://localhost:9870",
            hdfs_user="root",
            redirect_host="localhost",
        )

    def test_prepare_spark_input_path_stages_localhost_input(self) -> None:
        class FakeTemporaryDirectory:
            def __init__(self, name: str):
                self.path = HdfsUtilsTests._make_repo_temp_dir(self, name)

            def __enter__(self):
                return str(self.path)

            def __exit__(self, exc_type, exc, tb):
                HdfsUtilsTests._cleanup_repo_temp_dir(self, self.path)
                return False

        with patch(
            "common.hdfs_utils.temporary_local_staging_dir",
            side_effect=[FakeTemporaryDirectory("test_prepare_spark_input_path")],
        ):
            with patch(
                "common.hdfs_utils.stage_hdfs_path_for_spark",
                return_value=".tmp/test_prepare_spark_input_path/news.jsonl",
            ) as stage_mock:
                with prepare_spark_input_path(
                    client=object(),
                    input_path="/news/raw/2026/03/28/news.jsonl",
                    hdfs_url="http://localhost:9870",
                    hdfs_default_fs="hdfs://localhost:9000",
                    hdfs_user="root",
                    redirect_host="localhost",
                ) as runtime_input:
                    self.assertEqual(runtime_input, ".tmp/test_prepare_spark_input_path/news.jsonl")

        stage_mock.assert_called_once()

    def test_prepare_spark_input_output_paths_syncs_local_output_after_success(self) -> None:
        class FakeTemporaryDirectory:
            def __init__(self, name: str):
                self.path = HdfsUtilsTests._make_repo_temp_dir(self, name)

            def __enter__(self):
                return str(self.path)

            def __exit__(self, exc_type, exc, tb):
                HdfsUtilsTests._cleanup_repo_temp_dir(self, self.path)
                return False

        with patch(
            "common.hdfs_utils.temporary_local_staging_dir",
            side_effect=[
                FakeTemporaryDirectory("test_prepare_spark_io_input"),
                FakeTemporaryDirectory("test_prepare_spark_io_output"),
            ],
        ):
            with patch(
                "common.hdfs_utils.stage_hdfs_path_for_spark",
                return_value=".tmp/test_prepare_spark_io_input/news.jsonl",
            ) as stage_mock:
                with patch("common.hdfs_utils.sync_local_output_directory_to_hdfs") as sync_mock:
                    with prepare_spark_input_output_paths(
                        client=object(),
                        input_path="/news/raw/2026/03/28/news.jsonl",
                        output_path="/news/processed/2026/03/28/news_081501123456",
                        hdfs_url="http://localhost:9870",
                        hdfs_default_fs="hdfs://localhost:9000",
                        hdfs_user="root",
                        redirect_host="localhost",
                    ) as (runtime_input, runtime_output):
                        self.assertEqual(runtime_input, ".tmp/test_prepare_spark_io_input/news.jsonl")
                        self.assertTrue(runtime_output.endswith("news_081501123456"))

        stage_mock.assert_called_once()
        sync_mock.assert_called_once()

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
