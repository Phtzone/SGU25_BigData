import unittest
from pathlib import Path
from unittest.mock import patch

from Spark_jobs.transform_news_raw_to_processed import (
    choose_writable_spark_dir,
    ensure_java_home,
    infer_java_home_from_common_locations,
    infer_java_home_from_path,
    is_valid_java_home,
)


class SparkEnvTests(unittest.TestCase):
    @staticmethod
    def _matches_default_java_home(path_value: object) -> bool:
        return str(path_value).replace("\\", "/").endswith("/default-java")

    def test_ensure_java_home_returns_existing_configured_path(self) -> None:
        with patch.dict("os.environ", {"JAVA_HOME": "/fake/java-home"}, clear=False):
            with patch.object(Path, "exists", return_value=True):
                java_home = ensure_java_home()

        self.assertEqual(Path(java_home), Path("/fake/java-home"))

    def test_ensure_java_home_infers_from_java_binary_in_path(self) -> None:
        with patch.dict("os.environ", {"JAVA_HOME": ""}, clear=False):
            with patch("shutil.which", return_value="/usr/bin/java"):
                with patch.object(Path, "resolve", return_value=Path("/usr/lib/jvm/java-17-openjdk-amd64/bin/java")):
                    with patch.object(Path, "exists", return_value=True):
                        java_home = ensure_java_home()

        self.assertEqual(Path(java_home), Path("/usr/lib/jvm/java-17-openjdk-amd64"))

    def test_ensure_java_home_falls_back_when_configured_path_is_invalid(self) -> None:
        with patch.dict("os.environ", {"JAVA_HOME": "/bad/java-home"}, clear=False):
            with patch("shutil.which", return_value="/usr/bin/java"):
                with patch.object(Path, "resolve", return_value=Path("/usr/lib/jvm/default-java/bin/java")):
                    with patch.object(Path, "exists", side_effect=[False, True]):
                        java_home = ensure_java_home()

        self.assertEqual(Path(java_home), Path("/usr/lib/jvm/default-java"))

    def test_infer_java_home_from_path_returns_none_when_binary_missing(self) -> None:
        with patch("shutil.which", return_value="/usr/bin/java"):
            with patch.object(Path, "resolve", return_value=Path("/usr/lib/jvm/default-java/bin/java")):
                with patch.object(Path, "exists", return_value=False):
                    self.assertIsNone(infer_java_home_from_path())

    def test_infer_java_home_from_common_locations_uses_known_candidate(self) -> None:
        with patch(
            "Spark_jobs.transform_news_raw_to_processed.is_valid_java_home",
            side_effect=self._matches_default_java_home,
        ):
            java_home = infer_java_home_from_common_locations()

        self.assertEqual(Path(java_home), Path("/usr/lib/jvm/default-java"))

    def test_ensure_java_home_uses_common_locations_when_path_is_missing(self) -> None:
        with patch.dict("os.environ", {"JAVA_HOME": "/bad/java-home"}, clear=False):
            with patch("shutil.which", return_value=None):
                with patch(
                    "Spark_jobs.transform_news_raw_to_processed.is_valid_java_home",
                    side_effect=self._matches_default_java_home,
                ):
                    java_home = ensure_java_home()

        self.assertEqual(Path(java_home), Path("/usr/lib/jvm/default-java"))

    def test_is_valid_java_home_checks_bin_java(self) -> None:
        with patch.object(Path, "exists", return_value=True):
            self.assertTrue(is_valid_java_home("/usr/lib/jvm/default-java"))

    def test_ensure_java_home_raises_when_no_java_available(self) -> None:
        with patch.dict("os.environ", {"JAVA_HOME": ""}, clear=False):
            with patch("shutil.which", return_value=None):
                with self.assertRaises(SystemExit) as error:
                    ensure_java_home()

        self.assertIn("Java 17 is required", str(error.exception))

    def test_choose_writable_spark_dir_falls_back_when_configured_dir_is_not_writable(self) -> None:
        with patch("tempfile.mkdtemp", return_value="/tmp/spark-local-fallback"):
            with patch.object(
                Path,
                "mkdir",
                side_effect=[PermissionError("denied"), None],
            ):
                path = choose_writable_spark_dir(
                    env_var_name="SPARK_LOCAL_DIR",
                    default_dir=Path("/tmp/spark-local"),
                    fallback_prefix="spark-local-fallback-",
                )

        self.assertEqual(path, Path("/tmp/spark-local-fallback"))


if __name__ == "__main__":
    unittest.main()
