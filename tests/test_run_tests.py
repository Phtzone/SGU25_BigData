import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.run_tests import discover_start_dir, find_missing_test_dependencies


class RunTestsScriptTests(unittest.TestCase):
    def test_discover_start_dir_points_to_repo_tests_directory(self) -> None:
        start_dir = discover_start_dir()

        self.assertEqual(start_dir.name, "tests")
        self.assertEqual(start_dir, Path(__file__).resolve().parent)

    def test_find_missing_test_dependencies_reports_missing_modules(self) -> None:
        with patch("scripts.run_tests.importlib.import_module") as import_module:
            def fake_import(name: str):
                if name == "pandas":
                    raise ModuleNotFoundError("No module named 'pandas'")
                return object()

            import_module.side_effect = fake_import

            missing = find_missing_test_dependencies()

        self.assertEqual(missing, ["pandas"])


if __name__ == "__main__":
    unittest.main()
