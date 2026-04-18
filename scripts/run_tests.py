from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path

REQUIRED_TEST_MODULES = ("pandas", "psycopg2")


def discover_start_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "tests"


def find_missing_test_dependencies() -> list[str]:
    missing: list[str] = []

    for module_name in REQUIRED_TEST_MODULES:
        try:
            importlib.import_module(module_name)
        except ModuleNotFoundError:
            missing.append(module_name)

    return missing


def main() -> int:
    missing = find_missing_test_dependencies()
    if missing:
        missing_list = ", ".join(missing)
        print(
            "Missing test dependencies: "
            f"{missing_list}. Install them with: "
            "python -m pip install -r requirements.txt -r requirements-dashboard.txt"
        )
        return 1

    suite = unittest.defaultTestLoader.discover(str(discover_start_dir()))
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
