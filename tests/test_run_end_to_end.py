import io
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from scripts import run_end_to_end


class RunEndToEndScriptTests(unittest.TestCase):
    def test_resolve_demo_script_path_points_to_shell_script(self) -> None:
        script_path = run_end_to_end.resolve_demo_script_path()

        self.assertEqual(script_path.name, "demo_end_to_end.sh")
        self.assertEqual(script_path.parent.name, "scripts")
        self.assertTrue(script_path.is_absolute())

    def test_main_returns_error_when_bash_is_missing(self) -> None:
        output = io.StringIO()

        with patch("scripts.run_end_to_end.shutil.which", return_value=None):
            with patch("sys.stdout", output):
                exit_code = run_end_to_end.main()

        self.assertEqual(exit_code, 1)
        self.assertIn("bash is required", output.getvalue().lower())
        self.assertIn("demo_end_to_end.ps1", output.getvalue())

    def test_main_runs_demo_script_with_bash(self) -> None:
        completed_process = Mock(returncode=0)

        with patch("scripts.run_end_to_end.shutil.which", return_value="C:/Program Files/Git/bin/bash.exe"):
            with patch("scripts.run_end_to_end.subprocess.run", return_value=completed_process) as run_mock:
                exit_code = run_end_to_end.main()

        self.assertEqual(exit_code, 0)
        expected_script = run_end_to_end.resolve_demo_script_path()
        expected_cwd = expected_script.parent.parent
        run_mock.assert_called_once_with(
            ["C:/Program Files/Git/bin/bash.exe", str(expected_script)],
            cwd=str(expected_cwd),
            check=False,
        )

    def test_main_returns_error_when_demo_script_is_missing(self) -> None:
        output = io.StringIO()
        fake_script_path = Path("D:/missing/demo_end_to_end.sh")

        with patch("scripts.run_end_to_end.resolve_demo_script_path", return_value=fake_script_path):
            with patch("scripts.run_end_to_end.shutil.which", return_value="/usr/bin/bash"):
                with patch("sys.stdout", output):
                    exit_code = run_end_to_end.main()

        self.assertEqual(exit_code, 1)
        self.assertIn("cannot find", output.getvalue().lower())
        self.assertIn(str(fake_script_path), output.getvalue())


if __name__ == "__main__":
    unittest.main()
