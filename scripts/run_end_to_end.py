from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


def resolve_demo_script_path() -> Path:
    return Path(__file__).resolve().with_name("demo_end_to_end.sh")


def main() -> int:
    bash_path = shutil.which("bash")
    if not bash_path:
        print(
            "bash is required to run the full end-to-end demo. "
            "Install Git Bash or run the Windows wrapper at scripts/demo_end_to_end.ps1."
        )
        return 1

    demo_script_path = resolve_demo_script_path()
    if not demo_script_path.is_file():
        print(f"Cannot find end-to-end demo script: {demo_script_path}")
        return 1

    completed = subprocess.run(
        [bash_path, str(demo_script_path)],
        cwd=str(demo_script_path.parent.parent),
        check=False,
    )
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
