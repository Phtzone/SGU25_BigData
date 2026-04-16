# Safe Repo Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove clearly unused repo artifacts, reduce shell-script duplication, and make the README easier to scan without changing the news pipeline's runtime behavior.

**Architecture:** Keep the pipeline topology, public script entrypoints, dashboard, and analytics flow intact. Limit code changes to safe file removals, extraction of shared shell helpers into a single sourced script, and a README reorganization that separates core flow from optional tooling.

**Tech Stack:** Bash, Markdown, Python 3.12, unittest, compileall, git

---

## File Map

- Delete: `Spark_jobs/Demo.py`
- Delete: `docs/superpowers/plans/2026-04-14-async-dashboard-refresh.md`
- Delete: `docs/superpowers/plans/2026-04-15-codebase-cleanup.md`
- Delete: `docs/superpowers/plans/2026-04-15-submission-readiness-fixes.md`
- Delete: `docs/superpowers/specs/2026-04-14-async-dashboard-refresh-design.md`
- Delete: `docs/superpowers/specs/2026-04-15-codebase-cleanup-design.md`
- Delete: `docs/superpowers/specs/2026-04-15-submission-readiness-fixes-design.md`
- Create: `scripts/_pipeline_common.sh`
  - Hold the shared shell helpers currently duplicated in both pipeline scripts.
- Modify: `scripts/test_pipeline.sh`
  - Source shared helpers and keep the smoke-test flow intact.
- Modify: `scripts/demo_end_to_end.sh`
  - Source shared helpers and keep the full demo plus analytics-load flow intact.
- Modify: `README.md`
  - Reframe the docs so the core path comes first and optional flows are grouped more clearly.

### Task 1: Remove clearly unused tracked artifacts

**Files:**
- Delete: `Spark_jobs/Demo.py`
- Delete: `docs/superpowers/plans/2026-04-14-async-dashboard-refresh.md`
- Delete: `docs/superpowers/plans/2026-04-15-codebase-cleanup.md`
- Delete: `docs/superpowers/plans/2026-04-15-submission-readiness-fixes.md`
- Delete: `docs/superpowers/specs/2026-04-14-async-dashboard-refresh-design.md`
- Delete: `docs/superpowers/specs/2026-04-15-codebase-cleanup-design.md`
- Delete: `docs/superpowers/specs/2026-04-15-submission-readiness-fixes-design.md`

- [ ] **Step 1: Verify the removal list is not referenced by product code**

Run:

```bash
rg -n "Demo\.py|2026-04-14-async-dashboard-refresh|2026-04-15-codebase-cleanup|2026-04-15-submission-readiness-fixes" README.md docs scripts Spark_jobs dashboard dags tests common producer consumer sql config
```

Expected:

- matches should appear only inside the historical `docs/superpowers` files being removed
- no runtime file should depend on `Spark_jobs/Demo.py`

- [ ] **Step 2: Remove the tracked files**

Run:

```bash
git rm \
  Spark_jobs/Demo.py \
  docs/superpowers/plans/2026-04-14-async-dashboard-refresh.md \
  docs/superpowers/plans/2026-04-15-codebase-cleanup.md \
  docs/superpowers/plans/2026-04-15-submission-readiness-fixes.md \
  docs/superpowers/specs/2026-04-14-async-dashboard-refresh-design.md \
  docs/superpowers/specs/2026-04-15-codebase-cleanup-design.md \
  docs/superpowers/specs/2026-04-15-submission-readiness-fixes-design.md
```

- [ ] **Step 3: Verify the removals are staged and current-session docs remain**

Run:

```bash
git ls-files \
  Spark_jobs/Demo.py \
  docs/superpowers/plans/2026-04-14-async-dashboard-refresh.md \
  docs/superpowers/plans/2026-04-15-codebase-cleanup.md \
  docs/superpowers/plans/2026-04-15-submission-readiness-fixes.md \
  docs/superpowers/specs/2026-04-14-async-dashboard-refresh-design.md \
  docs/superpowers/specs/2026-04-15-codebase-cleanup-design.md \
  docs/superpowers/specs/2026-04-15-submission-readiness-fixes-design.md \
  docs/superpowers/specs/2026-04-16-safe-repo-cleanup-design.md \
  docs/superpowers/plans/2026-04-16-safe-repo-cleanup.md
```

Expected:

- no output for the deleted historical files
- output still includes `docs/superpowers/specs/2026-04-16-safe-repo-cleanup-design.md`
- output still includes `docs/superpowers/plans/2026-04-16-safe-repo-cleanup.md`

- [ ] **Step 4: Commit the safe file removals**

Run:

```bash
git add docs/superpowers/specs/2026-04-16-safe-repo-cleanup-design.md docs/superpowers/plans/2026-04-16-safe-repo-cleanup.md
git commit -m "chore: remove unused cleanup artifacts"
```

### Task 2: Extract shared shell helpers without changing script entrypoints

**Files:**
- Create: `scripts/_pipeline_common.sh`
- Modify: `scripts/test_pipeline.sh`
- Modify: `scripts/demo_end_to_end.sh`

- [ ] **Step 1: Snapshot the duplicated helper blocks before refactoring**

Run:

```bash
rg -n "command -v python|extract_json_field|read_output_path|wait_for_kafka_listener" scripts/test_pipeline.sh scripts/demo_end_to_end.sh
```

Expected:

- both scripts show the duplicated Python resolution block
- both scripts define `extract_json_field`
- both scripts define `read_output_path`
- both scripts define `wait_for_kafka_listener`

- [ ] **Step 2: Add a shared helper script**

Create `scripts/_pipeline_common.sh` with:

```bash
#!/usr/bin/env bash

resolve_python_bin() {
  if command -v python >/dev/null 2>&1; then
    echo "python"
  elif command -v python3 >/dev/null 2>&1; then
    echo "python3"
  else
    echo "Python interpreter not found. Install python3 or activate your virtual environment first." >&2
    return 1
  fi
}

create_artifact_dir() {
  mktemp -d
}

extract_json_field() {
  local field="$1"
  local required="${2:-0}"

  "$PYTHON_BIN" - "$field" "$required" <<'PY'
import json
import sys

field = sys.argv[1]
required = sys.argv[2] == "1"
text = sys.stdin.read()
decoder = json.JSONDecoder()
value_found = False
value = None
index = 0

while index < len(text):
    if text[index] not in "{[":
        index += 1
        continue
    try:
        payload, end = decoder.raw_decode(text, index)
    except json.JSONDecodeError:
        index += 1
        continue
    if isinstance(payload, dict) and field in payload:
        value = payload[field]
        value_found = True
    index = end

if not value_found:
    if required:
        raise SystemExit(f"Could not find JSON field {field!r} in command output")
    raise SystemExit(0)

if value is not None:
    print(value)
PY
}

read_output_path() {
  tr -d '\r\n' < "$1"
}

wait_for_kafka_listener() {
  "$PYTHON_BIN" - <<'PY'
import os
import socket
import time

host = os.getenv("KAFKA_WAIT_HOST", "localhost")
port = int(os.getenv("KAFKA_WAIT_PORT", "9093"))
timeout_seconds = float(os.getenv("KAFKA_STARTUP_TIMEOUT_SECONDS", "90"))
check_interval = float(os.getenv("KAFKA_STARTUP_CHECK_INTERVAL_SECONDS", "3"))
deadline = time.monotonic() + max(timeout_seconds, 0.0)
last_error = None

while time.monotonic() <= deadline:
    try:
        with socket.create_connection((host, port), timeout=5):
            print(f"Kafka listener is ready at {host}:{port}")
            raise SystemExit(0)
    except OSError as exc:
        last_error = exc
        time.sleep(max(check_interval, 0.1))

message = f"Timed out waiting for Kafka listener at {host}:{port}"
if last_error is not None:
    message += f" ({last_error})"
raise SystemExit(message)
PY
}
```

- [ ] **Step 3: Update `scripts/test_pipeline.sh` to source the shared helpers**

Replace the duplicated setup block at the top of `scripts/test_pipeline.sh` with:

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_pipeline_common.sh"

PYTHON_BIN="$(resolve_python_bin)"

artifact_dir="$(create_artifact_dir)"
trap 'rm -rf "$artifact_dir"' EXIT

raw_path_file="$artifact_dir/raw_path.txt"
processed_path_file="$artifact_dir/processed_path.txt"
curated_path_file="$artifact_dir/curated_path.txt"
keyword_path_file="$artifact_dir/keyword_path.txt"
```

Then delete the inline definitions of:

- `extract_json_field`
- `read_output_path`
- `wait_for_kafka_listener`

Leave the script's command flow from `bash scripts/init_kafka_topics.sh` onward unchanged.

- [ ] **Step 4: Update `scripts/demo_end_to_end.sh` to source the shared helpers**

Replace the duplicated setup block at the top of `scripts/demo_end_to_end.sh` with:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_DIR="$ROOT_DIR/scripts"
cd "$ROOT_DIR"

source "$SCRIPT_DIR/_pipeline_common.sh"

PYTHON_BIN="$(resolve_python_bin)"

artifact_dir="$(create_artifact_dir)"
trap 'rm -rf "$artifact_dir"' EXIT

raw_path_file="$artifact_dir/raw_path.txt"
processed_path_file="$artifact_dir/processed_path.txt"
curated_path_file="$artifact_dir/curated_path.txt"
keyword_path_file="$artifact_dir/keyword_path.txt"
```

Then delete the inline definitions of:

- `extract_json_field`
- `read_output_path`
- `wait_for_kafka_listener`

Leave the demo-only flow intact:

- start core Docker services
- start Airflow services
- load curated analytics
- load keyword analytics
- print demo summary output

- [ ] **Step 5: Verify shell syntax and helper ownership**

Run:

```bash
bash -n scripts/_pipeline_common.sh scripts/test_pipeline.sh scripts/demo_end_to_end.sh
rg -n "^(resolve_python_bin|create_artifact_dir|extract_json_field|read_output_path|wait_for_kafka_listener)\(\)" scripts
```

Expected:

- `bash -n` prints nothing and exits successfully
- the helper function definitions are reported only in `scripts/_pipeline_common.sh`

- [ ] **Step 6: Commit the shell cleanup**

Run:

```bash
git add scripts/_pipeline_common.sh scripts/test_pipeline.sh scripts/demo_end_to_end.sh
git commit -m "refactor: deduplicate pipeline shell helpers"
```

### Task 3: Reframe the README around core versus optional flows

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Capture the retained script and feature references before rewriting**

Run:

```bash
rg -n "demo_end_to_end|test_pipeline|preview_hdfs_data|export_keyword_review_sample|start_airflow|Streamlit Dashboard|Airflow Phase" README.md
```

Expected:

- the current README still references all retained scripts and optional features

- [ ] **Step 2: Rewrite the opening sections so the core path comes first**

Update the top of `README.md` so the early structure reads like this:

```md
# Real-Time News Ingestion Pipeline

## Thành viên nhóm

- 3123580051 - Phạm Hoàng Tiến
- 3123580046 - Thạch Ngọc Thảo
- 3123580058 - Nguyễn Thái Tú

This repository implements the core Big Data MVP:

`RSS -> Kafka -> HDFS raw -> Spark processed -> Spark curated -> Spark keywords`

## What Is Core

The core flow is:

- RSS ingestion
- Kafka publish and consume
- HDFS raw storage
- Spark processed and curated jobs
- keyword extraction
- validation scripts for each stage

## What Is Optional

Optional layers on top of the core flow:

- Airflow orchestration
- PostgreSQL analytics loading
- Streamlit dashboard
- debug and export utilities

## Quick Start (Core Pipeline)

1. Create a WSL/Linux virtual environment.
2. Install `requirements.txt`.
3. Start core infrastructure with `docker compose up -d`.
4. Create Kafka topics with `bash scripts/init_kafka_topics.sh`.
5. Run the full smoke path with `bash scripts/test_pipeline.sh`.
```

- [ ] **Step 3: Move utility-only commands into a dedicated optional section**

Add or rewrite a compact section near the helper scripts so it looks like:

```md
## Optional Utilities

- Preview the latest raw HDFS file:
  `python -m scripts.preview_hdfs_data --path /news/raw --limit 5`
- Export keyword review rows from PostgreSQL:
  `python -m scripts.export_keyword_review_sample --limit 100`
- Run the full demo with analytics loading:
  `bash scripts/demo_end_to_end.sh`
```

Keep all retained commands documented somewhere in the README, but stop mixing them into the shortest core setup path.

- [ ] **Step 4: Verify the README still documents all retained tools**

Run:

```bash
rg -n "demo_end_to_end|test_pipeline|preview_hdfs_data|export_keyword_review_sample|start_airflow|streamlit" README.md
```

Expected:

- all retained helper scripts are still documented
- Airflow and dashboard are still present, but no longer dominate the earliest setup flow

- [ ] **Step 5: Commit the README cleanup**

Run:

```bash
git add README.md
git commit -m "docs: clarify core and optional project flows"
```

### Task 4: Run final verification for safe cleanup

**Files:**
- Verify only

- [ ] **Step 1: Run the full unit suite**

Run:

```bash
python -m unittest discover -s tests -p "test_*.py"
```

Expected:

- all tests pass with 0 failures

- [ ] **Step 2: Compile the Python modules to catch syntax regressions**

Run:

```bash
python -m compileall common producer consumer dashboard scripts Spark_jobs dags
```

Expected:

- compileall completes successfully

- [ ] **Step 3: Re-run shell syntax verification**

Run:

```bash
bash -n scripts/_pipeline_common.sh scripts/test_pipeline.sh scripts/demo_end_to_end.sh
```

Expected:

- no output and exit code 0

- [ ] **Step 4: Review the final diff**

Run:

```bash
git status --short
```

Expected:

- only the intended deletions, helper-script changes, README edits, and current-session cleanup docs remain

- [ ] **Step 5: Commit the verified cleanup**

Run:

```bash
git add -A
git commit -m "chore: streamline repo cleanup safely"
```

## Self-Review

Spec coverage:

- safe removal of obviously unused files: covered in Task 1
- conservative shell deduplication with intact entrypoints: covered in Task 2
- README reframing without feature removal: covered in Task 3
- final verification with tests, compileall, and shell syntax checks: covered in Task 4

Placeholder scan:

- no `TODO`, `TBD`, or “implement later” placeholders remain
- each code-changing step includes exact file paths and concrete replacement content
- each verification step includes an exact command and expected outcome

Type consistency:

- shared shell helper file is named `scripts/_pipeline_common.sh` consistently
- helper functions are named consistently across creation and verification steps
- the retained public entrypoints remain `scripts/test_pipeline.sh` and `scripts/demo_end_to_end.sh`
