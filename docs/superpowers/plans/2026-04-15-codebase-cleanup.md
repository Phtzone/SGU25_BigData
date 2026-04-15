# Codebase Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce obvious repo and code maintenance overhead without changing the news pipeline's behavior.

**Architecture:** Add a shared helper module under `common/` for repeated pipeline path logic, keep existing entrypoints stable through thin wrappers where needed, and trim dashboard packaging to its real dependency boundary. Repo hygiene changes stay non-destructive by relying on ignore rules instead of deleting local user content.

**Tech Stack:** Python 3.12, PySpark, HDFS client helpers, Streamlit, Docker

---

### Task 1: Lock Cleanup Scope In Docs

**Files:**
- Create: `docs/superpowers/specs/2026-04-15-codebase-cleanup-design.md`
- Create: `docs/superpowers/plans/2026-04-15-codebase-cleanup.md`

- [ ] Record the approved cleanup scope and non-goals.
- [ ] Keep the cleanup focused on repo noise, shared helpers, and dashboard packaging.

### Task 2: Add Shared Helper Tests First

**Files:**
- Create: `tests/test_pipeline_paths.py`

- [ ] Add tests for writing output artifact paths.
- [ ] Add tests for resolving batch roots from partitioned and non-partitioned Parquet paths.
- [ ] Add tests for resolving the latest Parquet-backed batch from HDFS listings.
- [ ] Run the new test file and verify it fails before implementation.

### Task 3: Introduce Shared Pipeline Path Helpers

**Files:**
- Create: `common/pipeline_paths.py`
- Modify: `Spark_jobs/transform_news_raw_to_processed.py`
- Modify: `Spark_jobs/curate_news_processed_to_curated.py`
- Modify: `Spark_jobs/extract_news_keywords.py`
- Modify: `scripts/load_curated_to_postgres.py`
- Modify: `scripts/validate_keyword_output.py`

- [ ] Implement shared helpers in `common/pipeline_paths.py`.
- [ ] Replace repeated local helper implementations with imports or thin wrappers.
- [ ] Preserve current redirect-aware metadata behavior in touched scripts.
- [ ] Re-run focused helper-related tests.

### Task 4: Trim Dashboard Packaging And Repo Noise

**Files:**
- Modify: `.gitignore`
- Modify: `requirements-dashboard.txt`
- Modify: `requirements.txt`
- Modify: `Dockerfile.streamlit`

- [ ] Ignore non-product local directories without deleting them.
- [ ] Make dashboard dependencies explicit, including `pandas`.
- [ ] Remove full pipeline dependency installation from the dashboard image.

### Task 5: Verify End State

**Files:**
- Verify only

- [ ] Run the full unit suite.
- [ ] Review the final diff for accidental behavior changes in dirty files.
- [ ] Summarize what was cleaned up and what was intentionally left untouched.
