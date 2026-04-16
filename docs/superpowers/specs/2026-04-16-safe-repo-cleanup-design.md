# Safe Repo Cleanup Design

## Goal

Reduce obvious repository noise and duplication without changing the current news pipeline's runtime behavior or removing useful developer tooling.

The cleanup should keep the project easier to read for submission and maintenance while preserving the existing pipeline, dashboard, validators, loaders, and debugging utilities.

## Approved Direction

This cleanup follows the "gọn nhưng giữ đồ nghề" direction:

- remove files that are clearly unused or process-only artifacts
- keep developer-facing utilities that still help inspect or demo the pipeline
- reduce duplication in shell scripts where the duplication is obvious and low risk
- simplify the README so the core pipeline stands out and optional tooling is easier to scan

## Recommended Approach

Use a balanced cleanup that combines safe file removal with a small refactor of duplicated shell logic and a documentation pass.

This keeps the work meaningful without taking on risky architectural churn. The pipeline code, dashboard behavior, SQL schema, DAG order, and Spark/Kafka logic should remain unchanged.

## In Scope

### 1. Remove clearly unused or process-only files

Safe removal candidates:

- `Spark_jobs/Demo.py`
- historical AI planning artifacts under `docs/superpowers/plans/`
- historical AI spec artifacts under `docs/superpowers/specs/`

To avoid deleting the workflow artifacts currently needed for this session, the cleanup should remove only pre-existing historical files under `docs/superpowers/` and leave the current in-session spec/plan untouched until the task is finished.

### 2. Keep useful developer tools

These should stay:

- `scripts/preview_hdfs_data.py`
- `scripts/export_keyword_review_sample.py`
- dashboard refresh support in `dashboard/airflow_client.py` and `dashboard/refresh_state.py`
- validators, loaders, Airflow startup helpers, and test scripts

They are not part of the strict runtime pipeline, but they still serve real development, debugging, and demo workflows.

### 3. Remove shell duplication conservatively

There is visible duplication between:

- `scripts/test_pipeline.sh`
- `scripts/demo_end_to_end.sh`

The cleanup should extract only the repeated shell helpers into a shared script, or otherwise centralize the shared setup in a minimal and readable way.

The two public entrypoints should remain intact:

- `scripts/test_pipeline.sh` stays focused on smoke validation
- `scripts/demo_end_to_end.sh` stays focused on full demo plus analytics loading

### 4. Reframe the README

The README should be reorganized so that:

- the core pipeline path is shown first
- optional Airflow, dashboard, analytics, and helper scripts are clearly separated
- the main setup and run path is shorter and easier to follow

This is a documentation cleanup, not a product-scope reduction.

## Out Of Scope

- removing the dashboard entirely
- removing Airflow refresh support
- changing DAG task order
- changing PostgreSQL schema or analytics views
- changing Spark, Kafka, HDFS, or validation behavior
- renaming broad module boundaries in Python
- deleting user-owned local directories outside the tracked repo
- removing submission-adjacent notes such as `docs/report_notes.md`

## File Impact

Expected removals:

- `Spark_jobs/Demo.py`
- selected historical files under `docs/superpowers/plans/`
- selected historical files under `docs/superpowers/specs/`

Expected modifications:

- `README.md`
- `scripts/test_pipeline.sh`
- `scripts/demo_end_to_end.sh`

Expected additions:

- one shared shell helper file under `scripts/` for duplicated setup or utility logic

## Risks

### Risk 1: Breaking shell scripts while deduplicating

The two scripts overlap, but they are not identical. If too much logic is merged, one script may silently stop serving its distinct purpose.

Mitigation:

- extract only clearly shared helpers
- keep the main command flow in each script readable and local
- verify both scripts still reference the same public commands as before

### Risk 2: Removing files that still matter to the user

Some docs may be runtime-irrelevant but still useful for submission or personal workflow.

Mitigation:

- limit removals to files that are clearly historical or experimental
- avoid deleting current-session planning artifacts during execution
- call out any optional doc removals explicitly in the final summary

### Risk 3: README cleanup accidentally hides useful commands

A shorter README can become incomplete if optional commands disappear entirely.

Mitigation:

- preserve all meaningful commands
- move optional flows into dedicated sections instead of deleting them

## Verification Strategy

After cleanup, verify:

- the unit test suite still passes
- Python modules still compile cleanly
- the edited shell scripts still contain the expected pipeline flow
- the README still documents the retained scripts and optional tooling accurately

## Success Criteria

- the repo no longer contains obviously unused experimental files
- historical AI planning/spec noise is reduced
- duplicated shell helper logic is smaller and easier to maintain
- the README is easier to scan
- runtime behavior of the pipeline is unchanged
