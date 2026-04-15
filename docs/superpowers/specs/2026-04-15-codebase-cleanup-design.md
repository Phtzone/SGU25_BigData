# Codebase Cleanup Design

## Goal

Reduce obvious maintenance overhead in the current repository without changing the pipeline's user-facing behavior.

The cleanup focuses on:

- removing non-destructive repo noise
- trimming dashboard-only runtime dependencies
- centralizing repeated pipeline path helpers
- keeping all existing pipeline flows and tests working

## In Scope

- add ignore rules for generated or external helper directories that should not affect the main repo
- slim the dashboard container so it installs only dashboard dependencies
- introduce a shared helper module for repeated batch-path and output-path logic
- update existing call sites to use the shared helper module
- keep current redirect-aware HDFS metadata behavior intact

## Out Of Scope

- deleting user-owned directories from disk
- changing DAG task order
- rewriting Spark jobs for distributed performance
- redesigning PostgreSQL schema management
- changing keyword scoring behavior

## Design Summary

### 1. Repo hygiene stays non-destructive

The cleanup will not delete `awesome-codex-skills/` or temporary directories from disk. Instead, it will make the main repo less noisy by ignoring directories that are clearly outside the product code path or generated during local setup.

### 2. Dashboard runtime becomes explicit

The dashboard image should not install the ingestion and Spark stack. Its dependency list will explicitly include the packages needed by `dashboard/streamlit_app.py`, including `pandas`, and the dashboard Dockerfile will install only that set.

### 3. Repeated pipeline path logic moves to `common/`

Several modules currently repeat:

- "write output path to artifact file"
- "resolve batch root from Parquet path"
- "resolve latest Parquet-backed batch under an HDFS directory"

Those helpers belong in `common/` because they are pipeline mechanics, not job-specific logic.

### 4. Existing public entrypoints stay stable

Job and script modules that already expose resolver helpers will keep those functions as thin wrappers where needed, so tests and call sites do not need a broad rename.

## Risks

- touching files that already have local uncommitted changes
- accidentally changing keyword metadata behavior while deduplicating path logic
- making dashboard packaging incomplete if a required import is missed

## Mitigations

- preserve existing redirect-aware metadata code paths
- add tests for the new shared helper module first
- run the full unit suite after refactor

## Success Criteria

- repo noise is reduced without deleting user data
- dashboard Docker image no longer installs the full pipeline stack
- repeated path helper logic is centralized
- existing test suite remains green
