# Submission Readiness Fixes Design

## Goal

Raise the project's submission readiness without changing the core pipeline topology:

`RSS -> Kafka -> HDFS raw -> Spark processed -> Spark curated -> Spark keywords -> PostgreSQL -> Streamlit`

The work focuses on removing silent data-quality failure modes, making reload behavior safer, tightening validation gates, improving curated batch loading behavior, and cleaning repository noise before submission.

## Scope

In scope:

- enforce strict article-contract validation before normalization in producer and consumer paths
- ensure invalid payloads with unexpected fields are rejected or routed to dead-letter instead of being silently sanitized
- change curated analytics load semantics so the same `batch_path` can be reloaded when the underlying batch content changes
- strengthen processed, curated, and keyword validation scripts beyond simple file-existence checks
- remove the curated-to-PostgreSQL full-batch `collect()` pattern and replace it with chunked iteration
- remove tracked Airflow log artifacts from git and keep only `airflow/logs/.gitkeep`
- add regression tests for all changed behavior

Out of scope:

- changing DAG task order
- changing RSS source definitions
- redesigning the overall HDFS layout
- changing keyword scoring logic
- rewriting the keyword PostgreSQL loader for fully distributed JDBC writes
- redesigning the Streamlit dashboard UX

## Current Context

The current repository is already structurally complete for submission:

- producer and consumer stages publish and persist normalized article events
- Spark jobs create processed, curated, and keyword outputs
- PostgreSQL loaders build analytics marts and dashboard views
- Airflow orchestrates the same end-to-end pipeline
- the repo includes a substantial unit-test suite

The remaining issues are not about missing features. They are about correctness and submission quality:

- strict schema violations can currently be hidden by normalization
- curated analytics reload behavior is overly tied to `batch_path`
- validation scripts are too shallow to act as meaningful gates
- curated PostgreSQL loading currently pulls the full batch into Python memory
- the repository still tracks Airflow runtime logs that should not ship in a clean submission

## Design Summary

### 1. Contract enforcement moves ahead of normalization

The current flow normalizes payloads first and validates afterward. That order removes unexpected fields before validation can reject them.

The new design keeps normalization, but only for payloads that already satisfy the raw article contract. Validation will be split into two concerns:

- strict contract validation on the original payload shape
- normalization for accepted records

This preserves the current canonical article schema while making schema drift visible instead of silent.

### 2. Curated analytics load identity becomes content-aware

The current curated loader treats `batch_path` as the full idempotency key. That is too weak because the same path can be regenerated after logic changes or reruns.

The new design will keep `batch_path` as a stable reference but add batch fingerprint metadata so the loader can distinguish:

- same path, same content: skip safely
- same path, changed content: reload safely

This avoids stale PostgreSQL state when curated output is regenerated in place.

### 3. Validation scripts become real gates

The current validators mostly prove that files exist.

The new validators will prove the minimum structure that downstream steps rely on:

- processed output resolves to a valid batch root and contains Parquet output
- curated output resolves to a valid batch root and contains partitioned Parquet layout
- keyword output contains both required datasets and metadata with required fields

The goal is not to build full schema profilers. The goal is to fail early when downstream assumptions are broken.

### 4. Curated PostgreSQL load stops collecting the full batch in Python

The current curated loader uses Spark for filtering and then materializes the entire curated batch into a Python list before database upsert. That is the clearest mismatch with a big-data pipeline narrative.

The new loader will keep Spark-based filtering and projection but iterate through rows in chunks and upsert chunk by chunk. This preserves the current database contract while removing the highest-risk memory bottleneck in the curated load path.

### 5. Submission hygiene becomes explicit

The repo should not ship with tracked Airflow runtime logs. Only the log directory placeholder should remain in version control.

The design therefore includes:

- removing tracked `airflow/logs/**` runtime files from git
- keeping `airflow/logs/.gitkeep`
- preserving `.gitignore` rules so new runtime logs stay untracked

## Data Flow Changes

## Current Flow

```text
RSS
-> normalize article
-> Kafka
-> consumer normalize again
-> HDFS raw
-> Spark processed
-> Spark curated
-> Spark keywords
-> PostgreSQL
-> Streamlit
```

## Proposed Flow

```text
RSS
-> strict raw contract validation
-> normalize valid article
-> Kafka
-> strict consumed-payload validation
-> invalid => dead-letter
-> valid => HDFS raw
-> Spark processed
-> processed validation gate
-> Spark curated
-> curated validation gate
-> content-aware curated load decision
-> chunked curated load to PostgreSQL
-> Spark keywords
-> strict keyword validation gate
-> PostgreSQL
-> Streamlit
```

## What Changes And What Does Not

Unchanged:

- the pipeline stages and technologies remain the same
- DAG task order remains the same
- output zones and dashboard-facing marts remain the same

Changed:

- validation control points become stricter at ingress and before downstream loads
- curated load skip/reload logic becomes content-aware instead of path-only
- curated load execution becomes chunked instead of full-batch materialization in Python

## Architecture

### 1. Article Contract Validation

The producer and consumer will share a stricter contract check that operates on the original payload before normalization.

Expected behavior:

- producer rejects invalid payloads before publish
- consumer routes invalid consumed payloads to dead-letter with context
- unexpected fields count as contract violations
- normalized payloads remain the storage/publish format for valid records

The dead-letter payload should continue to include enough context to debug failures:

- original payload
- normalized payload when applicable
- validation errors
- Kafka message metadata for consumer-side failures

### 2. Curated Batch Fingerprinting

The curated loader history table will be extended to store a lightweight fingerprint for the resolved curated batch.

The fingerprint will be a deterministic hash built from the sorted HDFS file listing under the resolved curated batch root. Each listed file contributes:

- relative file path
- file length
- file modification time

This makes the fingerprint:

- stable for the same physical batch contents
- sensitive to regenerated output at the same `batch_path`
- cheap to compute without collecting the curated dataset into Python memory

Load behavior:

- no existing history row: load and record fingerprint
- existing history row with same fingerprint: skip
- existing history row with different fingerprint: reload affected data and update history row

### 3. Stronger Validation Gates

The validation scripts will remain lightweight CLI tools, but they will validate the minimum downstream assumptions.

#### Processed validation

Must prove:

- target HDFS path exists
- Parquet files exist
- the latest Parquet file resolves to a valid processed batch root

#### Curated validation

Must prove:

- target HDFS path exists
- curated Parquet files exist
- the latest Parquet file resolves to a valid curated batch root
- partition-style layout is present for `event_date` and `source`

#### Keyword validation

Must prove:

- keyword Parquet files exist
- both `article_keywords` and `keyword_daily_source` datasets exist in the same batch
- `_keyword_metadata.json` exists
- metadata contains required fields:
  - `batch_path`
  - `keyword_output_path`
  - `keyword_score_version`
  - `keyword_config_hash`

These validators should keep returning concise machine-readable JSON when requested.

### 4. Chunked Curated PostgreSQL Load

The curated loader will continue to:

- read curated Parquet with Spark
- trim and filter required fields
- deduplicate by `link`
- upsert into ODS
- refresh daily-source mart rows for affected dates

The implementation change is in how rows move from Spark to Python/PostgreSQL:

- avoid `collect()` into one Python list
- iterate through Spark rows with a local iterator
- build bounded chunks
- upsert each chunk through `execute_values`

This preserves the existing PostgreSQL schema and upsert semantics while improving memory behavior and making the design more defensible in a big-data project review.

### 5. Repo Hygiene

The repository cleanup is intentionally narrow and submission-focused:

- remove tracked Airflow runtime logs from the current git index
- keep `airflow/logs/.gitkeep`
- do not change runtime log generation behavior itself

This is a packaging cleanup, not an observability redesign.

## Testing Strategy

Regression tests will be added before implementation changes for the following behaviors:

- strict validation rejects payloads with unexpected fields
- producer does not publish payloads that only become valid after sanitization
- consumer does not treat extra-field payloads as valid after normalization
- curated load history distinguishes same-path/same-content from same-path/changed-content
- curated validation scripts fail on malformed batch structure
- keyword validation fails when required metadata fields are missing
- curated PostgreSQL loader uses chunked iteration instead of full-batch `collect()`

The existing unit suite must remain green after the change set.

## Migration And Compatibility

The only planned persistence migration is in curated analytics load history.

Migration rules:

- add new history columns with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
- preserve existing rows
- treat older rows without fingerprint data as reloadable on first post-change execution

This keeps the migration backward-compatible while allowing the new semantics to take effect progressively.

## Risks

- stricter validation may reject payloads that previously slipped through
- content-aware reload semantics may cause previously skipped curated batches to reload once
- stronger validators may cause demo scripts to fail earlier than before
- chunked loading adds implementation complexity around iterator and commit boundaries

## Mitigations

- add regression tests for each tightened behavior before changing production code
- keep the public CLI entrypoints and major output contracts stable
- keep database schema changes additive
- limit the change set to curated loading, validators, ingress validation, and repo hygiene only

## Success Criteria

- strict contract violations can no longer be silently normalized into valid records
- curated PostgreSQL state can refresh when a curated batch is regenerated at the same path
- validators fail on structurally incomplete outputs instead of only missing files
- curated PostgreSQL loading no longer materializes the full batch in one Python list
- the unit suite remains green
- the repo no longer tracks Airflow runtime log artifacts beyond `.gitkeep`

## Recommendation

Implement the balanced patch set:

- tighten ingress validation
- make curated reload semantics content-aware
- strengthen validation gates
- chunk curated analytics loading
- clean tracked Airflow logs out of the repo

This is the best fit for the current project state because it directly improves submission quality and technical defensibility without expanding into a risky redesign.
