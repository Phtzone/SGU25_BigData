# Submission Readiness Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the remaining correctness and submission-quality risks by tightening ingress validation, making curated reload semantics content-aware, strengthening validation gates, chunking curated PostgreSQL loads, and cleaning tracked Airflow runtime logs out of the repo.

**Architecture:** Keep the existing pipeline topology and public entrypoints, but move strict contract checks to the ingestion edges, add a deterministic curated-batch fingerprint derived from HDFS file metadata, strengthen validation scripts so they act as real gates, and replace the curated loader's full-batch Python materialization with chunked iteration. All schema changes stay additive and all repo cleanup stays scoped to tracked runtime artifacts.

**Tech Stack:** Python 3.12, unittest, PySpark, HDFS client helpers, psycopg2, Kafka, Docker, git

---

## File Map

- Modify: `common/article_schema.py`
  - Add strict raw-contract validation that preserves unexpected-field errors before normalization.
- Modify: `producer/kafka_producer.py`
  - Reject invalid raw payloads before normalization strips fields.
- Modify: `consumer/kafka_consumer_to_hdfs.py`
  - Route original invalid payloads with extra fields to dead-letter instead of treating normalized payloads as valid.
- Modify: `common/hdfs_utils.py`
  - Add a deterministic directory fingerprint helper based on HDFS file listings.
- Modify: `scripts/load_curated_to_postgres.py`
  - Extend load history schema, compute curated batch fingerprints, chunk Spark row iteration, and reload changed same-path batches safely.
- Modify: `scripts/validate_processed_output.py`
  - Resolve and report a valid processed batch root instead of only reporting file presence.
- Modify: `scripts/validate_curated_output.py`
  - Enforce valid curated batch layout and partition presence.
- Modify: `scripts/validate_keyword_output.py`
  - Require both datasets and required metadata fields.
- Modify: `tests/test_article_schema.py`
  - Add strict unexpected-field validation coverage.
- Modify: `tests/test_kafka_usage.py`
  - Add producer/consumer regression coverage for extra-field payloads.
- Modify: `tests/test_load_curated_to_postgres.py`
  - Add fingerprint-aware load-history coverage and chunked iterator coverage.
- Modify: `tests/test_validate_keyword_output.py`
  - Add missing-required-metadata regression coverage.
- Modify: `tests/test_validate_hdfs_output.py`
  - Keep raw validation coverage stable while new validation work lands elsewhere.
- Create: `tests/test_submission_readiness_validators.py`
  - Focused tests for processed and curated validator batch-root/layout behavior.
- Modify: `.gitignore`
  - Keep `airflow/logs/` ignored while preserving `.gitkeep`.
- Remove from git index: tracked files under `airflow/logs/**` except `.gitkeep`
  - Clean submission artifacts without changing runtime behavior.
- Modify: `README.md`
  - Document stricter validation and curated reload semantics only if command behavior or operational expectations become user-visible.

### Task 1: Enforce strict ingress contract validation before normalization

**Files:**
- Modify: `common/article_schema.py`
- Modify: `producer/kafka_producer.py`
- Modify: `consumer/kafka_consumer_to_hdfs.py`
- Modify: `tests/test_article_schema.py`
- Modify: `tests/test_kafka_usage.py`

- [ ] **Step 1: Write the failing tests**

Add these tests to `tests/test_article_schema.py`:

```python
    def test_validate_article_record_rejects_unexpected_fields(self) -> None:
        errors = validate_article_record(
            {
                "title": "Hello",
                "link": "https://example.com",
                "summary": "World",
                "published_at": "2026-03-28T12:34:56+00:00",
                "source": "VNExpress",
                "fetched_at": "2026-03-28T12:35:00+00:00",
                "ingestion_id": "ing-001",
                "extra_field": "should-fail",
            }
        )

        self.assertIn("unexpected fields: extra_field", errors)
```

Add these tests to `tests/test_kafka_usage.py`:

```python
    def test_split_rows_by_validity_rejects_original_payload_with_unexpected_fields(self) -> None:
        valid_rows, invalid_rows = split_rows_by_validity(
            [
                {
                    "title": "Good",
                    "link": "https://example.com/1",
                    "summary": "One",
                    "published_at": "2026-03-28T08:00:00+00:00",
                    "source": "VNExpress",
                    "fetched_at": "2026-03-28T08:05:00+00:00",
                    "ingestion_id": "ing-001",
                    "extra": "boom",
                }
            ]
        )

        self.assertEqual(len(valid_rows), 0)
        self.assertEqual(len(invalid_rows), 1)
        self.assertIn("unexpected fields: extra", invalid_rows[0]["errors"])

    def test_send_article_rejects_original_payload_with_unexpected_fields(self) -> None:
        class FakeProducer:
            def send(self, *args, **kwargs):  # noqa: ANN002, ANN003
                raise AssertionError("send should not be called")

            def flush(self) -> None:
                pass

            def close(self) -> None:
                pass

        producer = NewsKafkaProducer.__new__(NewsKafkaProducer)
        producer.topic = "news_raw"
        producer.producer = FakeProducer()

        with self.assertRaises(ValueError) as error:
            producer.send_article(
                {
                    "title": "Good",
                    "link": "https://example.com/1",
                    "summary": "One",
                    "published_at": "2026-03-28T08:00:00+00:00",
                    "source": "VNExpress",
                    "fetched_at": "2026-03-28T08:05:00+00:00",
                    "ingestion_id": "ing-001",
                    "extra": "boom",
                }
            )

        self.assertIn("unexpected fields: extra", str(error.exception))
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m unittest tests.test_article_schema tests.test_kafka_usage -v
```

Expected: FAIL because the producer and consumer paths currently normalize away unexpected fields before validation.

- [ ] **Step 3: Write minimal implementation**

In `common/article_schema.py`, add a strict raw-payload validator that preserves original shape checks:

```python
def validate_original_article_record(article: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    missing_fields = [field for field in ARTICLE_FIELDS if field not in article]
    if missing_fields:
        errors.append(f"missing fields: {', '.join(missing_fields)}")

    unexpected_fields = [field for field in article if field not in ARTICLE_FIELDS]
    if unexpected_fields:
        errors.append(f"unexpected fields: {', '.join(sorted(unexpected_fields))}")

    normalized = normalize_article_record(article)
    for field in REQUIRED_TEXT_FIELDS:
        if not normalized[field]:
            errors.append(f"{field} is required")

    for field in REQUIRED_DATETIME_FIELDS:
        if not normalized[field]:
            original_value = normalize_text(article.get(field))
            if not original_value:
                errors.append(f"{field} is required")
            else:
                errors.append(f"{field} must be a valid datetime")

    return errors
```

In `producer/kafka_producer.py`, validate the original payload before normalization:

```python
from common.article_schema import (
    normalize_article_record,
    normalize_text,
    validate_article_record,
    validate_original_article_record,
)

    def send_article(self, article: Dict[str, Any]) -> Dict[str, Any]:
        original_errors = validate_original_article_record(article)
        if original_errors:
            raise ValueError(f"Invalid article payload for Kafka: {', '.join(original_errors)}")

        normalized = normalize_article_record(article)
        errors = validate_article_record(normalized)
        if errors:
            raise ValueError(f"Invalid article payload for Kafka: {', '.join(errors)}")
```

In `consumer/kafka_consumer_to_hdfs.py`, preserve original validation failures:

```python
from common.article_schema import (
    normalize_article_record,
    normalize_text,
    validate_article_record,
    validate_original_article_record,
)

    for row in rows:
        original_errors = validate_original_article_record(row)
        normalized = normalize_article_record(row)
        normalized_errors = validate_article_record(normalized)
        errors = list(dict.fromkeys(original_errors + normalized_errors))
        if errors:
            invalid_rows.append(
                {
                    "original_payload": row,
                    "normalized_payload": normalized,
                    "errors": errors,
                }
            )
            continue
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
python -m unittest tests.test_article_schema tests.test_kafka_usage -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add common/article_schema.py producer/kafka_producer.py consumer/kafka_consumer_to_hdfs.py tests/test_article_schema.py tests/test_kafka_usage.py
git commit -m "fix: enforce strict article contract validation"
```

### Task 2: Make curated batch loading content-aware with deterministic fingerprints

**Files:**
- Modify: `common/hdfs_utils.py`
- Modify: `scripts/load_curated_to_postgres.py`
- Modify: `tests/test_load_curated_to_postgres.py`

- [ ] **Step 1: Write the failing tests**

Add these tests to `tests/test_load_curated_to_postgres.py`:

```python
from types import SimpleNamespace

from scripts.load_curated_to_postgres import (
    build_curated_batch_fingerprint,
    should_skip_curated_batch_load,
)


    def test_build_curated_batch_fingerprint_uses_sorted_file_metadata(self) -> None:
        fingerprint = build_curated_batch_fingerprint(
            [
                ("/news/curated/2026/04/04/news_120000000000/part-0001.parquet", {"length": 20, "modificationTime": 200}),
                ("/news/curated/2026/04/04/news_120000000000/part-0000.parquet", {"length": 10, "modificationTime": 100}),
            ],
            batch_path="/news/curated/2026/04/04/news_120000000000",
        )

        self.assertTrue(fingerprint)

    def test_should_skip_curated_batch_load_only_when_path_and_fingerprint_match(self) -> None:
        self.assertTrue(
            should_skip_curated_batch_load(
                loaded_batch_metadata={"batch_path": "/news/curated/a", "batch_fingerprint": "abc123"},
                batch_path="/news/curated/a",
                batch_fingerprint="abc123",
            )
        )
        self.assertFalse(
            should_skip_curated_batch_load(
                loaded_batch_metadata={"batch_path": "/news/curated/a", "batch_fingerprint": "abc123"},
                batch_path="/news/curated/a",
                batch_fingerprint="zzz999",
            )
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m unittest tests.test_load_curated_to_postgres -v
```

Expected: FAIL because fingerprint-aware helpers do not exist yet.

- [ ] **Step 3: Write minimal implementation**

In `common/hdfs_utils.py`, add a reusable HDFS directory metadata collector if needed:

```python
def list_hdfs_file_metadata(client: InsecureClient, path: str) -> list[tuple[str, dict]]:
    return list_hdfs_files(client, path)
```

In `scripts/load_curated_to_postgres.py`, add fingerprint helpers:

```python
import hashlib
import json

from common.hdfs_utils import list_hdfs_files


def build_curated_batch_fingerprint(
    files: list[tuple[str, dict[str, Any]]],
    *,
    batch_path: str,
) -> str:
    payload = [
        {
            "relative_path": path.removeprefix(batch_path.rstrip("/") + "/"),
            "length": metadata.get("length", 0),
            "modification_time": metadata.get("modificationTime", 0),
        }
        for path, metadata in sorted(files, key=lambda item: item[0])
    ]
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def should_skip_curated_batch_load(
    *,
    loaded_batch_metadata: dict[str, Any] | None,
    batch_path: str,
    batch_fingerprint: str,
) -> bool:
    if loaded_batch_metadata is None:
        return False
    return (
        loaded_batch_metadata.get("batch_path") == batch_path
        and str(loaded_batch_metadata.get("batch_fingerprint", "")) == batch_fingerprint
    )
```

Extend analytics history schema and metadata access:

```python
                CREATE TABLE IF NOT EXISTS {history_table} (
                    batch_path TEXT PRIMARY KEY,
                    batch_fingerprint TEXT,
                    row_count INTEGER NOT NULL,
                    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
```

```python
        cursor.execute(
            sql.SQL("ALTER TABLE {history_table} ADD COLUMN IF NOT EXISTS batch_fingerprint TEXT").format(
                history_table=sql.Identifier(history_table)
            )
        )
```

```python
def get_loaded_batch_metadata(*, connection: Any, history_table: str, batch_path: str) -> dict[str, Any] | None:
    ...
    return {
        "batch_path": row[0],
        "batch_fingerprint": row[1],
        "row_count": row[2],
        "loaded_at": row[3],
    }
```

Compute and use the fingerprint before the skip decision:

```python
    batch_files = [item for item in list_hdfs_files(hdfs_client, batch_path) if item[0].endswith(".parquet")]
    batch_fingerprint = build_curated_batch_fingerprint(batch_files, batch_path=batch_path)
    loaded_batch_metadata = get_loaded_batch_metadata(
        connection=connection,
        history_table=args.batch_history_table,
        batch_path=batch_path,
    )
    if should_skip_curated_batch_load(
        loaded_batch_metadata=loaded_batch_metadata,
        batch_path=batch_path,
        batch_fingerprint=batch_fingerprint,
    ):
        ...
```

Persist the fingerprint on completion:

```python
def mark_batch_loaded(
    *,
    connection: Any,
    history_table: str,
    batch_path: str,
    batch_fingerprint: str,
    row_count: int,
) -> None:
    ...
                    batch_fingerprint = EXCLUDED.batch_fingerprint,
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
python -m unittest tests.test_load_curated_to_postgres -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add common/hdfs_utils.py scripts/load_curated_to_postgres.py tests/test_load_curated_to_postgres.py
git commit -m "fix: make curated load fingerprint-aware"
```

### Task 3: Strengthen processed, curated, and keyword validation gates

**Files:**
- Modify: `scripts/validate_processed_output.py`
- Modify: `scripts/validate_curated_output.py`
- Modify: `scripts/validate_keyword_output.py`
- Create: `tests/test_submission_readiness_validators.py`
- Modify: `tests/test_validate_keyword_output.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_submission_readiness_validators.py` with:

```python
import unittest

from scripts.validate_curated_output import resolve_batch_path as resolve_curated_batch_path
from scripts.validate_processed_output import resolve_batch_path as resolve_processed_batch_path


class SubmissionReadinessValidatorTests(unittest.TestCase):
    def test_resolve_processed_batch_path_uses_parent_directory(self) -> None:
        self.assertEqual(
            resolve_processed_batch_path("/news/processed/2026/04/04/news_120000000000/part-0000.parquet"),
            "/news/processed/2026/04/04/news_120000000000",
        )

    def test_resolve_curated_batch_path_uses_partition_root(self) -> None:
        self.assertEqual(
            resolve_curated_batch_path(
                "/news/curated/2026/04/04/news_120000000000/event_date=2026-04-04/source=VNExpress/part-0000.parquet"
            ),
            "/news/curated/2026/04/04/news_120000000000",
        )
```

Add to `tests/test_validate_keyword_output.py`:

```python
    def test_read_keyword_metadata_requires_required_fields(self) -> None:
        with patch(
            "scripts.validate_keyword_output.read_hdfs_bytes",
            return_value=b'{\n  "keyword_score_version": "v2"\n}',
        ):
            with self.assertRaises(SystemExit) as error:
                read_keyword_metadata(
                    hdfs_url="http://namenode:9870",
                    hdfs_user="root",
                    metadata_path="/news/keywords/2026/04/14/news/_keyword_metadata.json",
                    redirect_host="datanode",
                )

        self.assertIn("missing required fields", str(error.exception))
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m unittest tests.test_submission_readiness_validators tests.test_validate_keyword_output -v
```

Expected: FAIL because the processed validator does not expose `resolve_batch_path()` and keyword metadata validation does not require all fields yet.

- [ ] **Step 3: Write minimal implementation**

In `scripts/validate_processed_output.py`, add:

```python
def resolve_batch_path(latest_parquet: str) -> str:
    path = PurePosixPath(latest_parquet)
    if len(path.parents) < 2:
        raise SystemExit(f"Unexpected processed file layout: {latest_parquet}")
    return str(path.parent)
```

Use it in `main()`:

```python
    latest_batch = resolve_batch_path(latest_parquet)
```

In `scripts/validate_curated_output.py`, keep `resolve_batch_path()` but ensure it remains the authoritative partition-root resolver.

In `scripts/validate_keyword_output.py`, require full metadata fields:

```python
def read_keyword_metadata(
    *,
    hdfs_url: str,
    hdfs_user: str,
    metadata_path: str,
    redirect_host: str = "",
) -> dict:
    payload = json.loads(...)
    if not isinstance(payload, dict):
        raise SystemExit(f"Keyword metadata file is invalid JSON object: {metadata_path}")

    required_fields = ("batch_path", "keyword_output_path", "keyword_score_version", "keyword_config_hash")
    missing_fields = [field for field in required_fields if not str(payload.get(field, "")).strip()]
    if missing_fields:
        raise SystemExit(
            f"Keyword metadata file is missing required fields: {', '.join(missing_fields)}"
        )
    return payload
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
python -m unittest tests.test_submission_readiness_validators tests.test_validate_keyword_output -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/validate_processed_output.py scripts/validate_curated_output.py scripts/validate_keyword_output.py tests/test_submission_readiness_validators.py tests/test_validate_keyword_output.py
git commit -m "fix: strengthen pipeline validation gates"
```

### Task 4: Replace curated full-batch collection with chunked iteration

**Files:**
- Modify: `scripts/load_curated_to_postgres.py`
- Modify: `tests/test_load_curated_to_postgres.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_load_curated_to_postgres.py`:

```python
from scripts.load_curated_to_postgres import iter_dataframe_chunks


    def test_iter_dataframe_chunks_splits_rows_by_chunk_size(self) -> None:
        class FakeRow:
            def __init__(self, payload):
                self.payload = payload

            def asDict(self, recursive: bool = True):  # noqa: ARG002
                return self.payload

        class FakeDataFrame:
            @staticmethod
            def toLocalIterator():
                return iter(
                    [
                        FakeRow({"id": 1}),
                        FakeRow({"id": 2}),
                        FakeRow({"id": 3}),
                    ]
                )

        chunks = list(iter_dataframe_chunks(FakeDataFrame(), 2))
        self.assertEqual(chunks, [[{"id": 1}, {"id": 2}], [{"id": 3}]])
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m unittest tests.test_load_curated_to_postgres -v
```

Expected: FAIL because `iter_dataframe_chunks()` does not exist yet and the loader still depends on `collect()`.

- [ ] **Step 3: Write minimal implementation**

In `scripts/load_curated_to_postgres.py`, add:

```python
def iter_dataframe_chunks(dataframe: Any, chunk_size: int):
    chunk: list[dict[str, Any]] = []
    for row in dataframe.toLocalIterator():
        chunk.append(row.asDict(recursive=True))
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
```

Replace `extract_curated_rows()` with a chunked DataFrame builder:

```python
def build_curated_dataframe(*, input_uri: str, app_name: str):
    from pyspark.sql import functions as F

    spark = create_spark_session(app_name)
    curated_df = (
        spark.read.parquet(input_uri)
        .select(
            ...
        )
        .where(
            ...
        )
        .dropDuplicates(["link"])
    )
    return spark, curated_df
```

Add a chunked upsert helper:

```python
def upsert_ods_row_chunks(
    *,
    connection: Any,
    ods_table: str,
    row_chunks: Any,
) -> tuple[int, list[date]]:
    total_rows = 0
    event_dates: list[date] = []
    for rows in row_chunks:
        values = [
            (
                row["link"],
                row["title"],
                row["summary"],
                row["source"],
                _ensure_utc_datetime(row["published_at"]),
                _ensure_utc_datetime(row["fetched_at"]),
                row["ingestion_id"],
                _ensure_date(row["event_date"]),
            )
            for row in rows
        ]
        ...
        total_rows += len(values)
        event_dates.extend(_ensure_date(row["event_date"]) for row in rows)
    return total_rows, event_dates
```

Use chunked iteration in `main()`:

```python
        spark, curated_df = build_curated_dataframe(input_uri=input_uri, app_name=args.app_name)
        try:
            upserted_count, event_dates = upsert_ods_row_chunks(
                connection=connection,
                ods_table=args.ods_table,
                row_chunks=iter_dataframe_chunks(curated_df, 500),
            )
        finally:
            spark.stop()
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
python -m unittest tests.test_load_curated_to_postgres -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/load_curated_to_postgres.py tests/test_load_curated_to_postgres.py
git commit -m "refactor: chunk curated postgres loads"
```

### Task 5: Clean tracked Airflow logs and verify final submission state

**Files:**
- Modify: `.gitignore`
- Remove from git index: `airflow/logs/**` except `airflow/logs/.gitkeep`
- Modify: `README.md`
- Verify: full test suite

- [ ] **Step 1: Keep repo ignore rules explicit**

Ensure `.gitignore` includes:

```gitignore
airflow/logs/
```

Leave `airflow/logs/.gitkeep` tracked.

- [ ] **Step 2: Remove tracked Airflow runtime logs from the git index**

Run:

```bash
git rm --cached -r airflow/logs
git add airflow/logs/.gitkeep .gitignore
```

Expected: all tracked log artifacts are removed from version control and `.gitkeep` remains staged.

- [ ] **Step 3: Update README only if behavior changed visibly**

If strict validation or curated reload behavior needs operator-facing documentation, add a concise note like:

```md
The curated analytics loader now tracks a deterministic batch fingerprint. If a curated batch is regenerated at the same HDFS path with different contents, the loader reloads it instead of silently skipping it.

Ingress validation is strict: payloads with missing required fields, invalid datetimes, or unexpected contract fields are rejected instead of being silently normalized into valid records.
```

- [ ] **Step 4: Run the full verification suite**

Run:

```bash
python -m unittest discover -s tests -p "test_*.py"
python -m compileall common producer consumer dashboard scripts Spark_jobs dags
git status --short
```

Expected:

- unittest suite passes with 0 failures
- compileall succeeds
- `git status --short` shows only intended source/doc/index changes and no tracked Airflow runtime logs beyond `.gitkeep`

- [ ] **Step 5: Commit**

```bash
git add .gitignore README.md airflow/logs/.gitkeep
git commit -m "chore: clean submission artifacts and docs"
```

## Self-Review

Spec coverage:

- strict ingress validation: covered in Task 1
- content-aware curated reload behavior: covered in Task 2
- stronger processed/curated/keyword validators: covered in Task 3
- chunked curated PostgreSQL loading: covered in Task 4
- repo hygiene and tracked Airflow log cleanup: covered in Task 5

Placeholder scan:

- no `TODO`, `TBD`, or “implement later” placeholders remain
- every code-changing task includes concrete code snippets
- every verification step includes an exact command and expected outcome

Type consistency:

- `validate_original_article_record()` is introduced in Task 1 and used consistently in producer and consumer code
- `build_curated_batch_fingerprint()` and `should_skip_curated_batch_load()` are defined in Task 2 and referenced consistently
- `iter_dataframe_chunks()` is defined in Task 4 before chunked upsert flow depends on it
