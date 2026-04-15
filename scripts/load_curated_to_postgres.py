from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import date, datetime, timezone
from typing import Any

from Spark_jobs.transform_news_raw_to_processed import create_spark_session
from common.hdfs_utils import (
    build_hdfs_uri,
    derive_hdfs_default_fs,
    list_hdfs_files,
    resolve_explicit_or_latest_path,
)
from common.logging_utils import configure_logging, log_event
from common.pipeline_paths import resolve_batch_from_parquet_path, resolve_latest_parquet_batch


def parse_args() -> argparse.Namespace:
    default_hdfs_url = os.getenv("HDFS_URL", "http://localhost:9870")
    parser = argparse.ArgumentParser(
        description="Load curated Parquet from HDFS into analytics PostgreSQL tables."
    )
    parser.add_argument("--input-path", default=os.getenv("HDFS_CURATED_PATH", "/news/curated"))
    parser.add_argument(
        "--input-batch-path",
        default="",
        help="Optional exact curated HDFS batch path. When provided, this batch is used instead of resolving the latest curated batch.",
    )
    parser.add_argument("--hdfs-url", default=default_hdfs_url)
    parser.add_argument("--hdfs-user", default=os.getenv("HDFS_USER", "root"))
    parser.add_argument(
        "--hdfs-default-fs",
        default=os.getenv("HDFS_DEFAULT_FS", derive_hdfs_default_fs(default_hdfs_url)),
        help="Spark-accessible HDFS root, for example hdfs://namenode:9000.",
    )
    parser.add_argument(
        "--app-name",
        default="news-curated-to-postgres",
        help="Spark application name.",
    )
    parser.add_argument("--db-host", default=os.getenv("ANALYTICS_DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("ANALYTICS_DB_PORT", "5432")))
    parser.add_argument("--db-name", default=os.getenv("ANALYTICS_DB_NAME", "analytics"))
    parser.add_argument("--db-user", default=os.getenv("ANALYTICS_DB_USER", "analytics"))
    parser.add_argument("--db-password", default=os.getenv("ANALYTICS_DB_PASSWORD", "analytics"))
    parser.add_argument(
        "--batch-history-table",
        default=os.getenv("ANALYTICS_BATCH_HISTORY_TABLE", "analytics_load_history"),
    )
    parser.add_argument(
        "--ods-table",
        default=os.getenv("ANALYTICS_ODS_TABLE", "ods_news_articles"),
    )
    parser.add_argument(
        "--mart-table",
        default=os.getenv("ANALYTICS_MART_TABLE", "mart_news_daily_source"),
    )
    return parser.parse_args()


def resolve_curated_batch_from_parquet(parquet_path: str) -> str:
    return resolve_batch_from_parquet_path(
        parquet_path,
        partition_prefixes=("event_date=",),
        parents_up_if_unpartitioned=1,
    )


def resolve_latest_curated_batch(client: Any, path: str) -> str:
    return resolve_latest_parquet_batch(
        client,
        path,
        batch_from_parquet=resolve_curated_batch_from_parquet,
        missing_status_message="HDFS path does not exist: {path}",
        missing_parquet_message="No curated Parquet files found under {path}",
    )


def _ensure_utc_datetime(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"Expected datetime value, got {type(value).__name__}")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _ensure_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    raise ValueError(f"Expected date value, got {type(value).__name__}")


def build_curated_batch_fingerprint(
    files: list[tuple[str, dict[str, Any]]],
    *,
    batch_path: str,
) -> str:
    batch_prefix = batch_path.rstrip("/") + "/"
    payload = [
        {
            "relative_path": path.removeprefix(batch_prefix),
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


def iter_dataframe_chunks(dataframe: Any, chunk_size: int):
    chunk: list[dict[str, Any]] = []
    for row in dataframe.toLocalIterator():
        chunk.append(row.asDict(recursive=True))
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def build_curated_dataframe(*, input_uri: str, app_name: str) -> tuple[Any, Any]:
    from pyspark.sql import functions as F

    spark = create_spark_session(app_name)
    curated_df = (
        spark.read.parquet(input_uri)
        .select(
            F.trim(F.coalesce(F.col("title"), F.lit(""))).alias("title"),
            F.trim(F.coalesce(F.col("link"), F.lit(""))).alias("link"),
            F.trim(F.coalesce(F.col("summary"), F.lit(""))).alias("summary"),
            F.trim(F.coalesce(F.col("source"), F.lit(""))).alias("source"),
            F.trim(F.coalesce(F.col("ingestion_id"), F.lit(""))).alias("ingestion_id"),
            F.col("published_at").cast("timestamp").alias("published_at"),
            F.col("fetched_at").cast("timestamp").alias("fetched_at"),
            F.col("event_date").cast("date").alias("event_date"),
        )
        .where(
            (F.col("title") != "")
            & (F.col("link") != "")
            & (F.col("source") != "")
            & (F.col("ingestion_id") != "")
            & F.col("published_at").isNotNull()
            & F.col("fetched_at").isNotNull()
            & F.col("event_date").isNotNull()
        )
        .dropDuplicates(["link"])
    )
    return spark, curated_df


def connect_to_postgres(args: argparse.Namespace) -> Any:
    import psycopg2

    return psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
        connect_timeout=10,
    )


def ensure_analytics_tables(
    *,
    connection: Any,
    history_table: str,
    ods_table: str,
    mart_table: str,
) -> None:
    from psycopg2 import sql

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {history_table} (
                    batch_path TEXT PRIMARY KEY,
                    batch_fingerprint TEXT,
                    row_count INTEGER NOT NULL,
                    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            ).format(history_table=sql.Identifier(history_table))
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {ods_table} (
                    link TEXT PRIMARY KEY,
                    batch_path TEXT NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    source TEXT NOT NULL,
                    published_at TIMESTAMPTZ NOT NULL,
                    fetched_at TIMESTAMPTZ NOT NULL,
                    ingestion_id TEXT NOT NULL,
                    event_date DATE NOT NULL,
                    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            ).format(ods_table=sql.Identifier(ods_table))
        )
        cursor.execute(
            sql.SQL(
                "CREATE INDEX IF NOT EXISTS {index_name} ON {ods_table} (event_date)"
            ).format(
                index_name=sql.Identifier(f"{ods_table}_event_date_idx"),
                ods_table=sql.Identifier(ods_table),
            )
        )
        cursor.execute(
            sql.SQL(
                "CREATE INDEX IF NOT EXISTS {index_name} ON {ods_table} (source)"
            ).format(
                index_name=sql.Identifier(f"{ods_table}_source_idx"),
                ods_table=sql.Identifier(ods_table),
            )
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {mart_table} (
                    event_date DATE NOT NULL,
                    source TEXT NOT NULL,
                    article_count INTEGER NOT NULL,
                    latest_published_at TIMESTAMPTZ NOT NULL,
                    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (event_date, source)
                )
                """
            ).format(mart_table=sql.Identifier(mart_table))
        )
        cursor.execute(
            sql.SQL("ALTER TABLE {history_table} ADD COLUMN IF NOT EXISTS batch_fingerprint TEXT").format(
                history_table=sql.Identifier(history_table)
            )
        )
        cursor.execute(
            sql.SQL("ALTER TABLE {ods_table} ADD COLUMN IF NOT EXISTS batch_path TEXT").format(
                ods_table=sql.Identifier(ods_table)
            )
        )
        cursor.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {index_name} ON {ods_table} (batch_path)").format(
                index_name=sql.Identifier(f"{ods_table}_batch_path_idx"),
                ods_table=sql.Identifier(ods_table),
            )
        )
    connection.commit()


def get_loaded_batch_metadata(
    *,
    connection: Any,
    history_table: str,
    batch_path: str,
) -> dict[str, Any] | None:
    from psycopg2 import sql

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                SELECT batch_path, batch_fingerprint, row_count, loaded_at
                FROM {history_table}
                WHERE batch_path = %s
                LIMIT 1
                """
            ).format(
                history_table=sql.Identifier(history_table)
            ),
            (batch_path,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return {
            "batch_path": row[0],
            "batch_fingerprint": row[1],
            "row_count": row[2],
            "loaded_at": row[3],
        }


def upsert_ods_rows(
    *,
    connection: Any,
    ods_table: str,
    batch_path: str,
    rows: list[dict[str, Any]],
) -> int:
    from psycopg2 import sql
    from psycopg2.extras import execute_values

    values = [
        (
            row["link"],
            batch_path,
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

    if not values:
        return 0

    query = sql.SQL(
        """
        INSERT INTO {ods_table} (
            link,
            batch_path,
            title,
            summary,
            source,
            published_at,
            fetched_at,
            ingestion_id,
            event_date
        )
        VALUES %s
        ON CONFLICT (link) DO UPDATE SET
            batch_path = EXCLUDED.batch_path,
            title = EXCLUDED.title,
            summary = EXCLUDED.summary,
            source = EXCLUDED.source,
            published_at = EXCLUDED.published_at,
            fetched_at = EXCLUDED.fetched_at,
            ingestion_id = EXCLUDED.ingestion_id,
            event_date = EXCLUDED.event_date,
            loaded_at = NOW()
        """
    ).format(ods_table=sql.Identifier(ods_table))

    with connection.cursor() as cursor:
        execute_values(cursor, query.as_string(connection), values, page_size=1000)

    return len(values)


def upsert_ods_row_chunks(
    *,
    connection: Any,
    ods_table: str,
    batch_path: str,
    row_chunks: Any,
) -> tuple[int, list[date]]:
    total_rows = 0
    event_dates: list[date] = []

    for rows in row_chunks:
        upserted_count = upsert_ods_rows(
            connection=connection,
            ods_table=ods_table,
            batch_path=batch_path,
            rows=rows,
        )
        total_rows += upserted_count
        event_dates.extend(_ensure_date(row["event_date"]) for row in rows)

    return total_rows, event_dates


def delete_existing_ods_batch_rows(
    *,
    connection: Any,
    ods_table: str,
    batch_path: str,
) -> list[date]:
    from psycopg2 import sql

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                DELETE FROM {ods_table}
                WHERE batch_path = %s
                RETURNING event_date
                """
            ).format(ods_table=sql.Identifier(ods_table)),
            (batch_path,),
        )
        return [_ensure_date(row[0]) for row in cursor.fetchall()]


def refresh_daily_source_mart(
    *,
    connection: Any,
    ods_table: str,
    mart_table: str,
    event_dates: list[date],
) -> int:
    from psycopg2 import sql

    if not event_dates:
        return 0

    unique_event_dates = sorted(set(event_dates))

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("DELETE FROM {mart_table} WHERE event_date = ANY(%s)").format(
                mart_table=sql.Identifier(mart_table)
            ),
            (unique_event_dates,),
        )
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO {mart_table} (
                    event_date,
                    source,
                    article_count,
                    latest_published_at,
                    refreshed_at
                )
                SELECT
                    event_date,
                    source,
                    COUNT(*)::INTEGER AS article_count,
                    MAX(published_at) AS latest_published_at,
                    NOW() AS refreshed_at
                FROM {ods_table}
                WHERE event_date = ANY(%s)
                GROUP BY event_date, source
                ON CONFLICT (event_date, source) DO UPDATE SET
                    article_count = EXCLUDED.article_count,
                    latest_published_at = EXCLUDED.latest_published_at,
                    refreshed_at = NOW()
                """
            ).format(
                mart_table=sql.Identifier(mart_table),
                ods_table=sql.Identifier(ods_table),
            ),
            (unique_event_dates,),
        )
        return cursor.rowcount


def mark_batch_loaded(
    *,
    connection: Any,
    history_table: str,
    batch_path: str,
    batch_fingerprint: str,
    row_count: int,
) -> None:
    from psycopg2 import sql

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO {history_table} (batch_path, batch_fingerprint, row_count, loaded_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (batch_path) DO UPDATE SET
                    batch_fingerprint = EXCLUDED.batch_fingerprint,
                    row_count = EXCLUDED.row_count,
                    loaded_at = NOW()
                """
            ).format(history_table=sql.Identifier(history_table)),
            (batch_path, batch_fingerprint, row_count),
        )


def main() -> None:
    logger = configure_logging("analytics_loader")
    started_at = time.perf_counter()
    args = parse_args()

    from hdfs import InsecureClient

    os.environ["HDFS_DEFAULT_FS"] = args.hdfs_default_fs
    hdfs_client = InsecureClient(args.hdfs_url, user=args.hdfs_user)

    batch_path = resolve_explicit_or_latest_path(
        hdfs_client,
        explicit_path=args.input_batch_path,
        fallback_path=args.input_path,
        latest_resolver=resolve_latest_curated_batch,
    )
    input_uri = build_hdfs_uri(batch_path, args.hdfs_default_fs)

    log_event(
        logger,
        20,
        "analytics_load_started",
        input_path=batch_path,
        input_uri=input_uri,
        db_host=args.db_host,
        db_name=args.db_name,
    )

    connection = connect_to_postgres(args)
    try:
        ensure_analytics_tables(
            connection=connection,
            history_table=args.batch_history_table,
            ods_table=args.ods_table,
            mart_table=args.mart_table,
        )

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
            log_event(
                logger,
                20,
                "analytics_load_skipped_already_loaded_batch",
                input_path=batch_path,
                batch_fingerprint=batch_fingerprint,
                status="success",
                duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
            )
            return

        deleted_event_dates = delete_existing_ods_batch_rows(
            connection=connection,
            ods_table=args.ods_table,
            batch_path=batch_path,
        )
        spark, curated_df = build_curated_dataframe(input_uri=input_uri, app_name=args.app_name)
        try:
            upserted_count, event_dates = upsert_ods_row_chunks(
                connection=connection,
                ods_table=args.ods_table,
                batch_path=batch_path,
                row_chunks=iter_dataframe_chunks(curated_df, 500),
            )
        finally:
            spark.stop()
        affected_event_dates = deleted_event_dates + event_dates
        refreshed_count = refresh_daily_source_mart(
            connection=connection,
            ods_table=args.ods_table,
            mart_table=args.mart_table,
            event_dates=affected_event_dates,
        )
        mark_batch_loaded(
            connection=connection,
            history_table=args.batch_history_table,
            batch_path=batch_path,
            batch_fingerprint=batch_fingerprint,
            row_count=upserted_count,
        )
        connection.commit()

        log_event(
            logger,
            20,
            "analytics_load_completed",
            input_path=batch_path,
            input_uri=input_uri,
            row_count=upserted_count,
            refreshed_group_count=refreshed_count,
            affected_event_dates=sorted({d.isoformat() for d in affected_event_dates}),
            status="success",
            duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
        )
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()
