from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date
from typing import Any, Iterable

from Spark_jobs.transform_news_raw_to_processed import create_spark_session
from common.hdfs_utils import build_hdfs_uri, derive_hdfs_default_fs, read_hdfs_bytes, resolve_explicit_or_latest_path
from common.logging_utils import configure_logging, log_event
from scripts.validate_keyword_output import resolve_latest_keyword_batch

KEYWORD_METADATA_FILENAME = "_keyword_metadata.json"


def parse_args() -> argparse.Namespace:
    default_hdfs_url = os.getenv("HDFS_URL", "http://localhost:9870")
    parser = argparse.ArgumentParser(
        description="Load keyword Parquet batches from HDFS into analytics PostgreSQL tables."
    )
    parser.add_argument("--input-path", default=os.getenv("HDFS_KEYWORDS_PATH", "/news/keywords"))
    parser.add_argument(
        "--input-batch-path",
        default="",
        help="Optional exact keyword HDFS batch path. When provided, this batch is used instead of resolving the latest keyword batch.",
    )
    parser.add_argument("--hdfs-url", default=default_hdfs_url)
    parser.add_argument("--hdfs-user", default=os.getenv("HDFS_USER", "root"))
    parser.add_argument(
        "--webhdfs-redirect-host",
        default=os.getenv("WEBHDFS_REDIRECT_HOST", ""),
        help="Override the hostname returned by WebHDFS redirects when reading metadata files.",
    )
    parser.add_argument(
        "--hdfs-default-fs",
        default=os.getenv("HDFS_DEFAULT_FS", derive_hdfs_default_fs(default_hdfs_url)),
        help="Spark-accessible HDFS root, for example hdfs://namenode:9000.",
    )
    parser.add_argument(
        "--app-name",
        default="news-keywords-to-postgres",
        help="Spark application name.",
    )
    parser.add_argument("--db-host", default=os.getenv("ANALYTICS_DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("ANALYTICS_DB_PORT", "5432")))
    parser.add_argument("--db-name", default=os.getenv("ANALYTICS_DB_NAME", "analytics"))
    parser.add_argument("--db-user", default=os.getenv("ANALYTICS_DB_USER", "analytics"))
    parser.add_argument("--db-password", default=os.getenv("ANALYTICS_DB_PASSWORD", "analytics"))
    parser.add_argument(
        "--history-table",
        default=os.getenv("ANALYTICS_KEYWORD_BATCH_HISTORY_TABLE", "analytics_keyword_load_history"),
    )
    parser.add_argument(
        "--article-keywords-table",
        default=os.getenv("ANALYTICS_ARTICLE_KEYWORDS_TABLE", "mart_article_keywords"),
    )
    parser.add_argument(
        "--keyword-daily-source-table",
        default=os.getenv("ANALYTICS_KEYWORD_DAILY_SOURCE_TABLE", "mart_keyword_daily_source"),
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="Number of rows to batch per PostgreSQL upsert chunk.",
    )
    parser.add_argument(
        "--force-reload",
        action="store_true",
        help="Reload the batch even if it already exists in the keyword load history table.",
    )
    return parser.parse_args()


def _ensure_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    raise ValueError(f"Expected date value, got {type(value).__name__}")


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


def read_keyword_batch_metadata(
    *,
    hdfs_client: Any,
    batch_path: str,
    hdfs_url: str,
    hdfs_user: str,
    redirect_host: str = "",
) -> dict[str, Any]:
    metadata_path = f"{batch_path}/{KEYWORD_METADATA_FILENAME}"
    if not hdfs_client.status(metadata_path, strict=False):
        raise SystemExit(f"Keyword batch metadata file does not exist: {metadata_path}")

    payload = json.loads(
        read_hdfs_bytes(
            hdfs_url=hdfs_url,
            hdfs_user=hdfs_user,
            path=metadata_path,
            redirect_host=redirect_host,
        ).decode("utf-8")
    )

    if not isinstance(payload, dict):
        raise SystemExit(f"Keyword batch metadata file is invalid JSON object: {metadata_path}")

    required_fields = ("batch_path", "keyword_output_path", "keyword_score_version", "keyword_config_hash")
    missing_fields = [field for field in required_fields if not str(payload.get(field, "")).strip()]
    if missing_fields:
        raise SystemExit(
            f"Keyword batch metadata file is missing required fields: {', '.join(missing_fields)}"
        )

    return payload


def reset_streamlit_keyword_views(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute("DROP VIEW IF EXISTS vw_streamlit_keyword_daily_overall_latest")
        cursor.execute("DROP VIEW IF EXISTS vw_streamlit_keyword_daily_source_latest")
        cursor.execute("DROP VIEW IF EXISTS vw_streamlit_article_keywords_latest")


def ensure_keyword_tables(
    *,
    connection: Any,
    history_table: str,
    article_keywords_table: str,
    keyword_daily_source_table: str,
) -> None:
    from psycopg2 import sql

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {history_table} (
                    batch_path TEXT PRIMARY KEY,
                    keyword_score_version TEXT,
                    keyword_config_hash TEXT,
                    article_keyword_row_count INTEGER NOT NULL,
                    keyword_daily_source_row_count INTEGER NOT NULL,
                    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            ).format(history_table=sql.Identifier(history_table))
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {article_keywords_table} (
                    batch_path TEXT NOT NULL,
                    event_date DATE NOT NULL,
                    source TEXT NOT NULL,
                    link TEXT NOT NULL,
                    title TEXT NOT NULL,
                    keyword TEXT NOT NULL,
                    keyword_normalized TEXT NOT NULL,
                    ngram_size INTEGER NOT NULL,
                    base_score DOUBLE PRECISION,
                    quality_penalty DOUBLE PRECISION,
                    article_score DOUBLE PRECISION NOT NULL,
                    quality_flags TEXT,
                    keyword_score_version TEXT,
                    keyword_config_hash TEXT,
                    rank_in_article INTEGER NOT NULL,
                    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (batch_path, link, keyword_normalized)
                )
                """
            ).format(article_keywords_table=sql.Identifier(article_keywords_table))
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {keyword_daily_source_table} (
                    batch_path TEXT NOT NULL,
                    event_date DATE NOT NULL,
                    source TEXT NOT NULL,
                    keyword TEXT NOT NULL,
                    keyword_normalized TEXT NOT NULL,
                    ngram_size INTEGER NOT NULL,
                    article_count INTEGER NOT NULL,
                    base_score DOUBLE PRECISION,
                    quality_penalty DOUBLE PRECISION,
                    weighted_score DOUBLE PRECISION NOT NULL,
                    avg_article_score DOUBLE PRECISION NOT NULL,
                    quality_flags TEXT,
                    keyword_score_version TEXT,
                    keyword_config_hash TEXT,
                    source_spread_score DOUBLE PRECISION,
                    recency_score DOUBLE PRECISION,
                    breakout_score DOUBLE PRECISION,
                    final_keyword_score DOUBLE PRECISION,
                    rank_in_group INTEGER NOT NULL,
                    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (batch_path, event_date, source, keyword_normalized)
                )
                """
            ).format(keyword_daily_source_table=sql.Identifier(keyword_daily_source_table))
        )
        cursor.execute(
            sql.SQL("ALTER TABLE {history_table} ADD COLUMN IF NOT EXISTS keyword_score_version TEXT").format(
                history_table=sql.Identifier(history_table)
            )
        )
        cursor.execute(
            sql.SQL("ALTER TABLE {history_table} ADD COLUMN IF NOT EXISTS keyword_config_hash TEXT").format(
                history_table=sql.Identifier(history_table)
            )
        )
        cursor.execute(
            sql.SQL("ALTER TABLE {article_keywords_table} ADD COLUMN IF NOT EXISTS base_score DOUBLE PRECISION").format(
                article_keywords_table=sql.Identifier(article_keywords_table)
            )
        )
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {article_keywords_table} ADD COLUMN IF NOT EXISTS quality_penalty DOUBLE PRECISION"
            ).format(article_keywords_table=sql.Identifier(article_keywords_table))
        )
        cursor.execute(
            sql.SQL("ALTER TABLE {article_keywords_table} ADD COLUMN IF NOT EXISTS quality_flags TEXT").format(
                article_keywords_table=sql.Identifier(article_keywords_table)
            )
        )
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {article_keywords_table} ADD COLUMN IF NOT EXISTS keyword_score_version TEXT"
            ).format(article_keywords_table=sql.Identifier(article_keywords_table))
        )
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {article_keywords_table} ADD COLUMN IF NOT EXISTS keyword_config_hash TEXT"
            ).format(article_keywords_table=sql.Identifier(article_keywords_table))
        )
        cursor.execute(
            sql.SQL("ALTER TABLE {keyword_daily_source_table} ADD COLUMN IF NOT EXISTS base_score DOUBLE PRECISION").format(
                keyword_daily_source_table=sql.Identifier(keyword_daily_source_table)
            )
        )
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {keyword_daily_source_table} ADD COLUMN IF NOT EXISTS quality_penalty DOUBLE PRECISION"
            ).format(keyword_daily_source_table=sql.Identifier(keyword_daily_source_table))
        )
        cursor.execute(
            sql.SQL("ALTER TABLE {keyword_daily_source_table} ADD COLUMN IF NOT EXISTS quality_flags TEXT").format(
                keyword_daily_source_table=sql.Identifier(keyword_daily_source_table)
            )
        )
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {keyword_daily_source_table} ADD COLUMN IF NOT EXISTS keyword_score_version TEXT"
            ).format(keyword_daily_source_table=sql.Identifier(keyword_daily_source_table))
        )
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {keyword_daily_source_table} ADD COLUMN IF NOT EXISTS keyword_config_hash TEXT"
            ).format(keyword_daily_source_table=sql.Identifier(keyword_daily_source_table))
        )
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {keyword_daily_source_table} ADD COLUMN IF NOT EXISTS source_spread_score DOUBLE PRECISION"
            ).format(keyword_daily_source_table=sql.Identifier(keyword_daily_source_table))
        )
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {keyword_daily_source_table} ADD COLUMN IF NOT EXISTS recency_score DOUBLE PRECISION"
            ).format(keyword_daily_source_table=sql.Identifier(keyword_daily_source_table))
        )
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {keyword_daily_source_table} ADD COLUMN IF NOT EXISTS breakout_score DOUBLE PRECISION"
            ).format(keyword_daily_source_table=sql.Identifier(keyword_daily_source_table))
        )
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {keyword_daily_source_table} ADD COLUMN IF NOT EXISTS final_keyword_score DOUBLE PRECISION"
            ).format(keyword_daily_source_table=sql.Identifier(keyword_daily_source_table))
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS idx_mart_article_keywords_event_date_source_rank
                    ON {article_keywords_table} (event_date, source, rank_in_article)
                """
            ).format(article_keywords_table=sql.Identifier(article_keywords_table))
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS idx_mart_article_keywords_keyword_normalized
                    ON {article_keywords_table} (keyword_normalized)
                """
            ).format(article_keywords_table=sql.Identifier(article_keywords_table))
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS idx_mart_article_keywords_link_keyword_loaded_at
                    ON {article_keywords_table} (link, keyword_normalized, loaded_at DESC)
                """
            ).format(article_keywords_table=sql.Identifier(article_keywords_table))
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS idx_mart_keyword_daily_source_event_date_source_rank
                    ON {keyword_daily_source_table} (event_date, source, rank_in_group)
                """
            ).format(keyword_daily_source_table=sql.Identifier(keyword_daily_source_table))
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS idx_mart_keyword_daily_source_keyword_normalized
                    ON {keyword_daily_source_table} (keyword_normalized)
                """
            ).format(keyword_daily_source_table=sql.Identifier(keyword_daily_source_table))
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS idx_mart_keyword_daily_source_lookup
                    ON {keyword_daily_source_table} (event_date, source, keyword_normalized, loaded_at DESC)
                """
            ).format(keyword_daily_source_table=sql.Identifier(keyword_daily_source_table))
        )

        reset_streamlit_keyword_views(connection)

        cursor.execute(
            sql.SQL(
                """
                CREATE OR REPLACE VIEW vw_streamlit_article_keywords_latest AS
                WITH ranked_keywords AS (
                    SELECT
                        batch_path,
                        event_date,
                        source,
                        link,
                        title,
                        keyword,
                        keyword_normalized,
                        ngram_size,
                        base_score,
                        quality_penalty,
                        article_score,
                        quality_flags,
                        keyword_score_version,
                        keyword_config_hash,
                        rank_in_article,
                        loaded_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY link, keyword_normalized
                            ORDER BY loaded_at DESC, batch_path DESC, rank_in_article ASC
                        ) AS recency_rank
                    FROM {article_keywords_table}
                )
                SELECT
                    batch_path,
                    event_date,
                    source,
                    link,
                    title,
                    keyword,
                    keyword_normalized,
                    ngram_size,
                    base_score,
                    quality_penalty,
                    article_score,
                    quality_flags,
                    keyword_score_version,
                    keyword_config_hash,
                    rank_in_article,
                    loaded_at
                FROM ranked_keywords
                WHERE recency_rank = 1
                """
            ).format(article_keywords_table=sql.Identifier(article_keywords_table))
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE OR REPLACE VIEW vw_streamlit_keyword_daily_source_latest AS
                WITH ranked_keywords AS (
                    SELECT
                        batch_path,
                        event_date,
                        source,
                        keyword,
                        keyword_normalized,
                        ngram_size,
                        article_count,
                        base_score,
                        quality_penalty,
                        weighted_score,
                        avg_article_score,
                        quality_flags,
                        keyword_score_version,
                        keyword_config_hash,
                        source_spread_score,
                        recency_score,
                        breakout_score,
                        final_keyword_score,
                        rank_in_group,
                        loaded_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY event_date, source, keyword_normalized
                            ORDER BY loaded_at DESC, batch_path DESC, rank_in_group ASC
                        ) AS recency_rank
                    FROM {keyword_daily_source_table}
                )
                SELECT
                    batch_path,
                    event_date,
                    source,
                    keyword,
                    keyword_normalized,
                    ngram_size,
                    article_count,
                    base_score,
                    quality_penalty,
                    weighted_score,
                    avg_article_score,
                    quality_flags,
                    keyword_score_version,
                    keyword_config_hash,
                    source_spread_score,
                    recency_score,
                    breakout_score,
                    final_keyword_score,
                    rank_in_group,
                    loaded_at
                FROM ranked_keywords
                WHERE recency_rank = 1
                """
            ).format(keyword_daily_source_table=sql.Identifier(keyword_daily_source_table))
        )
        cursor.execute(
            """
            CREATE OR REPLACE VIEW vw_streamlit_keyword_daily_overall_latest AS
            WITH latest_source_keywords AS (
                SELECT
                    event_date,
                    source,
                    keyword,
                    keyword_normalized,
                    ngram_size,
                    article_count,
                    weighted_score,
                    avg_article_score,
                    final_keyword_score,
                    keyword_score_version,
                    keyword_config_hash
                FROM vw_streamlit_keyword_daily_source_latest
            )
            SELECT
                event_date,
                keyword,
                keyword_normalized,
                MAX(ngram_size) AS ngram_size,
                COUNT(DISTINCT source) AS source_count,
                SUM(article_count) AS article_count,
                SUM(weighted_score) AS weighted_score,
                AVG(avg_article_score) AS avg_article_score,
                SUM(final_keyword_score) AS final_keyword_score,
                MAX(keyword_score_version) AS keyword_score_version,
                MAX(keyword_config_hash) AS keyword_config_hash,
                ROW_NUMBER() OVER (
                    PARTITION BY event_date
                    ORDER BY
                        SUM(final_keyword_score) DESC,
                        SUM(article_count) DESC,
                        MAX(ngram_size) DESC,
                        keyword_normalized ASC
                ) AS rank_in_day
            FROM latest_source_keywords
            GROUP BY event_date, keyword, keyword_normalized
            """
        )
    connection.commit()


def get_loaded_batch_metadata(*, connection: Any, history_table: str, batch_path: str) -> dict[str, Any] | None:
    from psycopg2 import sql

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                SELECT
                    batch_path,
                    keyword_score_version,
                    keyword_config_hash,
                    article_keyword_row_count,
                    keyword_daily_source_row_count,
                    loaded_at
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
            "keyword_score_version": row[1],
            "keyword_config_hash": row[2],
            "article_keyword_row_count": row[3],
            "keyword_daily_source_row_count": row[4],
            "loaded_at": row[5],
        }


def delete_existing_batch_rows(
    *,
    connection: Any,
    article_keywords_table: str,
    keyword_daily_source_table: str,
    batch_path: str,
) -> None:
    from psycopg2 import sql

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("DELETE FROM {article_keywords_table} WHERE batch_path = %s").format(
                article_keywords_table=sql.Identifier(article_keywords_table)
            ),
            (batch_path,),
        )
        cursor.execute(
            sql.SQL("DELETE FROM {keyword_daily_source_table} WHERE batch_path = %s").format(
                keyword_daily_source_table=sql.Identifier(keyword_daily_source_table)
            ),
            (batch_path,),
        )


def iter_dataframe_chunks(dataframe: Any, chunk_size: int) -> Iterable[list[dict[str, Any]]]:
    chunk: list[dict[str, Any]] = []

    for row in dataframe.toLocalIterator():
        chunk.append(row.asDict(recursive=True))
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []

    if chunk:
        yield chunk


def upsert_article_keyword_rows(
    *,
    connection: Any,
    table_name: str,
    row_chunks: Iterable[list[dict[str, Any]]],
) -> int:
    from psycopg2 import sql
    from psycopg2.extras import execute_values

    total_rows = 0
    query = sql.SQL(
        """
        INSERT INTO {table_name} (
            batch_path,
            event_date,
            source,
            link,
            title,
            keyword,
            keyword_normalized,
            ngram_size,
            base_score,
            quality_penalty,
            article_score,
            quality_flags,
            keyword_score_version,
            keyword_config_hash,
            rank_in_article
        )
        VALUES %s
        ON CONFLICT (batch_path, link, keyword_normalized) DO UPDATE SET
            event_date = EXCLUDED.event_date,
            source = EXCLUDED.source,
            title = EXCLUDED.title,
            keyword = EXCLUDED.keyword,
            ngram_size = EXCLUDED.ngram_size,
            base_score = EXCLUDED.base_score,
            quality_penalty = EXCLUDED.quality_penalty,
            article_score = EXCLUDED.article_score,
            quality_flags = EXCLUDED.quality_flags,
            keyword_score_version = EXCLUDED.keyword_score_version,
            keyword_config_hash = EXCLUDED.keyword_config_hash,
            rank_in_article = EXCLUDED.rank_in_article,
            loaded_at = NOW()
        """
    ).format(table_name=sql.Identifier(table_name))

    with connection.cursor() as cursor:
        for rows in row_chunks:
            values = [
                (
                    row["batch_path"],
                    _ensure_date(row["event_date"]),
                    row["source"],
                    row["link"],
                    row["title"],
                    row["keyword"],
                    row["keyword_normalized"],
                    int(row["ngram_size"]),
                    float(row.get("base_score") or 0.0),
                    float(row.get("quality_penalty") or 0.0),
                    float(row["article_score"]),
                    row.get("quality_flags") or "",
                    row.get("keyword_score_version") or "",
                    row.get("keyword_config_hash") or "",
                    int(row["rank_in_article"]),
                )
                for row in rows
            ]
            execute_values(cursor, query.as_string(connection), values, page_size=len(values))
            total_rows += len(values)

    return total_rows


def upsert_keyword_daily_source_rows(
    *,
    connection: Any,
    table_name: str,
    row_chunks: Iterable[list[dict[str, Any]]],
) -> int:
    from psycopg2 import sql
    from psycopg2.extras import execute_values

    total_rows = 0
    query = sql.SQL(
        """
        INSERT INTO {table_name} (
            batch_path,
            event_date,
            source,
            keyword,
            keyword_normalized,
            ngram_size,
            article_count,
            base_score,
            quality_penalty,
            weighted_score,
            avg_article_score,
            quality_flags,
            keyword_score_version,
            keyword_config_hash,
            source_spread_score,
            recency_score,
            breakout_score,
            final_keyword_score,
            rank_in_group
        )
        VALUES %s
        ON CONFLICT (batch_path, event_date, source, keyword_normalized) DO UPDATE SET
            keyword = EXCLUDED.keyword,
            ngram_size = EXCLUDED.ngram_size,
            article_count = EXCLUDED.article_count,
            base_score = EXCLUDED.base_score,
            quality_penalty = EXCLUDED.quality_penalty,
            weighted_score = EXCLUDED.weighted_score,
            avg_article_score = EXCLUDED.avg_article_score,
            quality_flags = EXCLUDED.quality_flags,
            keyword_score_version = EXCLUDED.keyword_score_version,
            keyword_config_hash = EXCLUDED.keyword_config_hash,
            source_spread_score = EXCLUDED.source_spread_score,
            recency_score = EXCLUDED.recency_score,
            breakout_score = EXCLUDED.breakout_score,
            final_keyword_score = EXCLUDED.final_keyword_score,
            rank_in_group = EXCLUDED.rank_in_group,
            loaded_at = NOW()
        """
    ).format(table_name=sql.Identifier(table_name))

    with connection.cursor() as cursor:
        for rows in row_chunks:
            values = [
                (
                    row["batch_path"],
                    _ensure_date(row["event_date"]),
                    row["source"],
                    row["keyword"],
                    row["keyword_normalized"],
                    int(row["ngram_size"]),
                    int(row["article_count"]),
                    float(row.get("base_score") or 0.0),
                    float(row.get("quality_penalty") or 0.0),
                    float(row["weighted_score"]),
                    float(row["avg_article_score"]),
                    row.get("quality_flags") or "",
                    row.get("keyword_score_version") or "",
                    row.get("keyword_config_hash") or "",
                    float(row.get("source_spread_score") or 0.0),
                    float(row.get("recency_score") or 0.0),
                    float(row.get("breakout_score") or 0.0),
                    float(row.get("final_keyword_score") or 0.0),
                    int(row["rank_in_group"]),
                )
                for row in rows
            ]
            execute_values(cursor, query.as_string(connection), values, page_size=len(values))
            total_rows += len(values)

    return total_rows


def mark_batch_loaded(
    *,
    connection: Any,
    history_table: str,
    batch_path: str,
    keyword_score_version: str,
    keyword_config_hash: str,
    article_keyword_row_count: int,
    keyword_daily_source_row_count: int,
) -> None:
    from psycopg2 import sql

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO {history_table} (
                    batch_path,
                    keyword_score_version,
                    keyword_config_hash,
                    article_keyword_row_count,
                    keyword_daily_source_row_count,
                    loaded_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (batch_path) DO UPDATE SET
                    keyword_score_version = EXCLUDED.keyword_score_version,
                    keyword_config_hash = EXCLUDED.keyword_config_hash,
                    article_keyword_row_count = EXCLUDED.article_keyword_row_count,
                    keyword_daily_source_row_count = EXCLUDED.keyword_daily_source_row_count,
                    loaded_at = NOW()
                """
            ).format(history_table=sql.Identifier(history_table)),
            (
                batch_path,
                keyword_score_version,
                keyword_config_hash,
                article_keyword_row_count,
                keyword_daily_source_row_count,
            ),
        )


def load_keyword_batch_with_spark(
    *,
    connection: Any,
    batch_path: str,
    hdfs_default_fs: str,
    app_name: str,
    chunk_size: int,
    article_keywords_table: str,
    keyword_daily_source_table: str,
) -> tuple[int, int]:
    from pyspark.sql import functions as F

    spark = create_spark_session(app_name)
    try:
        article_keywords_uri = build_hdfs_uri(f"{batch_path}/article_keywords", hdfs_default_fs)
        article_keywords_df = (
            spark.read.parquet(article_keywords_uri)
            .select(
                F.trim(F.coalesce(F.col("batch_path"), F.lit(""))).alias("batch_path"),
                F.col("event_date").cast("date").alias("event_date"),
                F.trim(F.coalesce(F.col("source"), F.lit(""))).alias("source"),
                F.trim(F.coalesce(F.col("link"), F.lit(""))).alias("link"),
                F.trim(F.coalesce(F.col("title"), F.lit(""))).alias("title"),
                F.trim(F.coalesce(F.col("keyword"), F.lit(""))).alias("keyword"),
                F.trim(F.coalesce(F.col("keyword_normalized"), F.lit(""))).alias("keyword_normalized"),
                F.col("ngram_size").cast("int").alias("ngram_size"),
                F.col("base_score").cast("double").alias("base_score"),
                F.col("quality_penalty").cast("double").alias("quality_penalty"),
                F.col("article_score").cast("double").alias("article_score"),
                F.trim(F.coalesce(F.col("quality_flags"), F.lit(""))).alias("quality_flags"),
                F.trim(F.coalesce(F.col("keyword_score_version"), F.lit(""))).alias("keyword_score_version"),
                F.trim(F.coalesce(F.col("keyword_config_hash"), F.lit(""))).alias("keyword_config_hash"),
                F.col("rank_in_article").cast("int").alias("rank_in_article"),
            )
            .where(
                (F.col("batch_path") != "")
                & F.col("event_date").isNotNull()
                & (F.col("source") != "")
                & (F.col("link") != "")
                & (F.col("title") != "")
                & (F.col("keyword") != "")
                & (F.col("keyword_normalized") != "")
                & F.col("ngram_size").isNotNull()
                & F.col("base_score").isNotNull()
                & F.col("quality_penalty").isNotNull()
                & F.col("article_score").isNotNull()
                & (F.col("keyword_score_version") != "")
                & (F.col("keyword_config_hash") != "")
                & F.col("rank_in_article").isNotNull()
            )
        )

        article_keyword_row_count = upsert_article_keyword_rows(
            connection=connection,
            table_name=article_keywords_table,
            row_chunks=iter_dataframe_chunks(article_keywords_df, chunk_size),
        )

        keyword_daily_source_uri = build_hdfs_uri(f"{batch_path}/keyword_daily_source", hdfs_default_fs)
        keyword_daily_source_df = (
            spark.read.parquet(keyword_daily_source_uri)
            .select(
                F.trim(F.coalesce(F.col("batch_path"), F.lit(""))).alias("batch_path"),
                F.col("event_date").cast("date").alias("event_date"),
                F.trim(F.coalesce(F.col("source"), F.lit(""))).alias("source"),
                F.trim(F.coalesce(F.col("keyword"), F.lit(""))).alias("keyword"),
                F.trim(F.coalesce(F.col("keyword_normalized"), F.lit(""))).alias("keyword_normalized"),
                F.col("ngram_size").cast("int").alias("ngram_size"),
                F.col("article_count").cast("int").alias("article_count"),
                F.col("base_score").cast("double").alias("base_score"),
                F.col("quality_penalty").cast("double").alias("quality_penalty"),
                F.col("weighted_score").cast("double").alias("weighted_score"),
                F.col("avg_article_score").cast("double").alias("avg_article_score"),
                F.trim(F.coalesce(F.col("quality_flags"), F.lit(""))).alias("quality_flags"),
                F.trim(F.coalesce(F.col("keyword_score_version"), F.lit(""))).alias("keyword_score_version"),
                F.trim(F.coalesce(F.col("keyword_config_hash"), F.lit(""))).alias("keyword_config_hash"),
                F.col("source_spread_score").cast("double").alias("source_spread_score"),
                F.col("recency_score").cast("double").alias("recency_score"),
                F.col("breakout_score").cast("double").alias("breakout_score"),
                F.col("final_keyword_score").cast("double").alias("final_keyword_score"),
                F.col("rank_in_group").cast("int").alias("rank_in_group"),
            )
            .where(
                (F.col("batch_path") != "")
                & F.col("event_date").isNotNull()
                & (F.col("source") != "")
                & (F.col("keyword") != "")
                & (F.col("keyword_normalized") != "")
                & F.col("ngram_size").isNotNull()
                & F.col("article_count").isNotNull()
                & F.col("base_score").isNotNull()
                & F.col("quality_penalty").isNotNull()
                & F.col("weighted_score").isNotNull()
                & F.col("avg_article_score").isNotNull()
                & (F.col("keyword_score_version") != "")
                & (F.col("keyword_config_hash") != "")
                & F.col("source_spread_score").isNotNull()
                & F.col("recency_score").isNotNull()
                & F.col("breakout_score").isNotNull()
                & F.col("final_keyword_score").isNotNull()
                & F.col("rank_in_group").isNotNull()
            )
        )

        keyword_daily_source_row_count = upsert_keyword_daily_source_rows(
            connection=connection,
            table_name=keyword_daily_source_table,
            row_chunks=iter_dataframe_chunks(keyword_daily_source_df, chunk_size),
        )
        return article_keyword_row_count, keyword_daily_source_row_count
    finally:
        spark.stop()


def main() -> None:
    logger = configure_logging("keyword_loader")
    started_at = time.perf_counter()
    args = parse_args()

    from hdfs import InsecureClient

    os.environ["HDFS_DEFAULT_FS"] = args.hdfs_default_fs
    hdfs_client = InsecureClient(args.hdfs_url, user=args.hdfs_user)
    batch_path = resolve_explicit_or_latest_path(
        hdfs_client,
        explicit_path=args.input_batch_path,
        fallback_path=args.input_path,
        latest_resolver=resolve_latest_keyword_batch,
    )
    batch_metadata = read_keyword_batch_metadata(
        hdfs_client=hdfs_client,
        batch_path=batch_path,
        hdfs_url=args.hdfs_url,
        hdfs_user=args.hdfs_user,
        redirect_host=args.webhdfs_redirect_host,
    )
    batch_uri = build_hdfs_uri(batch_path, args.hdfs_default_fs)

    log_event(
        logger,
        20,
        "keyword_load_started",
        input_path=batch_path,
        input_uri=batch_uri,
        keyword_score_version=batch_metadata["keyword_score_version"],
        keyword_config_hash=batch_metadata["keyword_config_hash"],
        db_host=args.db_host,
        db_name=args.db_name,
        status="running",
    )

    connection = connect_to_postgres(args)
    try:
        ensure_keyword_tables(
            connection=connection,
            history_table=args.history_table,
            article_keywords_table=args.article_keywords_table,
            keyword_daily_source_table=args.keyword_daily_source_table,
        )

        loaded_batch_metadata = get_loaded_batch_metadata(
            connection=connection,
            history_table=args.history_table,
            batch_path=batch_path,
        )
        same_config_loaded = (
            loaded_batch_metadata is not None
            and str(loaded_batch_metadata.get("keyword_config_hash", "")) == batch_metadata["keyword_config_hash"]
        )
        if same_config_loaded and not args.force_reload:
            log_event(
                logger,
                20,
                "keyword_load_skipped_already_loaded_batch",
                input_path=batch_path,
                keyword_score_version=batch_metadata["keyword_score_version"],
                keyword_config_hash=batch_metadata["keyword_config_hash"],
                status="success",
                duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
            )
            return

        if loaded_batch_metadata is not None:
            delete_existing_batch_rows(
                connection=connection,
                article_keywords_table=args.article_keywords_table,
                keyword_daily_source_table=args.keyword_daily_source_table,
                batch_path=batch_path,
            )

        article_keyword_row_count, keyword_daily_source_row_count = load_keyword_batch_with_spark(
            connection=connection,
            batch_path=batch_path,
            hdfs_default_fs=args.hdfs_default_fs,
            app_name=args.app_name,
            chunk_size=max(args.chunk_size, 1),
            article_keywords_table=args.article_keywords_table,
            keyword_daily_source_table=args.keyword_daily_source_table,
        )

        mark_batch_loaded(
            connection=connection,
            history_table=args.history_table,
            batch_path=batch_path,
            keyword_score_version=batch_metadata["keyword_score_version"],
            keyword_config_hash=batch_metadata["keyword_config_hash"],
            article_keyword_row_count=article_keyword_row_count,
            keyword_daily_source_row_count=keyword_daily_source_row_count,
        )
        connection.commit()

        log_event(
            logger,
            20,
            "keyword_load_completed",
            input_path=batch_path,
            input_uri=batch_uri,
            article_keyword_row_count=article_keyword_row_count,
            keyword_daily_source_row_count=keyword_daily_source_row_count,
            force_reload=args.force_reload,
            keyword_score_version=batch_metadata["keyword_score_version"],
            keyword_config_hash=batch_metadata["keyword_config_hash"],
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
