from __future__ import annotations

import argparse
import os
import time
from pathlib import Path, PurePosixPath

from Spark_jobs.transform_news_raw_to_processed import create_spark_session
from common.hdfs_utils import (
    build_hdfs_uri,
    derive_hdfs_default_fs,
    list_hdfs_files,
    resolve_explicit_or_latest_path,
)
from common.logging_utils import configure_logging, log_event


def parse_args() -> argparse.Namespace:
    default_hdfs_url = os.getenv("HDFS_URL", "http://localhost:9870")
    parser = argparse.ArgumentParser(
        description="Curate processed news Parquet into an analytics-ready HDFS zone."
    )
    parser.add_argument("--input-path", default=os.getenv("HDFS_PROCESSED_PATH", "/news/processed"))
    parser.add_argument(
        "--input-batch-path",
        default="",
        help="Optional exact processed HDFS batch path. When provided, this batch is used instead of resolving the latest processed batch.",
    )
    parser.add_argument("--output-path", default=os.getenv("HDFS_CURATED_PATH", "/news/curated"))
    parser.add_argument("--hdfs-url", default=default_hdfs_url)
    parser.add_argument("--hdfs-user", default=os.getenv("HDFS_USER", "root"))
    parser.add_argument(
        "--hdfs-default-fs",
        default=os.getenv("HDFS_DEFAULT_FS", derive_hdfs_default_fs(default_hdfs_url)),
        help="Spark-accessible HDFS root, for example hdfs://namenode:9000.",
    )
    parser.add_argument(
        "--webhdfs-redirect-host",
        default=os.getenv("WEBHDFS_REDIRECT_HOST", ""),
        help="Override the hostname returned by WebHDFS redirects when running outside Docker.",
    )
    parser.add_argument(
        "--app-name",
        default="news-processed-to-curated",
        help="Spark application name.",
    )
    parser.add_argument(
        "--write-output-path-file",
        default="",
        help="Optional local file used to persist the exact curated HDFS batch path for downstream tasks.",
    )
    return parser.parse_args()


def resolve_latest_processed_batch(client, path: str) -> str:
    status = client.status(path, strict=False)
    if not status:
        raise SystemExit(f"HDFS path does not exist: {path}")

    if status["type"] == "FILE":
        return str(PurePosixPath(path).parent)

    parquet_files = [item for item in list_hdfs_files(client, path) if item[0].endswith(".parquet")]
    if not parquet_files:
        raise SystemExit(f"No processed Parquet files found under {path}")

    latest_parquet = max(parquet_files, key=lambda item: item[1]["modificationTime"])[0]
    return str(PurePosixPath(latest_parquet).parent)


def build_curated_output_path(processed_batch_path: str, output_base_path: str) -> str:
    input_parts = PurePosixPath(processed_batch_path).parts
    if len(input_parts) < 4:
        raise ValueError(f"Unsupported processed batch path: {processed_batch_path}")

    year, month, day, batch_name = input_parts[-4:]
    return str(PurePosixPath(output_base_path, year, month, day, batch_name))


def write_output_path_file(path_file: str, output_path: str) -> None:
    if not path_file.strip():
        return

    path = Path(path_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(output_path + "\n", encoding="utf-8")


def transform_hdfs_processed_to_curated(
    *,
    input_uri: str,
    output_uri: str,
    app_name: str,
) -> tuple[int, dict[str, object]]:
    from pyspark.sql import functions as F

    spark = create_spark_session(app_name)
    processed_df = None
    curated_df = None

    try:
        processed_df = spark.read.parquet(input_uri).persist()
        input_row_count = int(processed_df.agg(F.count(F.lit(1)).alias("input_row_count")).collect()[0]["input_row_count"])

        curated_df = (
            processed_df.select(
                F.trim(F.coalesce(F.col("title"), F.lit(""))).alias("title"),
                F.trim(F.coalesce(F.col("link"), F.lit(""))).alias("link"),
                F.trim(F.coalesce(F.col("summary"), F.lit(""))).alias("summary"),
                F.col("published_at").cast("timestamp").alias("published_at"),
                F.trim(F.coalesce(F.col("source"), F.lit(""))).alias("source"),
                F.col("fetched_at").cast("timestamp").alias("fetched_at"),
                F.trim(F.coalesce(F.col("ingestion_id"), F.lit(""))).alias("ingestion_id"),
            )
            .where(
                (F.col("title") != "")
                & (F.col("link") != "")
                & (F.col("source") != "")
                & (F.col("ingestion_id") != "")
                & F.col("published_at").isNotNull()
                & F.col("fetched_at").isNotNull()
            )
            .withColumn("event_date", F.to_date(F.col("published_at")))
            .dropDuplicates(["link"])
        ).persist()

        source_counts = {
            row["source"]: row["count"]
            for row in curated_df.groupBy("source").count().collect()
        }
        record_count = int(sum(source_counts.values()))
        if record_count == 0:
            raise SystemExit("No valid rows remained after curated transformation.")

        partition_count = int(
            curated_df.select("event_date", "source")
            .distinct()
            .agg(F.count(F.lit(1)).alias("partition_count"))
            .collect()[0]["partition_count"]
        )
        curated_df.write.mode("overwrite").partitionBy("event_date", "source").parquet(output_uri)
        metrics = {
            "input_row_count": input_row_count,
            "duplicate_count": input_row_count - record_count,
            "articles_by_source": source_counts,
            "partition_count": partition_count,
        }
        return record_count, metrics
    finally:
        if curated_df is not None:
            curated_df.unpersist()
        if processed_df is not None:
            processed_df.unpersist()
        spark.stop()


def main() -> None:
    logger = configure_logging("spark_curated")
    started_at = time.perf_counter()
    args = parse_args()
    from hdfs import InsecureClient

    os.environ["HDFS_DEFAULT_FS"] = args.hdfs_default_fs
    client = InsecureClient(args.hdfs_url, user=args.hdfs_user)
    source_batch_path = resolve_explicit_or_latest_path(
        client,
        explicit_path=args.input_batch_path,
        fallback_path=args.input_path,
        latest_resolver=resolve_latest_processed_batch,
    )
    target_path = build_curated_output_path(source_batch_path, args.output_path)
    source_uri = build_hdfs_uri(source_batch_path, args.hdfs_default_fs)
    target_uri = build_hdfs_uri(target_path, args.hdfs_default_fs)

    record_count, metrics = transform_hdfs_processed_to_curated(
        input_uri=source_uri,
        output_uri=target_uri,
        app_name=args.app_name,
    )
    write_output_path_file(args.write_output_path_file, target_path)

    log_event(
        logger,
        20,
        "spark_curated_write_completed",
        input_path=source_batch_path,
        input_uri=source_uri,
        output_path=target_path,
        output_uri=target_uri,
        row_count=record_count,
        input_row_count=metrics["input_row_count"],
        duplicate_count=metrics["duplicate_count"],
        articles_by_source=metrics["articles_by_source"],
        partition_count=metrics["partition_count"],
        duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
        status="success",
    )


if __name__ == "__main__":
    main()
