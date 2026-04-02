from __future__ import annotations

import argparse
import os
import tempfile
import time
from pathlib import Path, PurePosixPath

from Spark_jobs.transform_news_raw_to_processed import create_spark_session
from common.hdfs_utils import (
    download_hdfs_directory,
    list_hdfs_files,
    upload_directory_to_hdfs,
)
from common.logging_utils import configure_logging, log_event


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Curate processed news Parquet into an analytics-ready HDFS zone."
    )
    parser.add_argument("--input-path", default=os.getenv("HDFS_PROCESSED_PATH", "/news/processed"))
    parser.add_argument("--output-path", default=os.getenv("HDFS_CURATED_PATH", "/news/curated"))
    parser.add_argument("--hdfs-url", default=os.getenv("HDFS_URL", "http://localhost:9870"))
    parser.add_argument("--hdfs-user", default=os.getenv("HDFS_USER", "root"))
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


def transform_local_processed_to_curated(
    *,
    local_input_dir: str,
    local_output_dir: str,
    app_name: str,
) -> tuple[int, dict[str, object]]:
    from pyspark.sql import functions as F

    spark = create_spark_session(app_name)

    try:
        processed_df = spark.read.parquet(local_input_dir)
        input_row_count = processed_df.count()

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
        )

        record_count = curated_df.count()
        if record_count == 0:
            raise SystemExit("No valid rows remained after curated transformation.")

        curated_df.write.mode("overwrite").partitionBy("event_date", "source").parquet(local_output_dir)
        source_counts = {
            row["source"]: row["count"]
            for row in curated_df.groupBy("source").count().collect()
        }
        metrics = {
            "input_row_count": input_row_count,
            "duplicate_count": input_row_count - record_count,
            "articles_by_source": source_counts,
            "partition_count": curated_df.select("event_date", "source").distinct().count(),
        }
        return record_count, metrics
    finally:
        spark.stop()


def main() -> None:
    logger = configure_logging("spark_curated")
    started_at = time.perf_counter()
    args = parse_args()
    from hdfs import InsecureClient

    client = InsecureClient(args.hdfs_url, user=args.hdfs_user)
    source_batch_path = resolve_latest_processed_batch(client, args.input_path)
    target_path = build_curated_output_path(source_batch_path, args.output_path)

    with tempfile.TemporaryDirectory(prefix="news-curated-") as temp_dir:
        local_input_dir = str(Path(temp_dir) / "processed")
        local_output_dir = str(Path(temp_dir) / "curated")

        download_hdfs_directory(
            client=client,
            hdfs_dir=source_batch_path,
            local_dir=local_input_dir,
            hdfs_url=args.hdfs_url,
            hdfs_user=args.hdfs_user,
            redirect_host=args.webhdfs_redirect_host,
        )

        record_count, metrics = transform_local_processed_to_curated(
            local_input_dir=local_input_dir,
            local_output_dir=local_output_dir,
            app_name=args.app_name,
        )

        client.delete(target_path, recursive=True)
        client.makedirs(target_path)
        upload_directory_to_hdfs(
            client=client,
            local_dir=local_output_dir,
            hdfs_dir=target_path,
            hdfs_url=args.hdfs_url,
            hdfs_user=args.hdfs_user,
            redirect_host=args.webhdfs_redirect_host,
        )

    log_event(
        logger,
        20,
        "spark_curated_write_completed",
        input_path=source_batch_path,
        output_path=target_path,
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
