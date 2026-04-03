from __future__ import annotations

import argparse
import os
import tempfile
import time
from pathlib import Path, PurePosixPath

from common.hdfs_utils import (
    read_hdfs_bytes,
    resolve_latest_hdfs_file,
    upload_directory_to_hdfs,
)
from common.logging_utils import configure_logging, log_event


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform raw HDFS news JSONL into processed Parquet using PySpark."
    )
    parser.add_argument("--input-path", default=os.getenv("HDFS_RAW_PATH", "/news/raw"))
    parser.add_argument("--output-path", default=os.getenv("HDFS_PROCESSED_PATH", "/news/processed"))
    parser.add_argument("--hdfs-url", default=os.getenv("HDFS_URL", "http://localhost:9870"))
    parser.add_argument("--hdfs-user", default=os.getenv("HDFS_USER", "root"))
    parser.add_argument(
        "--webhdfs-redirect-host",
        default=os.getenv("WEBHDFS_REDIRECT_HOST", ""),
        help="Override the hostname returned by WebHDFS redirects when running outside Docker.",
    )
    parser.add_argument(
        "--app-name",
        default="news-raw-to-processed",
        help="Spark application name.",
    )
    return parser.parse_args()


def build_processed_output_path(input_path: str, output_base_path: str) -> str:
    input_parts = PurePosixPath(input_path).parts
    if len(input_parts) < 4:
        raise ValueError(f"Unsupported raw input path: {input_path}")

    year, month, day, filename = input_parts[-4:]
    batch_name = PurePosixPath(filename).stem
    return str(PurePosixPath(output_base_path, year, month, day, batch_name))


def create_spark_session(app_name: str):
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def transform_local_json_to_parquet(
    *,
    local_input_path: str,
    local_output_dir: str,
    app_name: str,
) -> tuple[int, dict[str, object]]:
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    spark = create_spark_session(app_name)
    raw_df = None
    valid_rows_df = None
    processed_df = None

    schema = T.StructType(
        [
            T.StructField("title", T.StringType(), True),
            T.StructField("link", T.StringType(), True),
            T.StructField("summary", T.StringType(), True),
            T.StructField("published_at", T.StringType(), True),
            T.StructField("source", T.StringType(), True),
            T.StructField("fetched_at", T.StringType(), True),
            T.StructField("ingestion_id", T.StringType(), True),
        ]
    )

    try:
        raw_df = spark.read.schema(schema).json(local_input_path).persist()

        published_ts = F.coalesce(
            F.to_timestamp(F.col("published_at")),
            F.to_timestamp(F.col("published_at"), "EEE, dd MMM yyyy HH:mm:ss z"),
            F.to_timestamp(F.col("published_at"), "EEE, dd MMM yyyy HH:mm:ss Z"),
        )

        fetched_ts = F.coalesce(
            F.to_timestamp(F.col("fetched_at")),
            F.to_timestamp(F.col("fetched_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        )

        raw_metrics = raw_df.agg(
            F.count(F.lit(1)).alias("raw_count"),
            F.sum(
                F.when(F.trim(F.coalesce(F.col("title"), F.lit(""))) == "", F.lit(1)).otherwise(F.lit(0))
            ).alias("missing_title_count"),
            F.sum(
                F.when(F.trim(F.coalesce(F.col("link"), F.lit(""))) == "", F.lit(1)).otherwise(F.lit(0))
            ).alias("missing_link_count"),
        ).collect()[0]
        raw_count = int(raw_metrics["raw_count"])
        missing_title_count = int(raw_metrics["missing_title_count"])
        missing_link_count = int(raw_metrics["missing_link_count"])

        valid_rows_df = (
            raw_df.select(
                F.trim(F.coalesce(F.col("title"), F.lit(""))).alias("title"),
                F.trim(F.coalesce(F.col("link"), F.lit(""))).alias("link"),
                F.trim(F.coalesce(F.col("summary"), F.lit(""))).alias("summary"),
                F.trim(F.coalesce(F.col("published_at"), F.lit(""))).alias("published_at"),
                F.trim(F.coalesce(F.col("source"), F.lit(""))).alias("source"),
                F.trim(F.coalesce(F.col("fetched_at"), F.lit(""))).alias("fetched_at"),
                F.trim(F.coalesce(F.col("ingestion_id"), F.lit(""))).alias("ingestion_id"),
            )
            .withColumn("published_at", published_ts)
            .withColumn("fetched_at", fetched_ts)
            .where(
                (F.col("title") != "")
                & (F.col("link") != "")
                & (F.col("source") != "")
                & (F.col("ingestion_id") != "")
                & F.col("published_at").isNotNull()
                & F.col("fetched_at").isNotNull()
            )
        ).persist()
        valid_row_count = int(valid_rows_df.agg(F.count(F.lit(1)).alias("valid_row_count")).collect()[0]["valid_row_count"])
        processed_df = (
            valid_rows_df
            .withColumn("event_date", F.to_date(F.col("published_at")))
            .dropDuplicates(["link"])
        ).persist()

        source_counts = {
            row["source"]: row["count"]
            for row in processed_df.groupBy("source").count().collect()
        }
        record_count = int(sum(source_counts.values()))
        if record_count == 0:
            raise SystemExit("No valid rows remained after Spark transformation.")

        processed_df.write.mode("overwrite").parquet(local_output_dir)
        metrics = {
            "raw_count": raw_count,
            "valid_row_count": valid_row_count,
            "missing_title_count": missing_title_count,
            "missing_title_rate": round((missing_title_count / raw_count), 4) if raw_count else 0.0,
            "missing_link_count": missing_link_count,
            "missing_link_rate": round((missing_link_count / raw_count), 4) if raw_count else 0.0,
            "duplicate_count": valid_row_count - record_count,
            "articles_by_source": source_counts,
        }
        return record_count, metrics
    finally:
        if processed_df is not None:
            processed_df.unpersist()
        if valid_rows_df is not None:
            valid_rows_df.unpersist()
        if raw_df is not None:
            raw_df.unpersist()
        spark.stop()


def main() -> None:
    logger = configure_logging("spark_transform")
    started_at = time.perf_counter()
    args = parse_args()
    from hdfs import InsecureClient

    client = InsecureClient(args.hdfs_url, user=args.hdfs_user)
    source_path = resolve_latest_hdfs_file(client, args.input_path)
    target_path = build_processed_output_path(source_path, args.output_path)

    with tempfile.TemporaryDirectory(prefix="news-transform-") as temp_dir:
        local_input_path = str(Path(temp_dir) / "input.jsonl")
        local_output_dir = str(Path(temp_dir) / "processed")

        input_bytes = read_hdfs_bytes(
            hdfs_url=args.hdfs_url,
            hdfs_user=args.hdfs_user,
            path=source_path,
            redirect_host=args.webhdfs_redirect_host,
        )
        Path(local_input_path).write_bytes(input_bytes)

        record_count, metrics = transform_local_json_to_parquet(
            local_input_path=local_input_path,
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
        "spark_processed_write_completed",
        input_path=source_path,
        output_path=target_path,
        row_count=record_count,
        raw_count=metrics["raw_count"],
        valid_row_count=metrics["valid_row_count"],
        missing_title_count=metrics["missing_title_count"],
        missing_title_rate=metrics["missing_title_rate"],
        missing_link_count=metrics["missing_link_count"],
        missing_link_rate=metrics["missing_link_rate"],
        duplicate_count=metrics["duplicate_count"],
        articles_by_source=metrics["articles_by_source"],
        duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
        status="success",
    )


if __name__ == "__main__":
    main()
