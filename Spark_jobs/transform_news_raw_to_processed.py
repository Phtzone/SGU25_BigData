from __future__ import annotations

import argparse
import os
import shutil
import tempfile
import time
from pathlib import Path, PurePosixPath

from common.hdfs_utils import (
    build_hdfs_uri,
    derive_hdfs_default_fs,
    resolve_explicit_or_latest_path,
    resolve_latest_hdfs_file,
)
from common.logging_utils import configure_logging, log_event

COMMON_JAVA_HOME_CANDIDATES = (
    "/usr/lib/jvm/default-java",
    "/usr/lib/jvm/java-17-openjdk-amd64",
    "/usr/lib/jvm/java-17-openjdk",
    "/usr/lib/jvm/temurin-17-jdk-amd64",
    "/usr/lib/jvm/temurin-17-jdk",
    "/usr/lib/jvm/msopenjdk-17-amd64",
    "/usr/lib/jvm/msopenjdk-17",
)


def parse_args() -> argparse.Namespace:
    default_hdfs_url = os.getenv("HDFS_URL", "http://localhost:9870")
    parser = argparse.ArgumentParser(
        description="Transform raw HDFS news JSONL into processed Parquet using PySpark."
    )
    parser.add_argument("--input-path", default=os.getenv("HDFS_RAW_PATH", "/news/raw"))
    parser.add_argument(
        "--input-batch-path",
        default="",
        help="Optional exact raw HDFS file path. When provided, this batch is used instead of resolving the latest raw file.",
    )
    parser.add_argument("--output-path", default=os.getenv("HDFS_PROCESSED_PATH", "/news/processed"))
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
        default="news-raw-to-processed",
        help="Spark application name.",
    )
    parser.add_argument(
        "--write-output-path-file",
        default="",
        help="Optional local file used to persist the exact processed HDFS batch path for downstream tasks.",
    )
    return parser.parse_args()


def build_processed_output_path(input_path: str, output_base_path: str) -> str:
    input_parts = PurePosixPath(input_path).parts
    if len(input_parts) < 4:
        raise ValueError(f"Unsupported raw input path: {input_path}")

    year, month, day, filename = input_parts[-4:]
    batch_name = PurePosixPath(filename).stem
    return str(PurePosixPath(output_base_path, year, month, day, batch_name))


def write_output_path_file(path_file: str, output_path: str) -> None:
    if not path_file.strip():
        return

    path = Path(path_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(output_path + "\n", encoding="utf-8")


def is_valid_java_home(java_home: str | Path) -> bool:
    java_home_path = Path(java_home)
    java_binary = java_home_path / "bin" / "java"
    return java_binary.exists()


def infer_java_home_from_path() -> str | None:
    java_binary_path = shutil.which("java")
    if not java_binary_path:
        return None

    inferred_java_home = Path(java_binary_path).resolve().parent.parent
    if is_valid_java_home(inferred_java_home):
        return str(inferred_java_home)
    return None


def infer_java_home_from_common_locations() -> str | None:
    seen_paths: set[str] = set()

    for candidate in COMMON_JAVA_HOME_CANDIDATES:
        resolved_candidate = str(Path(candidate))
        if resolved_candidate in seen_paths:
            continue
        seen_paths.add(resolved_candidate)
        if is_valid_java_home(candidate):
            return resolved_candidate

    jvm_root = Path("/usr/lib/jvm")
    if jvm_root.exists():
        for candidate in sorted(jvm_root.glob("*17*")):
            resolved_candidate = str(candidate)
            if resolved_candidate in seen_paths:
                continue
            seen_paths.add(resolved_candidate)
            if is_valid_java_home(candidate):
                return resolved_candidate

    return None


def ensure_java_home() -> str:
    configured_java_home = os.getenv("JAVA_HOME", "").strip()
    if configured_java_home:
        if is_valid_java_home(configured_java_home):
            return str(Path(configured_java_home))

    inferred_java_home = infer_java_home_from_path()
    if inferred_java_home:
        os.environ["JAVA_HOME"] = inferred_java_home
        return inferred_java_home

    inferred_java_home = infer_java_home_from_common_locations()
    if inferred_java_home:
        os.environ["JAVA_HOME"] = inferred_java_home
        return inferred_java_home

    if configured_java_home:
        raise SystemExit(
            f"JAVA_HOME is set but invalid: {configured_java_home}. "
            "Expected to find a Java binary at $JAVA_HOME/bin/java, and no usable 'java' executable "
            "was found in PATH or common WSL/Linux JVM locations. Update JAVA_HOME or install OpenJDK 17."
        )

    raise SystemExit(
        "Java 17 is required for local PySpark jobs, but JAVA_HOME is not configured and no usable "
        "'java' executable was found in PATH or common WSL/Linux JVM locations. Install OpenJDK 17 and "
        "export JAVA_HOME before running Spark jobs. Example on Ubuntu/WSL: sudo apt-get install -y "
        "openjdk-17-jre-headless && export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64"
    )


def create_spark_session(app_name: str):
    from pyspark.sql import SparkSession

    ensure_java_home()
    spark_local_dir = Path(os.getenv("SPARK_LOCAL_DIR", str(Path(tempfile.gettempdir()) / "spark-local")))
    spark_warehouse_dir = Path(
        os.getenv("SPARK_WAREHOUSE_DIR", str(Path(tempfile.gettempdir()) / "spark-warehouse"))
    )
    spark_local_dir.mkdir(parents=True, exist_ok=True)
    spark_warehouse_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", str(spark_local_dir))
        .config("spark.sql.warehouse.dir", spark_warehouse_dir.resolve().as_uri())
        .getOrCreate()
    )
    spark.conf.set("spark.hadoop.fs.defaultFS", os.getenv("HDFS_DEFAULT_FS", "hdfs://localhost:9000"))
    spark.sparkContext.setLogLevel("WARN")
    return spark


def transform_hdfs_json_to_parquet(
    *,
    input_uri: str,
    output_uri: str,
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
        raw_df = spark.read.schema(schema).json(input_uri).persist()

        published_ts = F.to_timestamp(F.col("published_at"))
        fetched_ts = F.to_timestamp(F.col("fetched_at"))

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

        processed_df.write.mode("overwrite").parquet(output_uri)
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

    os.environ["HDFS_DEFAULT_FS"] = args.hdfs_default_fs
    client = InsecureClient(args.hdfs_url, user=args.hdfs_user)
    source_path = resolve_explicit_or_latest_path(
        client,
        explicit_path=args.input_batch_path,
        fallback_path=args.input_path,
        latest_resolver=resolve_latest_hdfs_file,
    )
    target_path = build_processed_output_path(source_path, args.output_path)
    source_uri = build_hdfs_uri(source_path, args.hdfs_default_fs)
    target_uri = build_hdfs_uri(target_path, args.hdfs_default_fs)

    record_count, metrics = transform_hdfs_json_to_parquet(
        input_uri=source_uri,
        output_uri=target_uri,
        app_name=args.app_name,
    )
    write_output_path_file(args.write_output_path_file, target_path)

    log_event(
        logger,
        20,
        "spark_processed_write_completed",
        input_path=source_path,
        input_uri=source_uri,
        output_path=target_path,
        output_uri=target_uri,
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
