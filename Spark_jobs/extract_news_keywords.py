from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections import Counter
from html import unescape
from pathlib import Path, PurePosixPath
from typing import Any

from Spark_jobs.transform_news_raw_to_processed import create_spark_session
from common.hdfs_utils import (
    build_hdfs_uri,
    derive_hdfs_default_fs,
    list_hdfs_files,
    resolve_explicit_or_latest_path,
)
from common.logging_utils import configure_logging, log_event

DEFAULT_KEYWORD_SETTINGS = {
    "top_keywords_per_article": 10,
    "top_keywords_per_day_source": 25,
    "min_token_length": 2,
    "max_ngram_size": 3,
    "title_weight": 2.0,
    "summary_weight": 1.0,
    "min_keyword_score": 1.0,
    "blocked_terms": [
        "anh",
        "ảnh",
        "bai",
        "bài",
        "bao",
        "báo",
        "clip",
        "doc",
        "đọc",
        "hinh",
        "hình",
        "link",
        "moi",
        "mới",
        "ngay",
        "nhat",
        "nhất",
        "tin",
        "video",
        "xem",
    ],
}

URL_PATTERN = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
TOKEN_PATTERN = re.compile(r"[0-9A-Za-zÀ-ỹĐđ]+", re.UNICODE)
WHITESPACE_PATTERN = re.compile(r"\s+")


def default_keyword_settings_path() -> str:
    return str(Path(__file__).resolve().parents[1] / "config" / "keyword_settings.json")


def default_stopwords_path() -> str:
    return str(Path(__file__).resolve().parents[1] / "config" / "stopwords_vi.txt")


def parse_args() -> argparse.Namespace:
    default_hdfs_url = os.getenv("HDFS_URL", "http://localhost:9870")
    parser = argparse.ArgumentParser(
        description="Extract keyword candidates from curated news Parquet batches."
    )
    parser.add_argument("--input-path", default=os.getenv("HDFS_CURATED_PATH", "/news/curated"))
    parser.add_argument(
        "--input-batch-path",
        default="",
        help="Optional exact curated HDFS batch path. When provided, this batch is used instead of resolving the latest curated batch.",
    )
    parser.add_argument("--output-path", default=os.getenv("HDFS_KEYWORDS_PATH", "/news/keywords"))
    parser.add_argument("--hdfs-url", default=default_hdfs_url)
    parser.add_argument("--hdfs-user", default=os.getenv("HDFS_USER", "root"))
    parser.add_argument(
        "--hdfs-default-fs",
        default=os.getenv("HDFS_DEFAULT_FS", derive_hdfs_default_fs(default_hdfs_url)),
        help="Spark-accessible HDFS root, for example hdfs://namenode:9000.",
    )
    parser.add_argument(
        "--config-path",
        default=os.getenv("KEYWORD_SETTINGS_PATH", default_keyword_settings_path()),
        help="JSON config file for keyword extraction settings.",
    )
    parser.add_argument(
        "--stopwords-path",
        default=os.getenv("KEYWORD_STOPWORDS_PATH", default_stopwords_path()),
        help="UTF-8 text file with one Vietnamese stopword per line.",
    )
    parser.add_argument(
        "--app-name",
        default="news-curated-to-keywords",
        help="Spark application name.",
    )
    parser.add_argument(
        "--write-output-path-file",
        default="",
        help="Optional local file used to persist the exact keyword HDFS batch path for downstream tasks.",
    )
    return parser.parse_args()


def resolve_curated_batch_from_parquet(parquet_path: str) -> str:
    path = PurePosixPath(parquet_path)
    parts = path.parts

    for index, part in enumerate(parts):
        if part.startswith("event_date="):
            if index == 0:
                break
            return str(PurePosixPath(*parts[:index]))

    return str(path.parent)


def resolve_latest_curated_batch(client: Any, path: str) -> str:
    status = client.status(path, strict=False)
    if not status:
        raise SystemExit(f"HDFS path does not exist: {path}")

    if status["type"] == "FILE":
        if not path.endswith(".parquet"):
            raise SystemExit(f"Expected a Parquet file but got: {path}")
        return resolve_curated_batch_from_parquet(path)

    parquet_files = [item for item in list_hdfs_files(client, path) if item[0].endswith(".parquet")]
    if not parquet_files:
        raise SystemExit(f"No curated Parquet files found under {path}")

    latest_parquet = max(parquet_files, key=lambda item: item[1]["modificationTime"])[0]
    return resolve_curated_batch_from_parquet(latest_parquet)


def build_keyword_output_path(curated_batch_path: str, output_base_path: str) -> str:
    input_parts = PurePosixPath(curated_batch_path).parts
    if len(input_parts) < 4:
        raise ValueError(f"Unsupported curated batch path: {curated_batch_path}")

    year, month, day, batch_name = input_parts[-4:]
    return str(PurePosixPath(output_base_path, year, month, day, batch_name))


def write_output_path_file(path_file: str, output_path: str) -> None:
    if not path_file.strip():
        return

    path = Path(path_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(output_path + "\n", encoding="utf-8")


def load_keyword_settings(config_path: str) -> dict[str, Any]:
    settings: dict[str, Any] = dict(DEFAULT_KEYWORD_SETTINGS)

    with Path(config_path).open("r", encoding="utf-8") as config_file:
        loaded_settings = json.load(config_file)

    if not isinstance(loaded_settings, dict):
        raise ValueError("Keyword settings file must contain a JSON object.")

    settings.update(loaded_settings)
    settings["blocked_terms"] = normalize_keyword_terms(settings.get("blocked_terms", []))
    return settings


def load_stopwords(stopwords_path: str) -> set[str]:
    stopwords: set[str] = set()
    with Path(stopwords_path).open("r", encoding="utf-8") as stopwords_file:
        for line in stopwords_file:
            normalized = normalize_keyword_text(line)
            if normalized:
                stopwords.add(normalized)
    return stopwords


def normalize_keyword_terms(values: Any) -> list[str]:
    if values is None:
        return []
    if not isinstance(values, list):
        raise ValueError("Keyword term lists must be JSON arrays.")

    normalized_terms: list[str] = []
    seen_terms: set[str] = set()
    for value in values:
        normalized = normalize_keyword_text(value)
        if not normalized or normalized in seen_terms:
            continue
        seen_terms.add(normalized)
        normalized_terms.append(normalized)
    return normalized_terms


def normalize_keyword_text(value: Any) -> str:
    if value is None:
        return ""

    text = unescape(str(value)).lower()
    text = URL_PATTERN.sub(" ", text)
    text = text.replace("_", " ")
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = WHITESPACE_PATTERN.sub(" ", text).strip()
    return text


def tokenize_keyword_text(text: str) -> list[str]:
    return TOKEN_PATTERN.findall(text)


def is_valid_keyword_token(
    token: str,
    *,
    stopwords: set[str],
    blocked_terms: set[str],
    min_token_length: int,
) -> bool:
    normalized = token.strip()
    if len(normalized) < min_token_length:
        return False
    if normalized.isdigit():
        return False
    if normalized in stopwords:
        return False
    if normalized in blocked_terms:
        return False
    return True


def filter_tokens(
    tokens: list[str],
    *,
    stopwords: set[str],
    blocked_terms: set[str] | None = None,
    min_token_length: int,
) -> list[str]:
    filtered_tokens: list[str] = []
    normalized_blocked_terms = blocked_terms or set()

    for token in tokens:
        normalized = token.strip()
        if is_valid_keyword_token(
            normalized,
            stopwords=stopwords,
            blocked_terms=normalized_blocked_terms,
            min_token_length=min_token_length,
        ):
            filtered_tokens.append(normalized)

    return filtered_tokens


def split_candidate_token_segments(
    tokens: list[str],
    *,
    stopwords: set[str],
    blocked_terms: set[str],
    min_token_length: int,
) -> list[list[str]]:
    segments: list[list[str]] = []
    current_segment: list[str] = []

    for token in tokens:
        normalized = token.strip()
        if is_valid_keyword_token(
            normalized,
            stopwords=stopwords,
            blocked_terms=blocked_terms,
            min_token_length=min_token_length,
        ):
            current_segment.append(normalized)
            continue

        if current_segment:
            segments.append(current_segment)
            current_segment = []

    if current_segment:
        segments.append(current_segment)

    return segments


def generate_ngrams(tokens: list[str], max_ngram_size: int) -> list[tuple[str, int]]:
    ngrams: list[tuple[str, int]] = []

    for ngram_size in range(1, max_ngram_size + 1):
        if len(tokens) < ngram_size:
            break

        for index in range(len(tokens) - ngram_size + 1):
            ngram_tokens = tokens[index : index + ngram_size]
            ngrams.append((" ".join(ngram_tokens), ngram_size))

    return ngrams


def is_valid_ngram(tokens: list[str]) -> bool:
    if not tokens:
        return False
    if len(tokens) > 1 and len(set(tokens)) != len(tokens):
        return False
    return True


def score_keywords_for_article(
    *,
    title: Any,
    summary: Any,
    settings: dict[str, Any],
    stopwords: set[str],
) -> list[dict[str, Any]]:
    min_token_length = int(settings["min_token_length"])
    max_ngram_size = int(settings["max_ngram_size"])
    title_weight = float(settings["title_weight"])
    summary_weight = float(settings["summary_weight"])
    min_keyword_score = float(settings["min_keyword_score"])
    top_keywords_per_article = int(settings["top_keywords_per_article"])
    blocked_terms = set(normalize_keyword_terms(settings.get("blocked_terms", [])))

    title_segments = split_candidate_token_segments(
        tokenize_keyword_text(normalize_keyword_text(title)),
        stopwords=stopwords,
        blocked_terms=blocked_terms,
        min_token_length=min_token_length,
    )
    summary_segments = split_candidate_token_segments(
        tokenize_keyword_text(normalize_keyword_text(summary)),
        stopwords=stopwords,
        blocked_terms=blocked_terms,
        min_token_length=min_token_length,
    )

    keyword_scores: Counter[tuple[str, int]] = Counter()
    for segment in title_segments:
        for keyword, ngram_size in generate_ngrams(segment, max_ngram_size):
            if is_valid_ngram(keyword.split(" ")):
                keyword_scores[(keyword, ngram_size)] += title_weight
    for segment in summary_segments:
        for keyword, ngram_size in generate_ngrams(segment, max_ngram_size):
            if is_valid_ngram(keyword.split(" ")):
                keyword_scores[(keyword, ngram_size)] += summary_weight

    ranked_keywords = [
        {
            "keyword": keyword,
            "keyword_normalized": keyword,
            "ngram_size": ngram_size,
            "article_score": float(score),
        }
        for (keyword, ngram_size), score in keyword_scores.items()
        if score >= min_keyword_score
    ]
    ranked_keywords.sort(
        key=lambda item: (
            -item["article_score"],
            -item["ngram_size"],
            item["keyword_normalized"],
        )
    )

    top_ranked_keywords = ranked_keywords[:top_keywords_per_article]
    for rank, item in enumerate(top_ranked_keywords, start=1):
        item["rank_in_article"] = rank

    return top_ranked_keywords


def extract_keywords_from_curated_batch(
    *,
    input_uri: str,
    output_path: str,
    hdfs_default_fs: str,
    app_name: str,
    batch_path: str,
    settings: dict[str, float | int],
    stopwords: set[str],
) -> dict[str, Any]:
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window

    spark = create_spark_session(app_name)
    curated_df = None
    article_keywords_df = None
    keyword_daily_source_df = None

    keyword_struct = T.StructType(
        [
            T.StructField("keyword", T.StringType(), False),
            T.StructField("keyword_normalized", T.StringType(), False),
            T.StructField("ngram_size", T.IntegerType(), False),
            T.StructField("article_score", T.DoubleType(), False),
            T.StructField("rank_in_article", T.IntegerType(), False),
        ]
    )
    extract_keywords_udf = F.udf(
        lambda title, summary: score_keywords_for_article(
            title=title,
            summary=summary,
            settings=settings,
            stopwords=stopwords,
        ),
        T.ArrayType(keyword_struct),
    )

    try:
        curated_df = (
            spark.read.parquet(input_uri)
            .select(
                F.col("event_date").cast("date").alias("event_date"),
                F.trim(F.coalesce(F.col("source"), F.lit(""))).alias("source"),
                F.trim(F.coalesce(F.col("link"), F.lit(""))).alias("link"),
                F.trim(F.coalesce(F.col("title"), F.lit(""))).alias("title"),
                F.trim(F.coalesce(F.col("summary"), F.lit(""))).alias("summary"),
            )
            .where(
                F.col("event_date").isNotNull()
                & (F.col("source") != "")
                & (F.col("link") != "")
                & (F.col("title") != "")
            )
        ).persist()

        curated_row_count = int(curated_df.count())
        if curated_row_count == 0:
            raise SystemExit("No curated rows available for keyword extraction.")

        article_keywords_df = (
            curated_df.withColumn("keyword_candidates", extract_keywords_udf("title", "summary"))
            .withColumn("keyword_candidate", F.explode("keyword_candidates"))
            .select(
                F.lit(batch_path).alias("batch_path"),
                "event_date",
                "source",
                "link",
                "title",
                F.col("keyword_candidate.keyword").alias("keyword"),
                F.col("keyword_candidate.keyword_normalized").alias("keyword_normalized"),
                F.col("keyword_candidate.ngram_size").alias("ngram_size"),
                F.col("keyword_candidate.article_score").alias("article_score"),
                F.col("keyword_candidate.rank_in_article").alias("rank_in_article"),
            )
        ).persist()

        article_keyword_count = int(article_keywords_df.count())
        if article_keyword_count == 0:
            raise SystemExit("No keyword candidates remained after keyword extraction.")

        daily_group_window = Window.partitionBy("batch_path", "event_date", "source").orderBy(
            F.col("weighted_score").desc(),
            F.col("article_count").desc(),
            F.col("ngram_size").desc(),
            F.col("keyword_normalized").asc(),
        )

        keyword_daily_source_df = (
            article_keywords_df.groupBy(
                "batch_path",
                "event_date",
                "source",
                "keyword",
                "keyword_normalized",
                "ngram_size",
            )
            .agg(
                F.countDistinct("link").cast("int").alias("article_count"),
                F.sum("article_score").alias("weighted_score"),
                F.avg("article_score").alias("avg_article_score"),
            )
            .withColumn("rank_in_group", F.row_number().over(daily_group_window))
            .where(F.col("rank_in_group") <= int(settings["top_keywords_per_day_source"]))
        ).persist()

        keyword_daily_source_count = int(keyword_daily_source_df.count())

        article_output_uri = build_hdfs_uri(f"{output_path}/article_keywords", hdfs_default_fs)
        daily_output_uri = build_hdfs_uri(f"{output_path}/keyword_daily_source", hdfs_default_fs)
        article_keywords_df.write.mode("overwrite").parquet(article_output_uri)
        keyword_daily_source_df.write.mode("overwrite").parquet(daily_output_uri)

        source_counts = {
            row["source"]: row["count"]
            for row in curated_df.groupBy("source").count().collect()
        }

        return {
            "curated_row_count": curated_row_count,
            "article_keyword_count": article_keyword_count,
            "keyword_daily_source_count": keyword_daily_source_count,
            "articles_by_source": source_counts,
        }
    finally:
        if keyword_daily_source_df is not None:
            keyword_daily_source_df.unpersist()
        if article_keywords_df is not None:
            article_keywords_df.unpersist()
        if curated_df is not None:
            curated_df.unpersist()
        spark.stop()


def main() -> None:
    logger = configure_logging("spark_keywords")
    started_at = time.perf_counter()
    args = parse_args()

    from hdfs import InsecureClient

    settings = load_keyword_settings(args.config_path)
    stopwords = load_stopwords(args.stopwords_path)

    os.environ["HDFS_DEFAULT_FS"] = args.hdfs_default_fs
    client = InsecureClient(args.hdfs_url, user=args.hdfs_user)
    source_batch_path = resolve_explicit_or_latest_path(
        client,
        explicit_path=args.input_batch_path,
        fallback_path=args.input_path,
        latest_resolver=resolve_latest_curated_batch,
    )
    target_path = build_keyword_output_path(source_batch_path, args.output_path)
    source_uri = build_hdfs_uri(source_batch_path, args.hdfs_default_fs)
    target_uri = build_hdfs_uri(target_path, args.hdfs_default_fs)

    metrics = extract_keywords_from_curated_batch(
        input_uri=source_uri,
        output_path=target_path,
        hdfs_default_fs=args.hdfs_default_fs,
        app_name=args.app_name,
        batch_path=source_batch_path,
        settings=settings,
        stopwords=stopwords,
    )
    write_output_path_file(args.write_output_path_file, target_path)

    log_event(
        logger,
        20,
        "spark_keywords_write_completed",
        input_path=source_batch_path,
        input_uri=source_uri,
        output_path=target_path,
        output_uri=target_uri,
        curated_row_count=metrics["curated_row_count"],
        article_keyword_count=metrics["article_keyword_count"],
        keyword_daily_source_count=metrics["keyword_daily_source_count"],
        articles_by_source=metrics["articles_by_source"],
        duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
        status="success",
    )


if __name__ == "__main__":
    main()
