from __future__ import annotations

import argparse
import hashlib
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
    "keyword_score_version": "v2",
    "top_keywords_per_article": 10,
    "top_keywords_per_day_source": 25,
    "min_token_length": 2,
    "max_ngram_size": 3,
    "title_weight": 2.0,
    "summary_weight": 1.0,
    "min_keyword_score": 1.0,
    "summary_only_penalty": 0.35,
    "weak_term_penalty": 0.25,
    "source_spread_weight": 0.2,
    "recency_weight": 0.15,
    "recency_window_days": 7,
    "breakout_weight": 0.15,
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
    "blocked_phrases": [
        "chi tiet",
        "chi tiết",
        "moi nhat",
        "mới nhất",
        "theo doi",
        "theo dõi",
        "xem them",
        "xem thêm",
    ],
    "weak_terms": [
        "goc",
        "góc",
        "ngay",
        "ngày",
        "tin",
        "truoc",
        "trước",
    ],
}

URL_PATTERN = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
TOKEN_PATTERN = re.compile(r"[0-9A-Za-zÀ-ỹĐđ]+", re.UNICODE)
WHITESPACE_PATTERN = re.compile(r"\s+")
KEYWORD_METADATA_FILENAME = "_keyword_metadata.json"


def default_keyword_settings_path() -> str:
    return str(Path(__file__).resolve().parents[1] / "config" / "keyword_settings.json")


def default_stopwords_path() -> str:
    return str(Path(__file__).resolve().parents[1] / "config" / "stopwords_vi.txt")


def default_source_blocklist_path() -> str:
    return str(Path(__file__).resolve().parents[1] / "config" / "source_keyword_blocklist.json")


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
        "--source-blocklist-path",
        default=os.getenv("KEYWORD_SOURCE_BLOCKLIST_PATH", default_source_blocklist_path()),
        help="JSON file mapping sources to keyword phrases that should be blocked for that source.",
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
    settings["blocked_phrases"] = normalize_keyword_terms(settings.get("blocked_phrases", []))
    settings["weak_terms"] = normalize_keyword_terms(settings.get("weak_terms", []))
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


def normalize_source_key(value: Any) -> str:
    return str(value or "").strip().lower()


def load_source_keyword_blocklist(source_blocklist_path: str) -> dict[str, set[str]]:
    if not Path(source_blocklist_path).exists():
        return {}

    with Path(source_blocklist_path).open("r", encoding="utf-8") as blocklist_file:
        loaded_blocklist = json.load(blocklist_file)

    if not isinstance(loaded_blocklist, dict):
        raise ValueError("Source keyword blocklist file must contain a JSON object.")

    normalized_blocklist: dict[str, set[str]] = {}
    for source_key, values in loaded_blocklist.items():
        normalized_source_key = normalize_source_key(source_key)
        normalized_blocklist[normalized_source_key] = set(normalize_keyword_terms(values))
    return normalized_blocklist


def build_keyword_config_hash(
    *,
    settings: dict[str, Any],
    stopwords: set[str],
    source_blocklist: dict[str, set[str]],
) -> str:
    payload = {
        "settings": settings,
        "stopwords": sorted(stopwords),
        "source_blocklist": {
            source_key: sorted(values)
            for source_key, values in sorted(source_blocklist.items())
        },
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


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


def collect_ngram_counts(segments: list[list[str]], max_ngram_size: int) -> Counter[tuple[str, int]]:
    counts: Counter[tuple[str, int]] = Counter()
    for segment in segments:
        for keyword, ngram_size in generate_ngrams(segment, max_ngram_size):
            if is_valid_ngram(keyword.split(" ")):
                counts[(keyword, ngram_size)] += 1
    return counts


def resolve_source_blocked_phrases(
    source: Any,
    source_blocklist: dict[str, set[str]],
) -> set[str]:
    blocked_phrases = set(source_blocklist.get("default", set()))
    blocked_phrases.update(source_blocklist.get(normalize_source_key(source), set()))
    return blocked_phrases


def score_keywords_for_article(
    *,
    source: Any,
    title: Any,
    summary: Any,
    settings: dict[str, Any],
    stopwords: set[str],
    source_blocklist: dict[str, set[str]],
    keyword_config_hash: str,
) -> list[dict[str, Any]]:
    min_token_length = int(settings["min_token_length"])
    max_ngram_size = int(settings["max_ngram_size"])
    title_weight = float(settings["title_weight"])
    summary_weight = float(settings["summary_weight"])
    min_keyword_score = float(settings["min_keyword_score"])
    top_keywords_per_article = int(settings["top_keywords_per_article"])
    blocked_terms = set(normalize_keyword_terms(settings.get("blocked_terms", [])))
    blocked_phrases = set(normalize_keyword_terms(settings.get("blocked_phrases", [])))
    weak_terms = set(normalize_keyword_terms(settings.get("weak_terms", [])))
    summary_only_penalty = float(settings["summary_only_penalty"])
    weak_term_penalty = float(settings["weak_term_penalty"])

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
    title_ngram_counts = collect_ngram_counts(title_segments, max_ngram_size)
    summary_ngram_counts = collect_ngram_counts(summary_segments, max_ngram_size)
    source_specific_blocked_phrases = resolve_source_blocked_phrases(source, source_blocklist)

    ranked_keywords: list[dict[str, Any]] = []
    for keyword_key in set(title_ngram_counts) | set(summary_ngram_counts):
        keyword, ngram_size = keyword_key
        if keyword in blocked_phrases or keyword in source_specific_blocked_phrases:
            continue

        title_frequency = int(title_ngram_counts.get(keyword_key, 0))
        summary_frequency = int(summary_ngram_counts.get(keyword_key, 0))
        base_score = (title_frequency * title_weight) + (summary_frequency * summary_weight)

        quality_flags: list[str] = []
        quality_penalty = 0.0

        if title_frequency == 0 and summary_frequency > 0:
            quality_flags.append("summary_only")
            quality_penalty += summary_only_penalty

        weak_term_matches = sum(1 for token in keyword.split(" ") if token in weak_terms)
        if weak_term_matches:
            quality_flags.append("contains_weak_term")
            quality_penalty += weak_term_matches * weak_term_penalty

        final_score = max(base_score - quality_penalty, 0.0)
        if final_score < min_keyword_score:
            continue

        ranked_keywords.append(
            {
                "keyword": keyword,
                "keyword_normalized": keyword,
                "ngram_size": ngram_size,
                "base_score": float(base_score),
                "quality_penalty": round(float(quality_penalty), 4),
                "article_score": round(float(final_score), 4),
                "quality_flags": ",".join(sorted(quality_flags)),
                "keyword_score_version": str(settings["keyword_score_version"]),
                "keyword_config_hash": keyword_config_hash,
            }
        )
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
    settings: dict[str, Any],
    stopwords: set[str],
    source_blocklist: dict[str, set[str]],
    keyword_config_hash: str,
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
            T.StructField("base_score", T.DoubleType(), False),
            T.StructField("quality_penalty", T.DoubleType(), False),
            T.StructField("article_score", T.DoubleType(), False),
            T.StructField("quality_flags", T.StringType(), False),
            T.StructField("keyword_score_version", T.StringType(), False),
            T.StructField("keyword_config_hash", T.StringType(), False),
            T.StructField("rank_in_article", T.IntegerType(), False),
        ]
    )
    extract_keywords_udf = F.udf(
        lambda source, title, summary: score_keywords_for_article(
            source=source,
            title=title,
            summary=summary,
            settings=settings,
            stopwords=stopwords,
            source_blocklist=source_blocklist,
            keyword_config_hash=keyword_config_hash,
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
            curated_df.withColumn("keyword_candidates", extract_keywords_udf("source", "title", "summary"))
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
                F.col("keyword_candidate.base_score").alias("base_score"),
                F.col("keyword_candidate.quality_penalty").alias("quality_penalty"),
                F.col("keyword_candidate.article_score").alias("article_score"),
                F.col("keyword_candidate.quality_flags").alias("quality_flags"),
                F.col("keyword_candidate.keyword_score_version").alias("keyword_score_version"),
                F.col("keyword_candidate.keyword_config_hash").alias("keyword_config_hash"),
                F.col("keyword_candidate.rank_in_article").alias("rank_in_article"),
            )
        ).persist()

        article_keyword_count = int(article_keywords_df.count())
        if article_keyword_count == 0:
            raise SystemExit("No keyword candidates remained after keyword extraction.")

        latest_event_date_window = Window.partitionBy("batch_path")
        breakout_window = (
            Window.partitionBy("batch_path", "source", "keyword_normalized")
            .orderBy(F.col("event_date").asc())
            .rowsBetween(Window.unboundedPreceding, -1)
        )
        daily_group_window = Window.partitionBy("batch_path", "event_date", "source").orderBy(
            F.col("final_keyword_score").desc(),
            F.col("article_count").desc(),
            F.col("ngram_size").desc(),
            F.col("keyword_normalized").asc(),
        )

        keyword_source_spread_df = article_keywords_df.groupBy(
            "batch_path",
            "event_date",
            "keyword_normalized",
        ).agg(F.countDistinct("source").cast("int").alias("source_count_for_keyword"))

        keyword_daily_source_df = (
            article_keywords_df.groupBy(
                "batch_path",
                "event_date",
                "source",
                "keyword",
                "keyword_normalized",
                "ngram_size",
                "keyword_score_version",
                "keyword_config_hash",
            )
            .agg(
                F.countDistinct("link").cast("int").alias("article_count"),
                F.sum("base_score").alias("base_score"),
                F.sum("quality_penalty").alias("quality_penalty"),
                F.sum("article_score").alias("weighted_score"),
                F.avg("article_score").alias("avg_article_score"),
                F.array_sort(
                    F.array_distinct(
                        F.flatten(F.collect_list(F.split(F.coalesce(F.col("quality_flags"), F.lit("")), ",")))
                    )
                ).alias("quality_flags_array"),
            )
            .join(
                keyword_source_spread_df,
                on=["batch_path", "event_date", "keyword_normalized"],
                how="left",
            )
            .withColumn(
                "source_spread_score",
                F.greatest(F.col("source_count_for_keyword") - F.lit(1), F.lit(0)).cast("double")
                * F.lit(float(settings["source_spread_weight"])),
            )
            .withColumn("latest_event_date", F.max("event_date").over(latest_event_date_window))
            .withColumn(
                "recency_score",
                (
                    F.greatest(
                        F.lit(float(settings["recency_window_days"]))
                        - F.datediff(F.col("latest_event_date"), F.col("event_date")).cast("double"),
                        F.lit(0.0),
                    )
                    / F.lit(max(float(settings["recency_window_days"]), 1.0))
                )
                * F.lit(float(settings["recency_weight"])),
            )
            .withColumn("previous_avg_weighted_score", F.avg("weighted_score").over(breakout_window))
            .withColumn(
                "breakout_score",
                F.greatest(
                    F.col("weighted_score") - F.coalesce(F.col("previous_avg_weighted_score"), F.lit(0.0)),
                    F.lit(0.0),
                )
                * F.lit(float(settings["breakout_weight"])),
            )
            .withColumn(
                "final_keyword_score",
                F.col("weighted_score")
                + F.col("source_spread_score")
                + F.col("recency_score")
                + F.col("breakout_score"),
            )
            .withColumn(
                "quality_flags",
                F.concat_ws(
                    ",",
                    F.array_except(F.col("quality_flags_array"), F.array(F.lit(""))),
                ),
            )
            .withColumn("rank_in_group", F.row_number().over(daily_group_window))
            .where(F.col("rank_in_group") <= int(settings["top_keywords_per_day_source"]))
            .drop("quality_flags_array", "source_count_for_keyword", "latest_event_date", "previous_avg_weighted_score")
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
    source_blocklist = load_source_keyword_blocklist(args.source_blocklist_path)
    keyword_config_hash = build_keyword_config_hash(
        settings=settings,
        stopwords=stopwords,
        source_blocklist=source_blocklist,
    )

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
        source_blocklist=source_blocklist,
        keyword_config_hash=keyword_config_hash,
    )
    metadata_payload = {
        "batch_path": source_batch_path,
        "keyword_output_path": target_path,
        "keyword_score_version": str(settings["keyword_score_version"]),
        "keyword_config_hash": keyword_config_hash,
        "curated_row_count": metrics["curated_row_count"],
        "article_keyword_count": metrics["article_keyword_count"],
        "keyword_daily_source_count": metrics["keyword_daily_source_count"],
    }
    client.write(
        f"{target_path}/{KEYWORD_METADATA_FILENAME}",
        data=json.dumps(metadata_payload, ensure_ascii=False, indent=2).encode("utf-8"),
        overwrite=True,
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
        keyword_score_version=str(settings["keyword_score_version"]),
        keyword_config_hash=keyword_config_hash,
        duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
        status="success",
    )


if __name__ == "__main__":
    main()
