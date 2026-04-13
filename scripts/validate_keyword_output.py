import argparse
import json
import os
from pathlib import PurePosixPath

from common.hdfs_utils import list_hdfs_files
from common.logging_utils import configure_logging, log_event

KEYWORD_DATASET_NAMES = ("article_keywords", "keyword_daily_source")
KEYWORD_METADATA_FILENAME = "_keyword_metadata.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate that keyword Parquet output exists for the news pipeline."
    )
    parser.add_argument("--path", default=os.getenv("HDFS_KEYWORDS_PATH", "/news/keywords"))
    parser.add_argument("--hdfs-url", default=os.getenv("HDFS_URL", "http://localhost:9870"))
    parser.add_argument("--hdfs-user", default=os.getenv("HDFS_USER", "root"))
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print validation details as JSON for scripts.",
    )
    return parser.parse_args()


def resolve_keyword_batch_from_parquet(parquet_path: str) -> str:
    path = PurePosixPath(parquet_path)
    if len(path.parents) < 2:
        raise SystemExit(f"Unexpected keyword file layout: {parquet_path}")
    return str(path.parents[1])


def resolve_latest_keyword_batch(client, path: str) -> str:
    status = client.status(path, strict=False)
    if not status:
        raise SystemExit(f"HDFS path does not exist: {path}")

    if status["type"] == "FILE":
        if not path.endswith(".parquet"):
            raise SystemExit(f"Expected a Parquet file but got: {path}")
        return resolve_keyword_batch_from_parquet(path)

    parquet_files = [item for item in list_hdfs_files(client, path) if item[0].endswith(".parquet")]
    if not parquet_files:
        raise SystemExit(f"No keyword Parquet files found under {path}")

    latest_parquet = max(parquet_files, key=lambda item: item[1]["modificationTime"])[0]
    return resolve_keyword_batch_from_parquet(latest_parquet)


def belongs_to_dataset(path: str, dataset_name: str) -> bool:
    return dataset_name in PurePosixPath(path).parts


def main() -> None:
    args = parse_args()
    logger = configure_logging("validate_keywords") if not args.json else None
    from hdfs import InsecureClient

    client = InsecureClient(args.hdfs_url, user=args.hdfs_user)

    if not client.status(args.path, strict=False):
        raise SystemExit(f"HDFS path does not exist: {args.path}")

    files = list_hdfs_files(client, args.path)
    parquet_files = [item for item in files if item[0].endswith(".parquet")]
    if not parquet_files:
        raise SystemExit(f"No keyword Parquet files found under {args.path}")

    article_keyword_files = [
        item for item in parquet_files if belongs_to_dataset(item[0], "article_keywords")
    ]
    keyword_daily_source_files = [
        item for item in parquet_files if belongs_to_dataset(item[0], "keyword_daily_source")
    ]
    if not article_keyword_files:
        raise SystemExit("Keyword batch is missing article_keywords Parquet output.")
    if not keyword_daily_source_files:
        raise SystemExit("Keyword batch is missing keyword_daily_source Parquet output.")

    success_markers = [item for item in files if PurePosixPath(item[0]).name == "_SUCCESS"]
    metadata_files = [item for item in files if PurePosixPath(item[0]).name == KEYWORD_METADATA_FILENAME]
    latest_parquet = max(parquet_files, key=lambda item: item[1]["modificationTime"])[0]
    latest_batch = resolve_keyword_batch_from_parquet(latest_parquet)
    if not metadata_files:
        raise SystemExit("Keyword batch is missing _keyword_metadata.json metadata output.")

    metadata_path = max(metadata_files, key=lambda item: item[1]["modificationTime"])[0]
    with client.read(metadata_path, encoding="utf-8") as metadata_file:
        metadata_payload = json.load(metadata_file)

    payload = {
        "path": args.path,
        "latest_batch": latest_batch,
        "article_keyword_parquet_count": len(article_keyword_files),
        "keyword_daily_source_parquet_count": len(keyword_daily_source_files),
        "success_marker_count": len(success_markers),
        "latest_parquet_file": latest_parquet,
        "metadata_path": metadata_path,
        "keyword_score_version": metadata_payload.get("keyword_score_version", ""),
        "keyword_config_hash": metadata_payload.get("keyword_config_hash", ""),
        "datasets": list(KEYWORD_DATASET_NAMES),
    }

    if args.json:
        print(json.dumps(payload))
        return

    log_event(
        logger,
        20,
        "keyword_zone_validation_completed",
        output_path=args.path,
        latest_batch=latest_batch,
        article_keyword_parquet_count=len(article_keyword_files),
        keyword_daily_source_parquet_count=len(keyword_daily_source_files),
        success_marker_count=len(success_markers),
        latest_parquet_file=latest_parquet,
        metadata_path=metadata_path,
        keyword_score_version=metadata_payload.get("keyword_score_version", ""),
        keyword_config_hash=metadata_payload.get("keyword_config_hash", ""),
        status="success",
    )


if __name__ == "__main__":
    main()
