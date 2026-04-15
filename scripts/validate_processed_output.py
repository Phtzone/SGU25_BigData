import argparse
import json
import os
from pathlib import PurePosixPath

from common.hdfs_utils import list_hdfs_files
from common.logging_utils import configure_logging, log_event


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate that processed Parquet output exists for the news pipeline."
    )
    parser.add_argument("--path", default="/news/processed")
    parser.add_argument("--hdfs-url", default=os.getenv("HDFS_URL", "http://localhost:9870"))
    parser.add_argument("--hdfs-user", default=os.getenv("HDFS_USER", "root"))
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print validation details as JSON for scripts.",
    )
    return parser.parse_args()


def resolve_batch_path(latest_parquet: str) -> str:
    path = PurePosixPath(latest_parquet)
    if not path.parent or str(path.parent) == ".":
        raise SystemExit(f"Unexpected processed file layout: {latest_parquet}")
    return str(path.parent)


def main() -> None:
    args = parse_args()
    logger = configure_logging("validate_processed") if not args.json else None
    from hdfs import InsecureClient

    client = InsecureClient(args.hdfs_url, user=args.hdfs_user)

    if not client.status(args.path, strict=False):
        raise SystemExit(f"HDFS path does not exist: {args.path}")

    files = list_hdfs_files(client, args.path)
    parquet_files = [item for item in files if item[0].endswith(".parquet")]
    success_markers = [item for item in files if PurePosixPath(item[0]).name == "_SUCCESS"]

    if not parquet_files:
        raise SystemExit(f"No Parquet files found under {args.path}")

    latest_parquet = max(parquet_files, key=lambda item: item[1]["modificationTime"])[0]
    latest_batch = resolve_batch_path(latest_parquet)
    payload = {
        "path": args.path,
        "parquet_file_count": len(parquet_files),
        "success_marker_count": len(success_markers),
        "latest_batch": latest_batch,
        "latest_parquet_file": latest_parquet,
    }

    if args.json:
        print(json.dumps(payload))
        return

    log_event(
        logger,
        20,
        "processed_zone_validation_completed",
        output_path=args.path,
        row_count=len(parquet_files),
        success_marker_count=len(success_markers),
        latest_batch=latest_batch,
        latest_parquet_file=latest_parquet,
        status="success",
    )


if __name__ == "__main__":
    main()
