import argparse
import json
import os
from pathlib import PurePosixPath

from common.hdfs_utils import list_hdfs_files


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


def main() -> None:
    args = parse_args()
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
    latest_batch = str(PurePosixPath(latest_parquet).parent)
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

    print(f"Found {len(parquet_files)} Parquet file(s) under {args.path}")
    print(f"Found {len(success_markers)} Spark _SUCCESS marker(s)")
    print(f"Latest processed batch: {latest_batch}")


if __name__ == "__main__":
    main()
