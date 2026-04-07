import argparse
import json
import os

from common.hdfs_utils import list_hdfs_files
from common.logging_utils import configure_logging, log_event


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate that HDFS output exists for the news pipeline."
    )
    parser.add_argument("--path", default="/news/raw")
    parser.add_argument("--hdfs-url", default=os.getenv("HDFS_URL", "http://localhost:9870"))
    parser.add_argument("--hdfs-user", default=os.getenv("HDFS_USER", "root"))
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print validation details as JSON for scripts.",
    )
    return parser.parse_args()


def resolve_raw_files(client, path: str) -> list[tuple[str, dict]]:
    status = client.status(path, strict=False)
    if not status:
        raise SystemExit(f"HDFS path does not exist: {path}")

    if status["type"] == "FILE":
        return [(path, status)]

    files = list_hdfs_files(client, path)
    if not files:
        raise SystemExit(f"No HDFS files found under {path}")

    return files


def main() -> None:
    args = parse_args()
    logger = configure_logging("validate_raw") if not args.json else None
    from hdfs import InsecureClient

    client = InsecureClient(args.hdfs_url, user=args.hdfs_user)
    files = resolve_raw_files(client, args.path)
    file_count = len(files)
    latest_file = max(files, key=lambda item: item[1]["modificationTime"])[0]
    if args.json:
        print(
            json.dumps(
                {
                    "path": args.path,
                    "file_count": file_count,
                    "latest_file": latest_file,
                }
            )
        )
        return

    log_event(
        logger,
        20,
        "raw_zone_validation_completed",
        output_path=args.path,
        row_count=file_count,
        latest_file=latest_file,
        status="success",
    )

if __name__ == "__main__":
    main()
