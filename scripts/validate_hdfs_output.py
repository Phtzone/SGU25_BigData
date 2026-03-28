import argparse
import json
import os

from common.hdfs_utils import list_hdfs_files


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


def main() -> None:
    args = parse_args()
    from hdfs import InsecureClient

    client = InsecureClient(args.hdfs_url, user=args.hdfs_user)

    if not client.status(args.path, strict=False):
        raise SystemExit(f"HDFS path does not exist: {args.path}")

    files = list_hdfs_files(client, args.path)
    file_count = len(files)
    if file_count == 0:
        raise SystemExit(f"No HDFS files found under {args.path}")

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

    print(f"Found {file_count} HDFS file(s) under {args.path}")
    print(f"Latest file: {latest_file}")

if __name__ == "__main__":
    main()
