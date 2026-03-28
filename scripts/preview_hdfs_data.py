import argparse
import json
import os
import re
import textwrap
from html import unescape

from common.hdfs_utils import read_hdfs_lines, resolve_latest_hdfs_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preview HDFS news data in a readable format."
    )
    parser.add_argument(
        "--path",
        default="/news/raw",
        help="HDFS file or directory to preview. If a directory is given, the latest file is used.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of records to display.",
    )
    parser.add_argument(
        "--summary-width",
        type=int,
        default=100,
        help="Maximum width used when wrapping the summary text.",
    )
    parser.add_argument("--hdfs-url", default=os.getenv("HDFS_URL", "http://localhost:9870"))
    parser.add_argument("--hdfs-user", default=os.getenv("HDFS_USER", "root"))
    parser.add_argument(
        "--webhdfs-redirect-host",
        default=os.getenv("WEBHDFS_REDIRECT_HOST", ""),
        help="Override the hostname returned by WebHDFS redirects when running outside Docker.",
    )
    return parser.parse_args()


def main() -> None:
    from hdfs import InsecureClient

    args = parse_args()
    client = InsecureClient(args.hdfs_url, user=args.hdfs_user)
    target_path = resolve_latest_hdfs_file(client, args.path)

    print(f"Previewing HDFS file: {target_path}")
    print()

    for index, line in enumerate(
        read_hdfs_lines(
            hdfs_url=args.hdfs_url,
            hdfs_user=args.hdfs_user,
            path=target_path,
            redirect_host=args.webhdfs_redirect_host,
        ),
        start=1,
    ):
        if index > args.limit:
            break

        article = json.loads(line)
        print(format_article(index, article, args.summary_width))
        print("-" * 80)

def format_article(index: int, article: dict, summary_width: int) -> str:
    summary = clean_summary(article.get("summary", ""))
    wrapped_summary = textwrap.fill(
        summary or "(empty summary)",
        width=summary_width,
        initial_indent="summary     : ",
        subsequent_indent="              ",
    )

    lines = [
        f"[{index}] {article.get('title', '(no title)')}",
        f"source      : {article.get('source', '-')}",
        f"published   : {article.get('published_at', '-')}",
        f"fetched_at  : {article.get('fetched_at', '-')}",
        f"link        : {article.get('link', '-')}",
        wrapped_summary,
    ]
    return "\n".join(lines)


def clean_summary(summary: str) -> str:
    no_html = re.sub(r"<[^>]+>", " ", summary)
    normalized = re.sub(r"\s+", " ", unescape(no_html)).strip()
    return normalized


if __name__ == "__main__":
    main()
