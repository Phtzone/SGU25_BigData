import argparse
import json
import os
import re
import textwrap
from html import unescape
from urllib.parse import urlsplit, urlunsplit

from hdfs import InsecureClient
import requests


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
    args = parse_args()
    client = InsecureClient(args.hdfs_url, user=args.hdfs_user)
    target_path = resolve_target_file(client, args.path)

    print(f"Previewing HDFS file: {target_path}")
    print()

    for index, line in enumerate(read_hdfs_lines(args, target_path), start=1):
        if index > args.limit:
            break

        article = json.loads(line)
        print(format_article(index, article, args.summary_width))
        print("-" * 80)


def resolve_target_file(client: InsecureClient, path: str) -> str:
    status = client.status(path, strict=False)
    if not status:
        raise SystemExit(f"HDFS path does not exist: {path}")

    if status["type"] == "FILE":
        return path

    files = list_hdfs_files(client, path)
    if not files:
        raise SystemExit(f"No HDFS files found under {path}")

    latest_path, _ = max(files, key=lambda item: item[1]["modificationTime"])
    return latest_path


def list_hdfs_files(client: InsecureClient, path: str) -> list[tuple[str, dict]]:
    files: list[tuple[str, dict]] = []
    for name, metadata in client.list(path, status=True):
        child_path = f"{path.rstrip('/')}/{name}"
        if metadata["type"] == "FILE":
            files.append((child_path, metadata))
        else:
            files.extend(list_hdfs_files(client, child_path))
    return files


def read_hdfs_lines(args: argparse.Namespace, path: str):
    open_url = f"{args.hdfs_url.rstrip('/')}/webhdfs/v1{path}"
    open_response = requests.get(
        open_url,
        params={
            "op": "OPEN",
            "user.name": args.hdfs_user,
            "offset": 0,
        },
        allow_redirects=False,
        timeout=30,
    )

    if open_response.status_code in (307, 308):
        redirect_url = rewrite_webhdfs_redirect(
            location=open_response.headers["Location"],
            requested_hdfs_url=args.hdfs_url,
            redirect_host=args.webhdfs_redirect_host,
        )
        file_response = requests.get(redirect_url, stream=True, timeout=60)
        file_response.raise_for_status()
        yield from file_response.iter_lines(decode_unicode=True)
        return

    open_response.raise_for_status()
    for line in open_response.text.splitlines():
        yield line


def rewrite_webhdfs_redirect(
    location: str,
    requested_hdfs_url: str,
    redirect_host: str,
) -> str:
    parts = urlsplit(location)
    effective_host = redirect_host.strip()

    if not effective_host:
        requested_host = urlsplit(requested_hdfs_url).hostname
        if requested_host in {"localhost", "127.0.0.1"}:
            effective_host = "localhost"

    if not effective_host:
        return location

    netloc = effective_host
    if parts.port:
        netloc = f"{effective_host}:{parts.port}"

    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


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
