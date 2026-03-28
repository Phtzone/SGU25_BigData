from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any, Iterator, TYPE_CHECKING
from urllib.parse import urlsplit, urlunsplit

import requests

if TYPE_CHECKING:
    from hdfs import InsecureClient
else:
    InsecureClient = Any


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


def list_hdfs_files(client: InsecureClient, path: str) -> list[tuple[str, dict]]:
    files: list[tuple[str, dict]] = []

    for name, metadata in client.list(path, status=True):
        child_path = f"{path.rstrip('/')}/{name}"
        if metadata["type"] == "FILE":
            files.append((child_path, metadata))
        else:
            files.extend(list_hdfs_files(client, child_path))

    return files


def resolve_latest_hdfs_file(client: InsecureClient, path: str) -> str:
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


def read_hdfs_bytes(
    *,
    hdfs_url: str,
    hdfs_user: str,
    path: str,
    redirect_host: str = "",
) -> bytes:
    open_url = f"{hdfs_url.rstrip('/')}/webhdfs/v1{path}"
    open_response = requests.get(
        open_url,
        params={
            "op": "OPEN",
            "user.name": hdfs_user,
            "offset": 0,
        },
        allow_redirects=False,
        timeout=30,
    )

    if open_response.status_code in (307, 308):
        redirect_url = rewrite_webhdfs_redirect(
            location=open_response.headers["Location"],
            requested_hdfs_url=hdfs_url,
            redirect_host=redirect_host,
        )
        file_response = requests.get(redirect_url, timeout=60)
        file_response.raise_for_status()
        return file_response.content

    open_response.raise_for_status()
    return open_response.content


def read_hdfs_lines(
    *,
    hdfs_url: str,
    hdfs_user: str,
    path: str,
    redirect_host: str = "",
) -> Iterator[str]:
    data = read_hdfs_bytes(
        hdfs_url=hdfs_url,
        hdfs_user=hdfs_user,
        path=path,
        redirect_host=redirect_host,
    )
    for line in data.decode("utf-8").splitlines():
        yield line


def upload_hdfs_bytes(
    *,
    hdfs_url: str,
    hdfs_user: str,
    path: str,
    data: bytes,
    redirect_host: str = "",
    overwrite: bool = True,
    content_type: str = "application/octet-stream",
) -> None:
    create_url = f"{hdfs_url.rstrip('/')}/webhdfs/v1{path}"
    create_response = requests.put(
        create_url,
        params={
            "op": "CREATE",
            "overwrite": str(overwrite).lower(),
            "user.name": hdfs_user,
        },
        allow_redirects=False,
        timeout=30,
    )

    if create_response.status_code in (307, 308):
        redirect_url = rewrite_webhdfs_redirect(
            location=create_response.headers["Location"],
            requested_hdfs_url=hdfs_url,
            redirect_host=redirect_host,
        )
        upload_response = requests.put(
            redirect_url,
            data=data,
            headers={"Content-Type": content_type},
            timeout=60,
        )
        upload_response.raise_for_status()
        return

    create_response.raise_for_status()


def upload_directory_to_hdfs(
    *,
    client: InsecureClient,
    local_dir: str,
    hdfs_dir: str,
    hdfs_url: str,
    hdfs_user: str,
    redirect_host: str = "",
) -> list[str]:
    uploaded_paths: list[str] = []
    source_dir = Path(local_dir)

    for entry in source_dir.rglob("*"):
        relative = entry.relative_to(source_dir).as_posix()
        target_path = f"{hdfs_dir.rstrip('/')}/{relative}" if relative else hdfs_dir

        if entry.is_dir():
            client.makedirs(target_path)
            continue

        client.makedirs(str(PurePosixPath(target_path).parent))
        upload_hdfs_bytes(
            hdfs_url=hdfs_url,
            hdfs_user=hdfs_user,
            path=target_path,
            data=entry.read_bytes(),
            redirect_host=redirect_host,
        )
        uploaded_paths.append(target_path)

    return uploaded_paths
