from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable

from common.hdfs_utils import list_hdfs_files


def write_output_path_file(path_file: str, output_path: str) -> None:
    if not path_file.strip():
        return

    path = Path(path_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(output_path + "\n", encoding="utf-8")


def resolve_batch_from_parquet_path(
    parquet_path: str,
    *,
    partition_prefixes: Iterable[str] = (),
    parents_up_if_unpartitioned: int = 1,
) -> str:
    path = PurePosixPath(parquet_path)
    parts = path.parts
    normalized_prefixes = tuple(prefix for prefix in partition_prefixes if prefix)

    for index, part in enumerate(parts):
        if any(part.startswith(prefix) for prefix in normalized_prefixes):
            if index == 0:
                break
            return str(PurePosixPath(*parts[:index]))

    if parents_up_if_unpartitioned < 1 or len(path.parents) < parents_up_if_unpartitioned:
        raise SystemExit(f"Unexpected Parquet file layout: {parquet_path}")

    return str(path.parents[parents_up_if_unpartitioned - 1])


def resolve_latest_parquet_batch(
    client: Any,
    path: str,
    *,
    batch_from_parquet: Callable[[str], str],
    missing_status_message: str,
    missing_parquet_message: str,
) -> str:
    status = client.status(path, strict=False)
    if not status:
        raise SystemExit(missing_status_message.format(path=path))

    if status["type"] == "FILE":
        if not path.endswith(".parquet"):
            raise SystemExit(f"Expected a Parquet file but got: {path}")
        return batch_from_parquet(path)

    parquet_files = [item for item in list_hdfs_files(client, path) if item[0].endswith(".parquet")]
    if not parquet_files:
        raise SystemExit(missing_parquet_message.format(path=path))

    latest_parquet = max(parquet_files, key=lambda item: item[1]["modificationTime"])[0]
    return batch_from_parquet(latest_parquet)
