from __future__ import annotations

import re

import pandas as pd


def dataframe_to_csv_bytes(dataframe: pd.DataFrame) -> bytes:
    return dataframe.to_csv(index=False).encode("utf-8-sig")


def make_export_filename(
    *,
    prefix: str,
    date_from: str | None,
    date_to: str | None,
    extension: str = "csv",
) -> str:
    normalized_prefix = re.sub(r"[^a-z0-9]+", "-", prefix.strip().lower()).strip("-")
    safe_extension = extension.lstrip(".") or "csv"

    filename_parts = [part for part in [normalized_prefix, date_from, date_to] if part]
    return "_".join(filename_parts) + f".{safe_extension}"
