from __future__ import annotations

import pandas as pd


def round_metric_columns(dataframe: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    rounded_df = dataframe.copy()
    for column_name in columns:
        if column_name in rounded_df.columns:
            numeric_column = pd.to_numeric(rounded_df[column_name], errors="coerce")
            rounded_df[column_name] = numeric_column.round(2)
    return rounded_df


def shorten_hash_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    display_df = dataframe.copy()
    if "keyword_config_hash" in display_df.columns:
        display_df["keyword_config_hash"] = display_df["keyword_config_hash"].astype(str).str.slice(0, 8)
    return display_df
