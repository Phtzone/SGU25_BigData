from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd

REFRESH_STATE_DEFAULTS = {
    "refresh_status": "idle",
    "active_dag_run_id": None,
    "last_triggered_at": None,
    "last_successful_refresh_at": None,
    "refresh_error": None,
}


def local_today(timezone_name: str) -> date:
    return datetime.now(ZoneInfo(timezone_name)).date()


def evaluate_today_data(*, dataframe: pd.DataFrame, today: date) -> dict[str, object]:
    if dataframe.empty or "event_date" not in dataframe.columns:
        return {
            "today": today,
            "today_row_count": 0,
            "latest_event_date": None,
            "show_empty_today_state": True,
        }

    event_dates = pd.to_datetime(dataframe["event_date"], errors="coerce").dt.date
    latest_event_date = event_dates.max() if not event_dates.empty else None
    today_row_count = int((event_dates == today).sum())
    return {
        "today": today,
        "today_row_count": today_row_count,
        "latest_event_date": latest_event_date,
        "show_empty_today_state": today_row_count == 0,
    }


def build_refresh_status_message(
    *,
    refresh_status: str,
    latest_event_date: object,
    today_row_count: int,
    refresh_error: str | None = None,
) -> str:
    status = (refresh_status or "idle").lower()
    base = (
        f"Refresh status: {status} | Latest event date: {latest_event_date} | "
        f"Today's articles: {today_row_count}"
    )
    if refresh_error:
        return f"{base} | Error: {refresh_error}"
    return base
