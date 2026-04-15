import unittest
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd

from dashboard.refresh_state import (
    REFRESH_STATE_DEFAULTS,
    default_date_window,
    evaluate_today_data,
    is_refresh_configured,
    local_now,
    local_today,
    summarize_today_article_availability,
)


class RefreshStateTests(unittest.TestCase):
    def test_refresh_state_defaults_include_expected_keys(self) -> None:
        self.assertEqual(REFRESH_STATE_DEFAULTS["refresh_status"], "idle")
        self.assertIn("active_dag_run_id", REFRESH_STATE_DEFAULTS)
        self.assertIn("refresh_error", REFRESH_STATE_DEFAULTS)

    def test_local_today_returns_date(self) -> None:
        result = local_today("Asia/Bangkok")
        self.assertIsInstance(result, date)

    def test_local_now_returns_timezone_aware_datetime(self) -> None:
        result = local_now("Asia/Bangkok")

        self.assertIsInstance(result, datetime)
        self.assertEqual(result.tzinfo, ZoneInfo("Asia/Bangkok"))

    def test_default_date_window_uses_local_today(self) -> None:
        date_from, date_to = default_date_window(date(2026, 4, 14), days=7)

        self.assertEqual(date_from, date(2026, 4, 8))
        self.assertEqual(date_to, date(2026, 4, 14))

    def test_evaluate_today_data_counts_today_rows(self) -> None:
        dataframe = pd.DataFrame(
            [
                {"event_date": date(2026, 4, 14)},
                {"event_date": date(2026, 4, 13)},
            ]
        )

        summary = evaluate_today_data(dataframe=dataframe, today=date(2026, 4, 14))

        self.assertEqual(summary["today_row_count"], 1)
        self.assertEqual(summary["latest_event_date"], date(2026, 4, 14))
        self.assertFalse(summary["show_empty_today_state"])

    def test_evaluate_today_data_returns_empty_state_for_historical_only(self) -> None:
        dataframe = pd.DataFrame([{"event_date": date(2026, 4, 13)}])

        summary = evaluate_today_data(dataframe=dataframe, today=date(2026, 4, 14))

        self.assertEqual(summary["today_row_count"], 0)
        self.assertTrue(summary["show_empty_today_state"])

    def test_summarize_today_article_availability_keeps_today_articles_without_keywords(self) -> None:
        summary = summarize_today_article_availability(
            today=date(2026, 4, 14),
            latest_event_date=date(2026, 4, 14),
            today_article_count=3,
        )

        self.assertEqual(summary["today_row_count"], 3)
        self.assertFalse(summary["show_empty_today_state"])

    def test_is_refresh_configured_requires_all_airflow_fields(self) -> None:
        self.assertTrue(
            is_refresh_configured(
                {
                    "base_url": "http://localhost:8080/api/v1",
                    "username": "airflow",
                    "password": "airflow",
                }
            )
        )
        self.assertFalse(
            is_refresh_configured(
                {
                    "base_url": "http://localhost:8080/api/v1",
                    "username": "airflow",
                    "password": "",
                }
            )
        )


if __name__ == "__main__":
    unittest.main()
