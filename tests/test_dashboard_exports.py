import unittest
from datetime import date

import pandas as pd

from dashboard.display_utils import round_metric_columns, shorten_hash_columns
from dashboard.export_utils import dataframe_to_csv_bytes, make_export_filename
from dashboard.refresh_state import build_refresh_status_message, evaluate_today_data


class DashboardExportUtilsTests(unittest.TestCase):
    def test_dataframe_to_csv_bytes_returns_utf8_sig_csv(self) -> None:
        dataframe = pd.DataFrame(
            [
                {"keyword": "chinh phu", "score": 12.5},
                {"keyword": "ai agent", "score": 8.0},
            ]
        )

        payload = dataframe_to_csv_bytes(dataframe)

        self.assertTrue(payload.startswith(b"\xef\xbb\xbf"))
        self.assertIn("keyword,score", payload.decode("utf-8-sig"))
        self.assertIn("chinh phu,12.5", payload.decode("utf-8-sig"))

    def test_make_export_filename_normalizes_prefix_and_range(self) -> None:
        filename = make_export_filename(
            prefix="Source Compare",
            date_from="2026-04-02",
            date_to="2026-04-08",
            extension="csv",
        )

        self.assertEqual(filename, "source-compare_2026-04-02_2026-04-08.csv")

    def test_round_metric_columns_handles_none_values(self) -> None:
        dataframe = pd.DataFrame(
            [
                {"keyword": "ai", "score": 12.3456},
                {"keyword": "seo", "score": None},
            ]
        )

        rounded = round_metric_columns(dataframe, ["score"])

        self.assertEqual(rounded.loc[0, "score"], 12.35)
        self.assertTrue(pd.isna(rounded.loc[1, "score"]))

    def test_shorten_hash_columns_truncates_keyword_config_hash(self) -> None:
        dataframe = pd.DataFrame(
            [
                {"keyword_config_hash": "1234567890abcdef"},
            ]
        )

        shortened = shorten_hash_columns(dataframe)

        self.assertEqual(shortened.loc[0, "keyword_config_hash"], "12345678")

    def test_evaluate_today_data_handles_missing_event_date_column(self) -> None:
        summary = evaluate_today_data(
            dataframe=pd.DataFrame([{"keyword": "seo"}]),
            today=date(2026, 4, 14),
        )

        self.assertTrue(summary["show_empty_today_state"])
        self.assertIsNone(summary["latest_event_date"])

    def test_build_refresh_status_message_shows_failure_message(self) -> None:
        text = build_refresh_status_message(
            refresh_status="failed",
            latest_event_date=None,
            today_row_count=0,
            refresh_error="boom",
        )

        self.assertIn("failed", text.lower())
        self.assertIn("boom", text)


if __name__ == "__main__":
    unittest.main()
