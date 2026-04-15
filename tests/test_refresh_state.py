import unittest
from datetime import date

import pandas as pd

from dashboard.refresh_state import REFRESH_STATE_DEFAULTS, evaluate_today_data, local_today


class RefreshStateTests(unittest.TestCase):
    def test_refresh_state_defaults_include_expected_keys(self) -> None:
        self.assertEqual(REFRESH_STATE_DEFAULTS["refresh_status"], "idle")
        self.assertIn("active_dag_run_id", REFRESH_STATE_DEFAULTS)
        self.assertIn("refresh_error", REFRESH_STATE_DEFAULTS)

    def test_local_today_returns_date(self) -> None:
        result = local_today("Asia/Bangkok")
        self.assertIsInstance(result, date)

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


if __name__ == "__main__":
    unittest.main()
