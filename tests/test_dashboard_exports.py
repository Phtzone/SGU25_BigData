import unittest

import pandas as pd

from dashboard.export_utils import dataframe_to_csv_bytes, make_export_filename


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


if __name__ == "__main__":
    unittest.main()
