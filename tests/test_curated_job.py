import unittest

from Spark_jobs.curate_news_processed_to_curated import build_curated_output_path


class CuratedJobTests(unittest.TestCase):
    def test_build_curated_output_path_mirrors_processed_date(self) -> None:
        output_path = build_curated_output_path(
            "/news/processed/2026/03/28/news_081501123456",
            "/news/curated",
        )

        self.assertEqual(output_path, "/news/curated/2026/03/28/news_081501123456")


if __name__ == "__main__":
    unittest.main()
