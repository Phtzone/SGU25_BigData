import unittest

from Spark_jobs.transform_news_raw_to_processed import build_processed_output_path


class TransformJobTests(unittest.TestCase):
    def test_build_processed_output_path_mirrors_raw_date(self) -> None:
        output_path = build_processed_output_path(
            "/news/raw/2026/03/28/news_081501123456.jsonl",
            "/news/processed",
        )

        self.assertEqual(output_path, "/news/processed/2026/03/28/news_081501123456")


if __name__ == "__main__":
    unittest.main()
