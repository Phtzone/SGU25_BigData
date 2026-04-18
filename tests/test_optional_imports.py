import unittest


class OptionalImportTests(unittest.TestCase):
    def test_airflow_client_imports_without_requests_at_import_time(self) -> None:
        from dashboard.airflow_client import AirflowApiClient

        self.assertTrue(hasattr(AirflowApiClient, "trigger_dag_run"))

    def test_hdfs_utils_imports_without_requests_at_import_time(self) -> None:
        from common.hdfs_utils import build_hdfs_uri

        self.assertEqual(
            build_hdfs_uri("/news/raw/file.jsonl", "hdfs://namenode:9000"),
            "hdfs://namenode:9000/news/raw/file.jsonl",
        )


if __name__ == "__main__":
    unittest.main()
