import unittest
from unittest.mock import Mock, patch

from dashboard.airflow_client import AirflowApiClient, AirflowApiError


class AirflowApiClientTests(unittest.TestCase):
    @patch("dashboard.airflow_client.requests.Session")
    def test_trigger_dag_run_returns_dag_run_id(self, session_cls: Mock) -> None:
        session = session_cls.return_value
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "dag_run_id": "manual__2026-04-14T10:00:00+00:00",
            "state": "queued",
        }
        session.post.return_value = response

        client = AirflowApiClient(
            base_url="http://localhost:8080/api/v1",
            username="airflow",
            password="airflow",
        )

        result = client.trigger_dag_run("news_pipeline")

        self.assertEqual(result["dag_run_id"], "manual__2026-04-14T10:00:00+00:00")
        self.assertEqual(result["state"], "queued")

    @patch("dashboard.airflow_client.requests.Session")
    def test_get_dag_run_status_returns_normalized_state(self, session_cls: Mock) -> None:
        session = session_cls.return_value
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "dag_run_id": "manual__1",
            "state": "running",
        }
        session.get.return_value = response

        client = AirflowApiClient(
            base_url="http://localhost:8080/api/v1",
            username="airflow",
            password="airflow",
        )

        result = client.get_dag_run("news_pipeline", "manual__1")

        self.assertEqual(result["dag_run_id"], "manual__1")
        self.assertEqual(result["state"], "running")

    @patch("dashboard.airflow_client.requests.Session")
    def test_trigger_raises_airflow_api_error_on_http_failure(self, session_cls: Mock) -> None:
        session = session_cls.return_value
        response = Mock()
        response.raise_for_status.side_effect = RuntimeError("boom")
        session.post.return_value = response

        client = AirflowApiClient(
            base_url="http://localhost:8080/api/v1",
            username="airflow",
            password="airflow",
        )

        with self.assertRaises(AirflowApiError):
            client.trigger_dag_run("news_pipeline")


if __name__ == "__main__":
    unittest.main()
