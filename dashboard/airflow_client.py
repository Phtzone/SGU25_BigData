from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import requests


class AirflowApiError(RuntimeError):
    pass


@dataclass
class AirflowApiClient:
    base_url: str
    username: str
    password: str
    timeout_seconds: int = 10
    session: requests.Session = field(init=False)

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        self.session = requests.Session()
        self.session.auth = (self.username, self.password)

    def trigger_dag_run(self, dag_id: str) -> dict[str, Any]:
        return self._request(
            "post",
            f"{self.base_url}/dags/{dag_id}/dagRuns",
            json={},
        )

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        return self._request(
            "get",
            f"{self.base_url}/dags/{dag_id}/dagRuns/{dag_run_id}",
        )

    def _request(self, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
        try:
            response = getattr(self.session, method)(
                url,
                timeout=self.timeout_seconds,
                **kwargs,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            raise AirflowApiError(str(exc)) from exc
        return {
            "dag_run_id": payload.get("dag_run_id"),
            "state": payload.get("state"),
        }
