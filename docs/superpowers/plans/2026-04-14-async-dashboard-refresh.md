# Async Dashboard Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an asynchronous `Refresh today's data` flow to the Streamlit dashboard that triggers Airflow, tracks run status, and clearly reports whether today has any keyword-backed articles in `Asia/Bangkok`.

**Architecture:** Keep orchestration in Airflow and keep Streamlit as a presentation layer. Add a small Airflow API client plus dashboard refresh helpers, then integrate them into the existing sidebar and header flow so the app can trigger a DAG run, poll it, invalidate cache on success, and surface a today-first empty state.

**Tech Stack:** Python, Streamlit, psycopg2, pandas, requests, unittest

---

## File Map

- Create: `dashboard/airflow_client.py`
  - Encapsulate Airflow REST API calls and normalize trigger/status responses.
- Create: `dashboard/refresh_state.py`
  - Hold refresh-state defaults, timezone helpers, and "today data" evaluation helpers.
- Modify: `dashboard/streamlit_app.py`
  - Wire refresh UI, session state, Airflow polling, cache invalidation, and today-first status rendering.
- Modify: `requirements-dashboard.txt`
  - Add `requests` if it is not already present through the dashboard dependency set.
- Modify: `tests/test_dashboard_exports.py`
  - Extend or split tests for dashboard helper behavior.
- Create: `tests/test_airflow_client.py`
  - Unit tests for API requests and error handling.
- Create: `tests/test_refresh_state.py`
  - Unit tests for session-state defaults and today evaluator logic.

### Task 1: Add Airflow API client

**Files:**
- Create: `dashboard/airflow_client.py`
- Test: `tests/test_airflow_client.py`
- Modify: `requirements-dashboard.txt`

- [ ] **Step 1: Write the failing tests**

```python
import unittest
from unittest.mock import Mock, patch

from dashboard.airflow_client import AirflowApiClient, AirflowApiError


class AirflowApiClientTests(unittest.TestCase):
    @patch("dashboard.airflow_client.requests.Session")
    def test_trigger_dag_run_returns_dag_run_id(self, session_cls: Mock) -> None:
        session = session_cls.return_value
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"dag_run_id": "manual__2026-04-14T10:00:00+00:00", "state": "queued"}
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
        response.json.return_value = {"dag_run_id": "manual__1", "state": "running"}
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m unittest tests.test_airflow_client -v
```

Expected: FAIL with `ModuleNotFoundError` or missing `AirflowApiClient`.

- [ ] **Step 3: Write minimal implementation**

```python
from __future__ import annotations

from dataclasses import dataclass
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
            response = getattr(self.session, method)(url, timeout=self.timeout_seconds, **kwargs)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            raise AirflowApiError(str(exc)) from exc
        return {
            "dag_run_id": payload.get("dag_run_id"),
            "state": payload.get("state"),
        }
```

Add to `requirements-dashboard.txt`:

```text
streamlit
psycopg2-binary
requests
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
python -m unittest tests.test_airflow_client -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add dashboard/airflow_client.py tests/test_airflow_client.py requirements-dashboard.txt
git commit -m "feat: add airflow dashboard client"
```

### Task 2: Add refresh-state and today evaluator helpers

**Files:**
- Create: `dashboard/refresh_state.py`
- Test: `tests/test_refresh_state.py`

- [ ] **Step 1: Write the failing tests**

```python
import unittest
from datetime import date

import pandas as pd

from dashboard.refresh_state import (
    REFRESH_STATE_DEFAULTS,
    evaluate_today_data,
    local_today,
)


class RefreshStateTests(unittest.TestCase):
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

        summary = evaluate_today_data(
            dataframe=dataframe,
            today=date(2026, 4, 14),
        )

        self.assertEqual(summary["today_row_count"], 1)
        self.assertEqual(summary["latest_event_date"], date(2026, 4, 14))
        self.assertFalse(summary["show_empty_today_state"])

    def test_evaluate_today_data_returns_empty_state_for_historical_only(self) -> None:
        dataframe = pd.DataFrame([{"event_date": date(2026, 4, 13)}])

        summary = evaluate_today_data(
            dataframe=dataframe,
            today=date(2026, 4, 14),
        )

        self.assertEqual(summary["today_row_count"], 0)
        self.assertTrue(summary["show_empty_today_state"])
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m unittest tests.test_refresh_state -v
```

Expected: FAIL with `ModuleNotFoundError` or missing helper definitions.

- [ ] **Step 3: Write minimal implementation**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
python -m unittest tests.test_refresh_state -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add dashboard/refresh_state.py tests/test_refresh_state.py
git commit -m "feat: add dashboard refresh state helpers"
```

### Task 3: Integrate asynchronous refresh into Streamlit

**Files:**
- Modify: `dashboard/streamlit_app.py`
- Modify: `dashboard/display_utils.py`
- Test: `tests/test_dashboard_exports.py`

- [ ] **Step 1: Write the failing tests**

Append focused tests such as:

```python
import unittest
from datetime import date

import pandas as pd

from dashboard.refresh_state import evaluate_today_data


class DashboardTodayStateTests(unittest.TestCase):
    def test_evaluate_today_data_handles_missing_event_date_column(self) -> None:
        summary = evaluate_today_data(
            dataframe=pd.DataFrame([{"keyword": "seo"}]),
            today=date(2026, 4, 14),
        )

        self.assertTrue(summary["show_empty_today_state"])
        self.assertIsNone(summary["latest_event_date"])
```

If Streamlit integration is split into pure helpers, add tests for helpers like:

```python
from dashboard.streamlit_app import build_refresh_status_caption


def test_build_refresh_status_caption_shows_failure_message():
    text = build_refresh_status_caption(
        refresh_status="failed",
        latest_event_date=None,
        today_row_count=0,
    )
    assert "failed" in text.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m unittest tests.test_dashboard_exports -v
```

Expected: FAIL because the new helper or behavior does not exist yet.

- [ ] **Step 3: Write minimal implementation**

In `dashboard/streamlit_app.py`, add imports:

```python
from datetime import date, datetime, timedelta

try:
    from dashboard.airflow_client import AirflowApiClient, AirflowApiError
    from dashboard.refresh_state import REFRESH_STATE_DEFAULTS, evaluate_today_data, local_today
except ModuleNotFoundError:
    from airflow_client import AirflowApiClient, AirflowApiError
    from refresh_state import REFRESH_STATE_DEFAULTS, evaluate_today_data, local_today
```

Add config helpers:

```python
APP_TIMEZONE = resolve_secret("APP_TIMEZONE", "Asia/Bangkok")


def airflow_config() -> dict[str, Any]:
    return {
        "base_url": resolve_secret("AIRFLOW_API_URL", "http://localhost:8080/api/v1"),
        "username": resolve_secret("AIRFLOW_USERNAME", ""),
        "password": resolve_secret("AIRFLOW_PASSWORD", ""),
    }
```

Add session helper:

```python
def ensure_refresh_state() -> None:
    for key, value in REFRESH_STATE_DEFAULTS.items():
        st.session_state.setdefault(key, value)
```

Add trigger/poll helpers:

```python
def get_airflow_client() -> AirflowApiClient:
    config = airflow_config()
    if not config["username"] or not config["password"]:
        raise AirflowApiError("Missing Airflow credentials.")
    return AirflowApiClient(**config)


def trigger_refresh() -> None:
    client = get_airflow_client()
    result = client.trigger_dag_run("news_pipeline")
    st.session_state["active_dag_run_id"] = result["dag_run_id"]
    st.session_state["refresh_status"] = result["state"] or "queued"
    st.session_state["last_triggered_at"] = datetime.now()
    st.session_state["refresh_error"] = None


def poll_refresh_status() -> None:
    dag_run_id = st.session_state.get("active_dag_run_id")
    if not dag_run_id:
        return
    client = get_airflow_client()
    result = client.get_dag_run("news_pipeline", dag_run_id)
    state = (result.get("state") or "queued").lower()
    st.session_state["refresh_status"] = state
    if state == "success":
        st.session_state["last_successful_refresh_at"] = datetime.now()
        st.session_state["active_dag_run_id"] = None
        st.cache_data.clear()
    elif state == "failed":
        st.session_state["active_dag_run_id"] = None
```

Replace the sidebar button section with:

```python
        refresh_active = st.session_state.get("active_dag_run_id") is not None
        if st.button("Refresh today's data", width="stretch", disabled=refresh_active):
            try:
                trigger_refresh()
                st.rerun()
            except AirflowApiError as exc:
                st.session_state["refresh_status"] = "failed"
                st.session_state["refresh_error"] = str(exc)
```

In `main()`, initialize and poll before rendering filters:

```python
    ensure_refresh_state()
    if st.session_state.get("active_dag_run_id"):
        try:
            poll_refresh_status()
        except AirflowApiError as exc:
            st.session_state["refresh_status"] = "failed"
            st.session_state["refresh_error"] = str(exc)
```

After loading the overall trends dataframe, compute and render today summary:

```python
    today_summary = evaluate_today_data(
        dataframe=overall_df,
        today=local_today(APP_TIMEZONE),
    )
```

Render a banner when `today_summary["show_empty_today_state"]` is true and refresh status is `success`.

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
python -m unittest tests.test_dashboard_exports -v
```

Expected: PASS

- [ ] **Step 5: Run a focused manual smoke test**

Run:

```bash
python -m unittest tests.test_airflow_client tests.test_refresh_state tests.test_dashboard_exports -v
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add dashboard/streamlit_app.py dashboard/display_utils.py tests/test_dashboard_exports.py
git commit -m "feat: add async dashboard refresh flow"
```

### Task 4: Verify end-to-end dashboard behavior

**Files:**
- Modify: `README.md`
- Test: existing dashboard + Airflow stack manually

- [ ] **Step 1: Document required env vars for refresh**

Add a short section to `README.md`:

```md
### Dashboard Refresh

The Streamlit dashboard can trigger a manual Airflow DAG run for `news_pipeline`.

Required environment variables:

- `AIRFLOW_API_URL`
- `AIRFLOW_USERNAME`
- `AIRFLOW_PASSWORD`
- `APP_TIMEZONE` (defaults to `Asia/Bangkok`)
```

- [ ] **Step 2: Run full test suite**

Run:

```bash
python -m unittest discover -s tests -q
```

Expected: all tests pass.

- [ ] **Step 3: Run manual verification**

Checklist:

- Start Streamlit with Airflow-enabled environment variables.
- Click `Refresh today's data`.
- Confirm status moves through `queued` or `running`.
- Confirm the button disables during active refresh.
- Confirm success clears cache and refreshes data.
- Confirm the success-without-today-data banner appears when no today rows exist.
- Confirm a failed Airflow call leaves historical data visible.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: document dashboard refresh setup"
```

## Self-Review

Spec coverage check:

- Airflow-triggered async refresh: covered in Tasks 1 and 3.
- Session state and polling: covered in Tasks 2 and 3.
- Today-first `Asia/Bangkok` behavior: covered in Tasks 2 and 3.
- Explicit no-today-data state: covered in Task 3.
- Backward-compatible docs and env handling: covered in Task 4.

Placeholder scan:

- No `TODO`, `TBD`, or implicit “write tests later” placeholders remain.

Type consistency:

- `AirflowApiClient.trigger_dag_run()` and `get_dag_run()` both return dicts with `dag_run_id` and `state`.
- `evaluate_today_data()` returns `today_row_count`, `latest_event_date`, and `show_empty_today_state`, matching the fields referenced in the integration task.
