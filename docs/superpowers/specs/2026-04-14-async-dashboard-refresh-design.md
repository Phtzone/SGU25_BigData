# Async Dashboard Refresh Design

## Goal

Add an asynchronous refresh flow to the Streamlit SEO dashboard so a user can request a new pipeline run from the UI and immediately know whether there are any news articles for "today" in `Asia/Bangkok`.

The feature must prioritize this user outcome:

- after pressing refresh, the dashboard should clearly tell the user whether there are articles from the current Vietnam date
- if there are no articles for today, the app should explicitly say so instead of silently showing empty charts

## Scope

In scope:

- add an asynchronous refresh trigger from Streamlit
- trigger the existing Airflow DAG `news_pipeline`
- poll Airflow for run status
- show refresh status in the dashboard
- define "today" using `Asia/Bangkok`
- show a clear empty-state when refresh succeeds but no articles exist for the current date

Out of scope:

- changing DAG task order or task logic
- creating a separate dashboard-only DAG
- changing RSS source behavior
- forcing the sources to publish same-day articles
- rewriting keyword extraction logic

## Current Context

The repository already has:

- a Streamlit dashboard in `dashboard/streamlit_app.py`
- an Airflow DAG `news_pipeline` in `dags/news_pipeline_dag.py`
- PostgreSQL keyword views consumed by Streamlit
- a refresh button placeholder in the dashboard UI

The current dashboard reads precomputed PostgreSQL views. It does not orchestrate ingestion or processing itself, which is the correct architectural boundary to preserve.

## Design Summary

The dashboard will trigger a manual Airflow DAG run through the Airflow REST API. The trigger is asynchronous: Streamlit does not execute the ETL itself and does not wait inside a long-running request for the pipeline to finish.

Instead, the app:

1. creates a manual DAG run for `news_pipeline`
2. stores the returned `dag_run_id` in session state
3. polls Airflow for run status
4. reloads PostgreSQL data after success
5. evaluates whether any rows exist for the current `Asia/Bangkok` date
6. renders either the "today" keyword views or a clear "no articles today" state

## User Experience

### Refresh action

The dashboard will expose a primary action labeled `Refresh today's data`.

When the user presses it:

- the button becomes disabled while a refresh is active
- the dashboard shows a status panel with the run state
- the dashboard continues to show the latest successful data already in PostgreSQL until a new run completes

### Status panel

The status panel should display:

- refresh status: `idle`, `queued`, `running`, `success`, or `failed`
- last trigger time in `Asia/Bangkok`
- last successful refresh time in `Asia/Bangkok`
- latest `event_date` currently available in PostgreSQL
- article count for `today`

### Success behavior

When the DAG run succeeds:

- the app clears cached query results
- the app fetches current dashboard data again
- the app computes the current date in `Asia/Bangkok`
- the app checks whether there are any today rows

If there are today rows:

- the dashboard renders its charts and tables using the refreshed dataset
- the app highlights that data for today is available

If there are no today rows:

- the app shows a clear informational banner:
  - `Da refresh thanh cong nhung chua co bai bao ngay hom nay tu cac nguon RSS.`
- the dashboard may still keep non-today historical data accessible through date filters, but the today-first state remains explicit

### Failure behavior

When the DAG run fails:

- the status panel shows `failed`
- the latest successful dashboard data remains visible
- the app shows a short message telling the user to inspect Airflow logs for the failed run

## Architecture

### Components

#### 1. Streamlit refresh UI

Responsible for:

- rendering the refresh button
- rendering the refresh status panel
- deciding when to rerun the page
- deciding whether to render the today-first empty state

This logic stays in the dashboard layer.

#### 2. Airflow API client module

A small helper module should be introduced for:

- triggering a DAG run
- fetching DAG run status
- normalizing Airflow responses into a small internal shape used by the UI

This keeps HTTP and auth details out of the main Streamlit file.

Expected configuration:

- `AIRFLOW_API_URL`
- `AIRFLOW_USERNAME`
- `AIRFLOW_PASSWORD`
- optional request timeout settings

#### 3. Dashboard refresh state

State is stored in `st.session_state` and should include:

- active `dag_run_id`
- active run status
- last trigger timestamp
- last successful refresh timestamp
- whether a poll cycle is currently active

This state is UI-scoped and should not be persisted in PostgreSQL.

#### 4. Today evaluator

A small dashboard-side helper should evaluate the loaded query results against the current `Asia/Bangkok` date and return:

- current local date
- latest available `event_date`
- count of today rows
- whether the today-first empty state should be shown

This isolates time-based display logic from SQL query assembly.

## Data Flow

### Trigger flow

1. user presses `Refresh today's data`
2. Streamlit calls the Airflow API client
3. client creates a manual run for `news_pipeline`
4. Streamlit stores `dag_run_id` and marks status as `queued`
5. Streamlit begins polling for that specific run

### Polling flow

1. Streamlit requests current run state from Airflow
2. if state is `queued` or `running`, the UI keeps the in-progress status visible
3. if state is `success`, the app clears data cache and reloads PostgreSQL-backed views
4. if state is `failed`, the app stops polling and surfaces the failure state

### Data display flow

1. dashboard query results are loaded from PostgreSQL
2. the app computes `today` in `Asia/Bangkok`
3. the app compares `event_date` values to that date
4. the app either renders today-backed results or the explicit no-today-data message

## Timezone Rules

The feature will define "today" exclusively using `Asia/Bangkok`.

Rules:

- UI timestamps are displayed in `Asia/Bangkok`
- the today-first check is based on the current date in `Asia/Bangkok`
- if stored timestamps already arrive as dates, the comparison will be against the local calendar date
- if stored timestamps are datetimes, they should be converted or interpreted consistently before the date comparison

This avoids a mismatch where the dashboard is opened in Vietnam time but interprets "today" using UTC or source-local offsets.

## Error Handling

### Airflow unreachable

If Streamlit cannot reach the Airflow API:

- refresh is not started
- the dashboard shows a concise error banner
- the existing dashboard data remains available

### Authentication error

If Airflow credentials are missing or invalid:

- refresh is not started
- the dashboard shows a configuration error
- the app does not silently fall back to any local execution path

### Duplicate clicks

If a refresh is already active:

- the button remains disabled
- the dashboard does not create a second run from the same browser session

This keeps the initial implementation simple and prevents duplicate manual DAG runs from one session.

### Success without today data

This is not treated as an error.

It is a valid, explicit outcome:

- refresh succeeded
- the sources did not provide articles whose `event_date` equals today in `Asia/Bangkok`

## Testing Strategy

### Unit tests

Add tests for:

- Airflow API client request and response handling
- session-state refresh transitions
- today evaluator behavior for:
  - today data exists
  - only historical data exists
  - no data exists
- failure-state rendering helpers

### Dashboard behavior tests

Add tests that verify:

- refresh button disables during active polling
- cache invalidation happens only after success
- no-today-data banner appears after successful refresh with zero today rows

### Manual verification

Verify:

- trigger works against local Airflow
- status moves through `queued` and `running`
- successful runs repopulate dashboard data
- today-first empty state is shown when appropriate
- failure state preserves last good data

## Rollout Notes

The implementation should remain backward-compatible:

- if Airflow env vars are absent, the dashboard can still load historical data
- only the refresh feature should be unavailable in that case

This prevents the dashboard from becoming unusable in environments where Airflow is not enabled.

## Recommendation

Implement asynchronous refresh by calling the existing Airflow DAG and keep the dashboard today-first at the presentation layer.

This is the best fit for the current repository because it:

- preserves the current pipeline boundary
- avoids duplicating orchestration logic
- gives users a clear answer for the SEO use case
- keeps failure handling and observability aligned with Airflow
