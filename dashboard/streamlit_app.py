from __future__ import annotations

import os
import time
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
import streamlit as st

try:
    from dashboard.airflow_client import AirflowApiClient, AirflowApiError
    from dashboard.display_utils import round_metric_columns, shorten_hash_columns
    from dashboard.query_builders import (
        build_article_keywords_query,
        build_breakout_keywords_query,
        build_keyword_detail_query,
        build_keyword_metrics_query,
        build_today_article_summary_query,
        build_keyword_source_compare_query,
        build_keyword_timeseries_query,
        build_overall_keyword_trends_query,
        build_source_keyword_trends_query,
        build_source_options_query,
    )
    from dashboard.export_utils import dataframe_to_csv_bytes, make_export_filename
    from dashboard.refresh_state import (
        REFRESH_STATE_DEFAULTS,
        build_refresh_status_message,
        default_date_window,
        evaluate_today_data,
        is_refresh_configured,
        local_now,
        local_today,
        summarize_today_article_availability,
    )
except ModuleNotFoundError:
    from airflow_client import AirflowApiClient, AirflowApiError
    from display_utils import round_metric_columns, shorten_hash_columns
    from query_builders import (
        build_article_keywords_query,
        build_breakout_keywords_query,
        build_keyword_detail_query,
        build_keyword_metrics_query,
        build_today_article_summary_query,
        build_keyword_source_compare_query,
        build_keyword_timeseries_query,
        build_overall_keyword_trends_query,
        build_source_keyword_trends_query,
        build_source_options_query,
    )
    from export_utils import dataframe_to_csv_bytes, make_export_filename
    from refresh_state import (
        REFRESH_STATE_DEFAULTS,
        build_refresh_status_message,
        default_date_window,
        evaluate_today_data,
        is_refresh_configured,
        local_now,
        local_today,
        summarize_today_article_availability,
    )


def page_setup() -> None:
    st.set_page_config(
        page_title="SEO Keyword Radar",
        page_icon="R",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(196, 224, 255, 0.55), transparent 28%),
                radial-gradient(circle at top right, rgba(255, 225, 196, 0.45), transparent 22%),
                linear-gradient(180deg, #f5f2ea 0%, #fbfaf7 45%, #f3efe6 100%);
        }
        .hero-shell {
            padding: 1.4rem 1.6rem;
            border: 1px solid rgba(40, 44, 36, 0.12);
            background: rgba(255, 252, 245, 0.9);
            border-radius: 18px;
            box-shadow: 0 18px 42px rgba(68, 54, 29, 0.08);
            margin-bottom: 1rem;
        }
        .hero-kicker {
            letter-spacing: 0.12em;
            text-transform: uppercase;
            font-size: 0.78rem;
            color: #7a6448;
            margin-bottom: 0.35rem;
        }
        .hero-title {
            font-size: 2.3rem;
            font-weight: 700;
            line-height: 1.05;
            color: #172313;
            margin-bottom: 0.4rem;
        }
        .hero-copy {
            font-size: 1rem;
            color: #41503b;
            max-width: 52rem;
        }
        div[data-testid="stMetric"] {
            background: rgba(255, 252, 245, 0.88);
            border: 1px solid rgba(40, 44, 36, 0.1);
            padding: 0.8rem 1rem;
            border-radius: 16px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def resolve_secret(name: str, default: str) -> str:
    try:
        database_secrets = st.secrets.get("analytics_db", {})
    except Exception:
        database_secrets = {}
    secret_key = name.lower()
    if secret_key in database_secrets:
        return str(database_secrets[secret_key])
    return os.getenv(name, default)


def db_config() -> dict[str, Any]:
    return {
        "host": resolve_secret("ANALYTICS_DB_HOST", "localhost"),
        "port": int(resolve_secret("ANALYTICS_DB_PORT", "5433")),
        "dbname": resolve_secret("ANALYTICS_DB_NAME", "analytics"),
        "user": resolve_secret("ANALYTICS_DB_USER", "analytics"),
        "password": resolve_secret("ANALYTICS_DB_PASSWORD", "analytics"),
        "connect_timeout": 10,
    }


APP_TIMEZONE = resolve_secret("APP_TIMEZONE", "Asia/Bangkok")


def airflow_config() -> dict[str, Any]:
    return {
        "base_url": resolve_secret("AIRFLOW_API_URL", "http://localhost:8080/api/v1"),
        "username": resolve_secret("AIRFLOW_USERNAME", ""),
        "password": resolve_secret("AIRFLOW_PASSWORD", ""),
    }


@st.cache_data(ttl=60, show_spinner=False)
def run_dataframe_query(query: str, params: tuple[Any, ...]) -> pd.DataFrame:
    with psycopg2.connect(**db_config()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
        return pd.DataFrame(rows, columns=columns)


def render_header() -> None:
    st.markdown(
        """
        <div class="hero-shell">
            <div class="hero-kicker">News Analytics Dashboard</div>
            <div class="hero-title">SEO Keyword Radar</div>
            <div class="hero-copy">
                Track keyword candidates, daily keyword momentum, and article-level signals
                directly from the PostgreSQL keyword views. This app reads precomputed marts,
                not Spark jobs on request.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def ensure_refresh_state() -> None:
    for key, value in REFRESH_STATE_DEFAULTS.items():
        st.session_state.setdefault(key, value)


def get_airflow_client() -> AirflowApiClient:
    config = airflow_config()
    if not is_refresh_configured(config):
        raise AirflowApiError("Missing Airflow credentials.")
    return AirflowApiClient(**config)


def format_refresh_timestamp(value: datetime | None) -> str:
    if value is None:
        return "None"
    if value.tzinfo is None:
        value = value.replace(tzinfo=ZoneInfo(APP_TIMEZONE))
    return value.astimezone(ZoneInfo(APP_TIMEZONE)).strftime("%Y-%m-%d %H:%M:%S")


def trigger_refresh() -> None:
    client = get_airflow_client()
    result = client.trigger_dag_run("news_pipeline")
    st.session_state["active_dag_run_id"] = result["dag_run_id"]
    st.session_state["refresh_status"] = (result.get("state") or "queued").lower()
    st.session_state["last_triggered_at"] = local_now(APP_TIMEZONE)
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
        st.session_state["last_successful_refresh_at"] = local_now(APP_TIMEZONE)
        st.session_state["active_dag_run_id"] = None
        st.session_state["refresh_error"] = None
        st.cache_data.clear()
    elif state == "failed":
        st.session_state["active_dag_run_id"] = None


def fetch_today_summary(filters: dict[str, Any]) -> dict[str, Any]:
    today = local_today(APP_TIMEZONE)
    query, params = build_today_article_summary_query(
        today=today,
        sources=filters["sources"],
    )
    summary_df = run_dataframe_query(query, tuple(params))
    if summary_df.empty:
        return summarize_today_article_availability(
            today=today,
            latest_event_date=None,
            today_article_count=0,
        )

    summary_row = summary_df.iloc[0]
    return summarize_today_article_availability(
        today=today,
        latest_event_date=summary_row.get("latest_event_date"),
        today_article_count=int(summary_row.get("today_article_count") or 0),
    )


def render_refresh_status_panel(
    filters: dict[str, Any],
    metrics: pd.Series | None,
) -> None:
    today_summary = fetch_today_summary(filters)
    latest_event_date = today_summary.get("latest_event_date")
    if latest_event_date is None and metrics is not None:
        latest_event_date = metrics.get("latest_event_date")
    st.caption(
        build_refresh_status_message(
            refresh_status=str(st.session_state.get("refresh_status", "idle")),
            latest_event_date=latest_event_date,
            today_row_count=int(today_summary["today_row_count"]),
            refresh_error=st.session_state.get("refresh_error"),
        )
    )
    st.caption(
        "Last triggered: "
        f"{format_refresh_timestamp(st.session_state.get('last_triggered_at'))} | "
        "Last successful refresh: "
        f"{format_refresh_timestamp(st.session_state.get('last_successful_refresh_at'))}"
    )
    if st.session_state.get("refresh_status") == "failed" and st.session_state.get("refresh_error"):
        st.error(
            "Refresh failed. Inspect the Airflow run logs for details.\n\n"
            f"{st.session_state['refresh_error']}"
        )
    elif st.session_state.get("refresh_status") == "success" and today_summary["show_empty_today_state"]:
        st.info("Da refresh thanh cong nhung chua co bai bao ngay hom nay tu cac nguon RSS.")
    elif st.session_state.get("active_dag_run_id"):
        st.info("Refresh is in progress. Dashboard status will update automatically.")


def sidebar_filters() -> dict[str, Any]:
    today = local_today(APP_TIMEZONE)
    default_start, default_end = default_date_window(today, days=7)
    refresh_configured = is_refresh_configured(airflow_config())

    with st.sidebar:
        st.header("Filters")
        date_from = st.date_input("Start date", value=default_start)
        date_to = st.date_input("End date", value=default_end)
        if date_from > date_to:
            date_from, date_to = date_to, date_from

        source_query, source_params = build_source_options_query(
            date_from=date_from,
            date_to=date_to,
        )
        sources_df = run_dataframe_query(source_query, tuple(source_params))
        source_options = sources_df["source"].tolist() if not sources_df.empty else []

        selected_sources = st.multiselect(
            "Sources",
            options=source_options,
            default=[],
            help="Leave empty to include all sources.",
        )
        ngram_sizes = st.multiselect(
            "N-gram sizes",
            options=[1, 2, 3],
            default=[2, 3],
        )
        top_n = st.slider("Top rows", min_value=10, max_value=200, value=50, step=10)
        keyword_search = st.text_input("Keyword contains", value="")
        title_search = st.text_input("Article title contains", value="")

        refresh_active = st.session_state.get("active_dag_run_id") is not None
        refresh_disabled = refresh_active or not refresh_configured
        if st.button("Refresh today's data", width="stretch", disabled=refresh_disabled):
            try:
                trigger_refresh()
                st.rerun()
            except AirflowApiError as exc:
                st.session_state["refresh_status"] = "failed"
                st.session_state["refresh_error"] = str(exc)
        if not refresh_configured:
            st.caption(
                "Refresh unavailable until AIRFLOW_API_URL, AIRFLOW_USERNAME, and AIRFLOW_PASSWORD are configured."
            )

    return {
        "date_from": date_from,
        "date_to": date_to,
        "sources": selected_sources,
        "ngram_sizes": ngram_sizes,
        "top_n": top_n,
        "keyword_search": keyword_search,
        "title_search": title_search,
    }


def render_metrics(filters: dict[str, Any]) -> pd.Series | None:
    metrics_query, metrics_params = build_keyword_metrics_query(
        date_from=filters["date_from"],
        date_to=filters["date_to"],
        sources=filters["sources"],
        ngram_sizes=filters["ngram_sizes"],
        keyword_search=filters["keyword_search"],
    )
    metrics_df = run_dataframe_query(metrics_query, tuple(metrics_params))
    metrics = metrics_df.iloc[0] if not metrics_df.empty else None

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Keyword Rows", int(metrics["keyword_rows"]) if metrics is not None else 0)
    col2.metric(
        "Distinct Keywords",
        int(metrics["distinct_keywords"]) if metrics is not None else 0,
    )
    col3.metric(
        "Sources",
        int(metrics["source_count"]) if metrics is not None else 0,
    )
    col4.metric(
        "Supporting Articles",
        int(metrics["supporting_articles"]) if metrics is not None else 0,
    )
    if metrics is not None:
        latest_event_date = metrics.get("latest_event_date")
        version_label = metrics.get("score_versions") or "unknown"
        config_versions = int(metrics.get("config_versions") or 0)
        st.caption(
            f"Keyword model: {version_label} | Config variants in current slice: {config_versions} | Latest event date: {latest_event_date}"
        )
    return metrics
def render_download_button(
    *,
    label: str,
    dataframe: pd.DataFrame,
    prefix: str,
    filters: dict[str, Any],
    key: str,
) -> None:
    if dataframe.empty:
        return

    st.download_button(
        label=label,
        data=dataframe_to_csv_bytes(dataframe),
        file_name=make_export_filename(
            prefix=prefix,
            date_from=filters["date_from"].isoformat(),
            date_to=filters["date_to"].isoformat(),
        ),
        mime="text/csv",
        key=key,
        width="stretch",
    )


def build_keyword_option_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame(columns=["keyword", "keyword_normalized"])

    return (
        dataframe.loc[:, ["keyword", "keyword_normalized"]]
        .dropna(subset=["keyword_normalized"])
        .drop_duplicates(subset=["keyword_normalized"])
        .reset_index(drop=True)
    )


def render_keyword_timeseries(
    *,
    title: str,
    date_from: date,
    date_to: date,
    sources: list[str],
    ngram_sizes: list[int],
    keyword_search: str,
    limit_keywords: int,
) -> None:
    query, params = build_keyword_timeseries_query(
        date_from=date_from,
        date_to=date_to,
        sources=sources,
        ngram_sizes=ngram_sizes,
        keyword_search=keyword_search,
        limit_keywords=limit_keywords,
    )
    timeseries_df = run_dataframe_query(query, tuple(params))

    st.subheader(title)
    if timeseries_df.empty:
        st.info("No time-series rows match the current filters.")
        return

    weighted_pivot = timeseries_df.pivot_table(
        index="event_date",
        columns="keyword",
        values="final_keyword_score",
        aggfunc="sum",
        fill_value=0,
    )
    article_pivot = timeseries_df.pivot_table(
        index="event_date",
        columns="keyword",
        values="article_count",
        aggfunc="sum",
        fill_value=0,
    )

    chart_left, chart_right = st.columns(2)
    with chart_left:
        st.caption("Final keyword score by day")
        st.line_chart(weighted_pivot, width="stretch")
    with chart_right:
        st.caption("Supporting articles by day")
        st.line_chart(article_pivot, width="stretch")


def render_breakout_table(filters: dict[str, Any]) -> None:
    query, params = build_breakout_keywords_query(
        date_from=filters["date_from"],
        date_to=filters["date_to"],
        sources=filters["sources"],
        ngram_sizes=filters["ngram_sizes"],
        keyword_search=filters["keyword_search"],
        limit=filters["top_n"],
    )
    breakout_df = run_dataframe_query(query, tuple(params))

    st.subheader("Top Breakout Keywords")
    if breakout_df.empty:
        st.info("No breakout candidates are available for the current filters.")
        return

    display_df = round_metric_columns(
        breakout_df,
        [
            "weighted_score",
            "final_keyword_score",
            "previous_avg_final_keyword_score",
            "breakout_score",
            "previous_avg_article_count",
            "article_count_delta",
        ],
    )
    display_df = shorten_hash_columns(display_df)
    st.dataframe(display_df, width="stretch", hide_index=True)
    render_download_button(
        label="Download breakout keywords CSV",
        dataframe=display_df,
        prefix="breakout-keywords",
        filters=filters,
        key="download_breakout_keywords",
    )


def render_overall_tab(filters: dict[str, Any]) -> None:
    query, params = build_overall_keyword_trends_query(
        date_from=filters["date_from"],
        date_to=filters["date_to"],
        sources=filters["sources"],
        ngram_sizes=filters["ngram_sizes"],
        keyword_search=filters["keyword_search"],
        limit=filters["top_n"],
    )
    overall_df = run_dataframe_query(query, tuple(params))

    left, right = st.columns([1.2, 1])
    with left:
        st.subheader("Daily Overall Trends")
        if overall_df.empty:
            st.info("No overall keyword rows match the current filters.")
        else:
            display_df = round_metric_columns(
                overall_df,
                ["weighted_score", "avg_article_score", "final_keyword_score"],
            )
            display_df = shorten_hash_columns(display_df)
            st.dataframe(display_df, width="stretch", hide_index=True)
            render_download_button(
                label="Download overall trends CSV",
                dataframe=display_df,
                prefix="overall-keyword-trends",
                filters=filters,
                key="download_overall_keyword_trends",
            )

    with right:
        st.subheader("Latest-Day Leaders")
        if overall_df.empty:
            st.info("No chart data available.")
            return

        latest_date = overall_df["event_date"].max()
        latest_df = (
            overall_df.loc[overall_df["event_date"] == latest_date, ["keyword", "final_keyword_score"]]
            .head(12)
            .set_index("keyword")
        )
        st.caption(f"Event date: {latest_date}")
        st.bar_chart(latest_df)

    render_keyword_timeseries(
        title="Keyword Momentum By Day",
        date_from=filters["date_from"],
        date_to=filters["date_to"],
        sources=filters["sources"],
        ngram_sizes=filters["ngram_sizes"],
        keyword_search=filters["keyword_search"],
        limit_keywords=min(max(filters["top_n"] // 10, 3), 8),
    )
    render_breakout_table(filters)


def render_source_tab(filters: dict[str, Any]) -> None:
    query, params = build_source_keyword_trends_query(
        date_from=filters["date_from"],
        date_to=filters["date_to"],
        sources=filters["sources"],
        ngram_sizes=filters["ngram_sizes"],
        keyword_search=filters["keyword_search"],
        limit=filters["top_n"],
    )
    source_df = run_dataframe_query(query, tuple(params))

    st.subheader("Source-Level Keyword Trends")
    if source_df.empty:
        st.info("No source-level keyword rows match the current filters.")
        return

    display_df = round_metric_columns(
        source_df,
        ["weighted_score", "avg_article_score", "source_spread_score", "recency_score", "breakout_score", "final_keyword_score"],
    )
    display_df = shorten_hash_columns(display_df)
    st.dataframe(display_df, width="stretch", hide_index=True)
    render_download_button(
        label="Download source trends CSV",
        dataframe=display_df,
        prefix="source-keyword-trends",
        filters=filters,
        key="download_source_keyword_trends",
    )

    latest_date = source_df["event_date"].max()
    latest_df = source_df.loc[source_df["event_date"] == latest_date]
    if not latest_df.empty:
        pivot_df = (
            latest_df.head(20)
            .assign(keyword_source=lambda frame: frame["source"] + " | " + frame["keyword"])
            .loc[:, ["keyword_source", "final_keyword_score"]]
            .set_index("keyword_source")
        )
        st.caption(f"Latest source keyword scores for {latest_date}")
        st.bar_chart(pivot_df)

    available_sources = sorted(source_df["source"].dropna().unique().tolist())
    focus_source_default = filters["sources"][0] if len(filters["sources"]) == 1 else available_sources[0]
    focus_source = st.selectbox(
        "Focus source",
        options=available_sources,
        index=available_sources.index(focus_source_default),
    )

    render_keyword_timeseries(
        title=f"{focus_source} Keyword History",
        date_from=filters["date_from"],
        date_to=filters["date_to"],
        sources=[focus_source],
        ngram_sizes=filters["ngram_sizes"],
        keyword_search=filters["keyword_search"],
        limit_keywords=min(max(filters["top_n"] // 10, 3), 6),
    )

    source_focus_df = source_df.loc[source_df["source"] == focus_source].copy()
    if source_focus_df.empty:
        return

    keyword_options = source_focus_df["keyword"].dropna().unique().tolist()
    selected_keyword = st.selectbox(
        "Inspect keyword within selected source",
        options=[""] + keyword_options,
        format_func=lambda value: "All keywords" if value == "" else value,
    )

    source_article_query, source_article_params = build_article_keywords_query(
        date_from=filters["date_from"],
        date_to=filters["date_to"],
        sources=[focus_source],
        ngram_sizes=filters["ngram_sizes"],
        keyword_search=selected_keyword or filters["keyword_search"],
        title_search=filters["title_search"],
        limit=filters["top_n"],
    )
    source_article_df = run_dataframe_query(source_article_query, tuple(source_article_params))

    st.subheader(f"{focus_source} Article Drill-Down")
    if source_article_df.empty:
        st.info("No article drill-down rows match the current source filters.")
    else:
        source_article_df = round_metric_columns(source_article_df, ["base_score", "quality_penalty", "article_score"])
        source_article_df = shorten_hash_columns(source_article_df)
        st.dataframe(
            source_article_df,
            width="stretch",
            hide_index=True,
            column_config={
                "link": st.column_config.LinkColumn("Article link"),
            },
        )
        render_download_button(
            label="Download source article drill-down CSV",
            dataframe=source_article_df,
            prefix="source-article-drilldown",
            filters=filters,
            key="download_source_article_drilldown",
        )


def render_article_tab(filters: dict[str, Any]) -> None:
    query, params = build_article_keywords_query(
        date_from=filters["date_from"],
        date_to=filters["date_to"],
        sources=filters["sources"],
        ngram_sizes=filters["ngram_sizes"],
        keyword_search=filters["keyword_search"],
        title_search=filters["title_search"],
        limit=filters["top_n"],
    )
    article_df = run_dataframe_query(query, tuple(params))

    st.subheader("Article Keyword Explorer")
    if article_df.empty:
        st.info("No article keyword rows match the current filters.")
        return

    display_df = round_metric_columns(article_df, ["base_score", "quality_penalty", "article_score"])
    display_df = shorten_hash_columns(display_df)
    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "link": st.column_config.LinkColumn("Article link"),
        },
    )
    render_download_button(
        label="Download article keywords CSV",
        dataframe=display_df,
        prefix="article-keywords",
        filters=filters,
        key="download_article_keywords",
    )


def render_keyword_detail_tab(filters: dict[str, Any]) -> None:
    options_query, options_params = build_overall_keyword_trends_query(
        date_from=filters["date_from"],
        date_to=filters["date_to"],
        sources=filters["sources"],
        ngram_sizes=filters["ngram_sizes"],
        keyword_search=filters["keyword_search"],
        limit=max(filters["top_n"], 100),
    )
    keyword_options_df = run_dataframe_query(options_query, tuple(options_params))
    keyword_option_frame = build_keyword_option_frame(keyword_options_df)

    st.subheader("Keyword Detail Explorer")
    if keyword_option_frame.empty:
        st.info("No keyword candidates are available for the current filters.")
        return

    keyword_labels = {
        row["keyword_normalized"]: row["keyword"]
        for _, row in keyword_option_frame.iterrows()
    }
    selected_keyword = st.selectbox(
        "Keyword to inspect",
        options=keyword_option_frame["keyword_normalized"].tolist(),
        format_func=lambda value: keyword_labels.get(value, value),
    )

    detail_query, detail_params = build_keyword_detail_query(
        date_from=filters["date_from"],
        date_to=filters["date_to"],
        sources=filters["sources"],
        ngram_sizes=filters["ngram_sizes"],
        keyword_normalized=selected_keyword,
        limit=max(filters["top_n"] * 3, 100),
    )
    detail_df = run_dataframe_query(detail_query, tuple(detail_params))

    compare_query, compare_params = build_keyword_source_compare_query(
        date_from=filters["date_from"],
        date_to=filters["date_to"],
        sources=filters["sources"],
        ngram_sizes=filters["ngram_sizes"],
        keyword_normalized=selected_keyword,
        limit=max(filters["top_n"] * 4, 120),
    )
    compare_df = run_dataframe_query(compare_query, tuple(compare_params))

    article_query, article_params = build_article_keywords_query(
        date_from=filters["date_from"],
        date_to=filters["date_to"],
        sources=filters["sources"],
        ngram_sizes=filters["ngram_sizes"],
        keyword_search="",
        title_search=filters["title_search"],
        limit=max(filters["top_n"], 50),
        keyword_normalized_exact=selected_keyword,
    )
    article_df = run_dataframe_query(article_query, tuple(article_params))

    if detail_df.empty:
        st.info("No detail rows match the selected keyword.")
        return

    latest_event_date = detail_df["event_date"].max()
    latest_rows = detail_df.loc[detail_df["event_date"] == latest_event_date]

    metric_1, metric_2, metric_3, metric_4 = st.columns(4)
    metric_1.metric("Tracked Days", int(detail_df["event_date"].nunique()))
    metric_2.metric("Sources In Slice", int(detail_df["source"].nunique()))
    metric_3.metric("Supporting Articles", int(detail_df["article_count"].sum()))
    metric_4.metric(
        "Latest Final Score",
        round(float(latest_rows["final_keyword_score"].sum()), 2),
    )
    st.caption(
        f"Inspecting `{selected_keyword}` | Latest event date: {latest_event_date} | Score version(s): {', '.join(sorted(detail_df['keyword_score_version'].dropna().astype(str).unique().tolist()))}"
    )

    compare_display_df = round_metric_columns(
        compare_df,
        ["weighted_score", "final_keyword_score"],
    )
    compare_display_df = shorten_hash_columns(compare_display_df)
    detail_display_df = round_metric_columns(
        detail_df,
        [
            "base_score",
            "quality_penalty",
            "weighted_score",
            "avg_article_score",
            "source_spread_score",
            "recency_score",
            "breakout_score",
            "final_keyword_score",
        ],
    )
    detail_display_df = shorten_hash_columns(detail_display_df)

    left, right = st.columns([1.15, 1])
    with left:
        st.caption("Source comparison by day")
        source_pivot = compare_df.pivot_table(
            index="event_date",
            columns="source",
            values="final_keyword_score",
            aggfunc="sum",
            fill_value=0,
        )
        st.line_chart(source_pivot, width="stretch")
    with right:
        st.caption("Latest-day source leaders")
        latest_compare_df = compare_df.loc[compare_df["event_date"] == latest_event_date]
        latest_source_scores = (
            latest_compare_df.loc[:, ["source", "final_keyword_score"]]
            .set_index("source")
            .sort_values("final_keyword_score", ascending=False)
        )
        st.bar_chart(latest_source_scores, width="stretch")

    compare_col, detail_col = st.columns(2)
    with compare_col:
        st.subheader("Compare Sources")
        st.dataframe(compare_display_df, width="stretch", hide_index=True)
        render_download_button(
            label="Download source compare CSV",
            dataframe=compare_display_df,
            prefix="source-compare",
            filters=filters,
            key="download_keyword_source_compare",
        )
    with detail_col:
        st.subheader("Keyword Detail Rows")
        st.dataframe(detail_display_df, width="stretch", hide_index=True)
        render_download_button(
            label="Download keyword detail CSV",
            dataframe=detail_display_df,
            prefix="keyword-detail",
            filters=filters,
            key="download_keyword_detail",
        )

    st.subheader("Supporting Articles")
    if article_df.empty:
        st.info("No supporting article rows match the current keyword and title filters.")
        return

    article_display_df = round_metric_columns(
        article_df,
        ["base_score", "quality_penalty", "article_score"],
    )
    article_display_df = shorten_hash_columns(article_display_df)
    st.dataframe(
        article_display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "link": st.column_config.LinkColumn("Article link"),
        },
    )
    render_download_button(
        label="Download supporting articles CSV",
        dataframe=article_display_df,
        prefix="keyword-supporting-articles",
        filters=filters,
        key="download_keyword_supporting_articles",
    )


def main() -> None:
    page_setup()
    render_header()
    ensure_refresh_state()

    try:
        if st.session_state.get("active_dag_run_id"):
            try:
                poll_refresh_status()
            except AirflowApiError as exc:
                st.session_state["refresh_status"] = "failed"
                st.session_state["refresh_error"] = str(exc)
                st.session_state["active_dag_run_id"] = None

        filters = sidebar_filters()
        metrics = render_metrics(filters)
        render_refresh_status_panel(filters, metrics)
        overall_tab, source_tab, article_tab, keyword_tab = st.tabs(
            ["Overall Trends", "Source Trends", "Article Keywords", "Keyword Detail"]
        )
        with overall_tab:
            render_overall_tab(filters)
        with source_tab:
            render_source_tab(filters)
        with article_tab:
            render_article_tab(filters)
        with keyword_tab:
            render_keyword_detail_tab(filters)
    except psycopg2.Error as exc:
        st.error("Unable to connect to analytics PostgreSQL.")
        st.code(str(exc))
        st.info(
            "Set ANALYTICS_DB_* environment variables or Streamlit secrets before starting the app."
        )

    if st.session_state.get("active_dag_run_id"):
        time.sleep(5)
        st.rerun()


if __name__ == "__main__":
    main()
