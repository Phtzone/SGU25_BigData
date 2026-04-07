from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any

import pandas as pd
import psycopg2
import streamlit as st

try:
    from dashboard.query_builders import (
        build_article_keywords_query,
        build_breakout_keywords_query,
        build_keyword_metrics_query,
        build_keyword_timeseries_query,
        build_overall_keyword_trends_query,
        build_source_keyword_trends_query,
        build_source_options_query,
    )
except ModuleNotFoundError:
    from query_builders import (
        build_article_keywords_query,
        build_breakout_keywords_query,
        build_keyword_metrics_query,
        build_keyword_timeseries_query,
        build_overall_keyword_trends_query,
        build_source_keyword_trends_query,
        build_source_options_query,
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


def sidebar_filters() -> dict[str, Any]:
    today = date.today()
    default_start = today - timedelta(days=6)

    with st.sidebar:
        st.header("Filters")
        date_from = st.date_input("Start date", value=default_start)
        date_to = st.date_input("End date", value=today)
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

        if st.button("Refresh data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    return {
        "date_from": date_from,
        "date_to": date_to,
        "sources": selected_sources,
        "ngram_sizes": ngram_sizes,
        "top_n": top_n,
        "keyword_search": keyword_search,
        "title_search": title_search,
    }


def render_metrics(filters: dict[str, Any]) -> None:
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


def round_metric_columns(dataframe: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    rounded_df = dataframe.copy()
    for column_name in columns:
        if column_name in rounded_df.columns:
            rounded_df[column_name] = rounded_df[column_name].round(2)
    return rounded_df


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
        values="weighted_score",
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
        st.caption("Weighted score by day")
        st.line_chart(weighted_pivot, use_container_width=True)
    with chart_right:
        st.caption("Supporting articles by day")
        st.line_chart(article_pivot, use_container_width=True)


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
            "previous_avg_weighted_score",
            "breakout_score",
            "previous_avg_article_count",
            "article_count_delta",
        ],
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)


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
                ["weighted_score", "avg_article_score"],
            )
            st.dataframe(display_df, use_container_width=True, hide_index=True)

    with right:
        st.subheader("Latest-Day Leaders")
        if overall_df.empty:
            st.info("No chart data available.")
            return

        latest_date = overall_df["event_date"].max()
        latest_df = (
            overall_df.loc[overall_df["event_date"] == latest_date, ["keyword", "weighted_score"]]
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

    display_df = round_metric_columns(source_df, ["weighted_score", "avg_article_score"])
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    latest_date = source_df["event_date"].max()
    latest_df = source_df.loc[source_df["event_date"] == latest_date]
    if not latest_df.empty:
        pivot_df = (
            latest_df.head(20)
            .assign(keyword_source=lambda frame: frame["source"] + " | " + frame["keyword"])
            .loc[:, ["keyword_source", "weighted_score"]]
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
        source_article_df = round_metric_columns(source_article_df, ["article_score"])
        st.dataframe(
            source_article_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "link": st.column_config.LinkColumn("Article link"),
            },
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

    display_df = round_metric_columns(article_df, ["article_score"])
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "link": st.column_config.LinkColumn("Article link"),
        },
    )


def main() -> None:
    page_setup()
    render_header()

    try:
        filters = sidebar_filters()
        render_metrics(filters)
        overall_tab, source_tab, article_tab = st.tabs(
            ["Overall Trends", "Source Trends", "Article Keywords"]
        )
        with overall_tab:
            render_overall_tab(filters)
        with source_tab:
            render_source_tab(filters)
        with article_tab:
            render_article_tab(filters)
    except psycopg2.Error as exc:
        st.error("Unable to connect to analytics PostgreSQL.")
        st.code(str(exc))
        st.info(
            "Set ANALYTICS_DB_* environment variables or Streamlit secrets before starting the app."
        )


if __name__ == "__main__":
    main()
