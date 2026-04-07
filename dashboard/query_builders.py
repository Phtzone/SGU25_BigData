from __future__ import annotations

from datetime import date
from typing import Any, Iterable

MAX_DASHBOARD_ROWS = 500


def clamp_limit(limit: int, *, max_limit: int = MAX_DASHBOARD_ROWS) -> int:
    return max(1, min(int(limit), max_limit))


def append_date_filters(
    filters: list[str],
    params: list[Any],
    *,
    column_name: str,
    date_from: date | None,
    date_to: date | None,
) -> None:
    if date_from is not None:
        filters.append(f"{column_name} >= %s")
        params.append(date_from)
    if date_to is not None:
        filters.append(f"{column_name} <= %s")
        params.append(date_to)


def append_in_filter(
    filters: list[str],
    params: list[Any],
    *,
    column_name: str,
    values: Iterable[Any],
) -> None:
    cleaned_values = [value for value in values if value not in (None, "")]
    if not cleaned_values:
        return

    placeholders = ", ".join(["%s"] * len(cleaned_values))
    filters.append(f"{column_name} IN ({placeholders})")
    params.extend(cleaned_values)


def append_search_filter(
    filters: list[str],
    params: list[Any],
    *,
    column_name: str,
    search_term: str,
) -> None:
    normalized_term = search_term.strip()
    if not normalized_term:
        return

    filters.append(f"{column_name} ILIKE %s")
    params.append(f"%{normalized_term}%")


def compose_where_clause(filters: list[str]) -> str:
    if not filters:
        return ""
    return " WHERE " + " AND ".join(filters)


def build_source_options_query(
    *,
    date_from: date | None,
    date_to: date | None,
) -> tuple[str, list[Any]]:
    filters: list[str] = []
    params: list[Any] = []
    append_date_filters(
        filters,
        params,
        column_name="event_date",
        date_from=date_from,
        date_to=date_to,
    )
    query = f"""
        SELECT DISTINCT source
        FROM vw_streamlit_keyword_daily_source_latest
        {compose_where_clause(filters)}
        ORDER BY source ASC
    """
    return query, params


def build_keyword_metrics_query(
    *,
    date_from: date | None,
    date_to: date | None,
    sources: Iterable[str],
    ngram_sizes: Iterable[int],
    keyword_search: str,
) -> tuple[str, list[Any]]:
    filters: list[str] = []
    params: list[Any] = []
    append_date_filters(
        filters,
        params,
        column_name="event_date",
        date_from=date_from,
        date_to=date_to,
    )
    append_in_filter(filters, params, column_name="source", values=sources)
    append_in_filter(filters, params, column_name="ngram_size", values=ngram_sizes)
    append_search_filter(
        filters,
        params,
        column_name="keyword_normalized",
        search_term=keyword_search,
    )
    query = f"""
        SELECT
            COUNT(*) AS keyword_rows,
            COUNT(DISTINCT keyword_normalized) AS distinct_keywords,
            COUNT(DISTINCT source) AS source_count,
            COALESCE(SUM(article_count), 0) AS supporting_articles,
            MAX(event_date) AS latest_event_date
        FROM vw_streamlit_keyword_daily_source_latest
        {compose_where_clause(filters)}
    """
    return query, params


def build_overall_keyword_trends_query(
    *,
    date_from: date | None,
    date_to: date | None,
    sources: Iterable[str],
    ngram_sizes: Iterable[int],
    keyword_search: str,
    limit: int,
) -> tuple[str, list[Any]]:
    filters: list[str] = []
    params: list[Any] = []
    append_date_filters(
        filters,
        params,
        column_name="event_date",
        date_from=date_from,
        date_to=date_to,
    )
    append_in_filter(filters, params, column_name="source", values=sources)
    append_in_filter(filters, params, column_name="ngram_size", values=ngram_sizes)
    append_search_filter(
        filters,
        params,
        column_name="keyword_normalized",
        search_term=keyword_search,
    )
    query = f"""
        WITH filtered_source_keywords AS (
            SELECT
                event_date,
                source,
                keyword,
                keyword_normalized,
                ngram_size,
                article_count,
                weighted_score,
                avg_article_score
            FROM vw_streamlit_keyword_daily_source_latest
            {compose_where_clause(filters)}
        ),
        aggregated_keywords AS (
            SELECT
                event_date,
                keyword,
                keyword_normalized,
                MAX(ngram_size) AS ngram_size,
                COUNT(DISTINCT source) AS source_count,
                SUM(article_count) AS article_count,
                SUM(weighted_score) AS weighted_score,
                AVG(avg_article_score) AS avg_article_score
            FROM filtered_source_keywords
            GROUP BY event_date, keyword, keyword_normalized
        )
        SELECT
            event_date,
            keyword,
            keyword_normalized,
            ngram_size,
            source_count,
            article_count,
            weighted_score,
            avg_article_score,
            ROW_NUMBER() OVER (
                PARTITION BY event_date
                ORDER BY
                    weighted_score DESC,
                    article_count DESC,
                    ngram_size DESC,
                    keyword_normalized ASC
            ) AS rank_in_day
        FROM aggregated_keywords
        ORDER BY event_date DESC, rank_in_day ASC
        LIMIT %s
    """
    params.append(clamp_limit(limit))
    return query, params


def build_keyword_timeseries_query(
    *,
    date_from: date | None,
    date_to: date | None,
    sources: Iterable[str],
    ngram_sizes: Iterable[int],
    keyword_search: str,
    limit_keywords: int,
) -> tuple[str, list[Any]]:
    filters: list[str] = []
    params: list[Any] = []
    append_date_filters(
        filters,
        params,
        column_name="event_date",
        date_from=date_from,
        date_to=date_to,
    )
    append_in_filter(filters, params, column_name="source", values=sources)
    append_in_filter(filters, params, column_name="ngram_size", values=ngram_sizes)
    append_search_filter(
        filters,
        params,
        column_name="keyword_normalized",
        search_term=keyword_search,
    )
    query = f"""
        WITH filtered_source_keywords AS (
            SELECT
                event_date,
                keyword,
                keyword_normalized,
                article_count,
                weighted_score
            FROM vw_streamlit_keyword_daily_source_latest
            {compose_where_clause(filters)}
        ),
        aggregated_keywords AS (
            SELECT
                event_date,
                keyword,
                keyword_normalized,
                SUM(article_count) AS article_count,
                SUM(weighted_score) AS weighted_score
            FROM filtered_source_keywords
            GROUP BY event_date, keyword, keyword_normalized
        ),
        top_keywords AS (
            SELECT keyword_normalized
            FROM aggregated_keywords
            GROUP BY keyword_normalized
            ORDER BY
                SUM(weighted_score) DESC,
                SUM(article_count) DESC,
                keyword_normalized ASC
            LIMIT %s
        )
        SELECT
            aggregated_keywords.event_date,
            aggregated_keywords.keyword,
            aggregated_keywords.keyword_normalized,
            aggregated_keywords.article_count,
            aggregated_keywords.weighted_score
        FROM aggregated_keywords
        INNER JOIN top_keywords
            ON top_keywords.keyword_normalized = aggregated_keywords.keyword_normalized
        ORDER BY aggregated_keywords.event_date ASC, aggregated_keywords.weighted_score DESC
    """
    params.append(clamp_limit(limit_keywords, max_limit=20))
    return query, params


def build_breakout_keywords_query(
    *,
    date_from: date | None,
    date_to: date | None,
    sources: Iterable[str],
    ngram_sizes: Iterable[int],
    keyword_search: str,
    limit: int,
) -> tuple[str, list[Any]]:
    filters: list[str] = []
    params: list[Any] = []
    append_date_filters(
        filters,
        params,
        column_name="event_date",
        date_from=date_from,
        date_to=date_to,
    )
    append_in_filter(filters, params, column_name="source", values=sources)
    append_in_filter(filters, params, column_name="ngram_size", values=ngram_sizes)
    append_search_filter(
        filters,
        params,
        column_name="keyword_normalized",
        search_term=keyword_search,
    )
    query = f"""
        WITH filtered_source_keywords AS (
            SELECT
                event_date,
                source,
                keyword,
                keyword_normalized,
                ngram_size,
                article_count,
                weighted_score
            FROM vw_streamlit_keyword_daily_source_latest
            {compose_where_clause(filters)}
        ),
        aggregated_keywords AS (
            SELECT
                event_date,
                keyword,
                keyword_normalized,
                MAX(ngram_size) AS ngram_size,
                COUNT(DISTINCT source) AS source_count,
                SUM(article_count) AS article_count,
                SUM(weighted_score) AS weighted_score
            FROM filtered_source_keywords
            GROUP BY event_date, keyword, keyword_normalized
        ),
        ranked_keywords AS (
            SELECT
                event_date,
                keyword,
                keyword_normalized,
                ngram_size,
                source_count,
                article_count,
                weighted_score,
                ROW_NUMBER() OVER (
                    PARTITION BY event_date
                    ORDER BY
                        weighted_score DESC,
                        article_count DESC,
                        ngram_size DESC,
                        keyword_normalized ASC
                ) AS rank_in_day
            FROM aggregated_keywords
        ),
        latest_date AS (
            SELECT MAX(event_date) AS latest_event_date
            FROM ranked_keywords
        ),
        history AS (
            SELECT
                keyword_normalized,
                AVG(weighted_score) AS previous_avg_weighted_score,
                AVG(article_count) AS previous_avg_article_count,
                COUNT(*) AS history_days
            FROM ranked_keywords
            WHERE event_date < (SELECT latest_event_date FROM latest_date)
            GROUP BY keyword_normalized
        )
        SELECT
            ranked_keywords.event_date,
            ranked_keywords.keyword,
            ranked_keywords.keyword_normalized,
            ranked_keywords.ngram_size,
            ranked_keywords.source_count,
            ranked_keywords.article_count,
            ranked_keywords.weighted_score,
            ranked_keywords.rank_in_day,
            COALESCE(history.previous_avg_weighted_score, 0) AS previous_avg_weighted_score,
            COALESCE(history.previous_avg_article_count, 0) AS previous_avg_article_count,
            COALESCE(history.history_days, 0) AS history_days,
            ranked_keywords.weighted_score - COALESCE(history.previous_avg_weighted_score, 0) AS breakout_score,
            ranked_keywords.article_count - COALESCE(history.previous_avg_article_count, 0) AS article_count_delta
        FROM ranked_keywords
        LEFT JOIN history
            ON history.keyword_normalized = ranked_keywords.keyword_normalized
        WHERE ranked_keywords.event_date = (SELECT latest_event_date FROM latest_date)
        ORDER BY
            breakout_score DESC,
            ranked_keywords.weighted_score DESC,
            ranked_keywords.article_count DESC,
            ranked_keywords.keyword_normalized ASC
        LIMIT %s
    """
    params.append(clamp_limit(limit))
    return query, params


def build_source_keyword_trends_query(
    *,
    date_from: date | None,
    date_to: date | None,
    sources: Iterable[str],
    ngram_sizes: Iterable[int],
    keyword_search: str,
    limit: int,
) -> tuple[str, list[Any]]:
    filters: list[str] = []
    params: list[Any] = []
    append_date_filters(
        filters,
        params,
        column_name="event_date",
        date_from=date_from,
        date_to=date_to,
    )
    append_in_filter(filters, params, column_name="source", values=sources)
    append_in_filter(filters, params, column_name="ngram_size", values=ngram_sizes)
    append_search_filter(
        filters,
        params,
        column_name="keyword_normalized",
        search_term=keyword_search,
    )
    query = f"""
        SELECT
            event_date,
            source,
            keyword,
            keyword_normalized,
            ngram_size,
            article_count,
            weighted_score,
            avg_article_score,
            rank_in_group
        FROM vw_streamlit_keyword_daily_source_latest
        {compose_where_clause(filters)}
        ORDER BY event_date DESC, source ASC, rank_in_group ASC
        LIMIT %s
    """
    params.append(clamp_limit(limit))
    return query, params


def build_article_keywords_query(
    *,
    date_from: date | None,
    date_to: date | None,
    sources: Iterable[str],
    ngram_sizes: Iterable[int],
    keyword_search: str,
    title_search: str,
    limit: int,
) -> tuple[str, list[Any]]:
    filters: list[str] = []
    params: list[Any] = []
    append_date_filters(
        filters,
        params,
        column_name="event_date",
        date_from=date_from,
        date_to=date_to,
    )
    append_in_filter(filters, params, column_name="source", values=sources)
    append_in_filter(filters, params, column_name="ngram_size", values=ngram_sizes)
    append_search_filter(
        filters,
        params,
        column_name="keyword_normalized",
        search_term=keyword_search,
    )
    append_search_filter(
        filters,
        params,
        column_name="title",
        search_term=title_search,
    )
    query = f"""
        SELECT
            event_date,
            source,
            title,
            link,
            keyword,
            keyword_normalized,
            ngram_size,
            article_score,
            rank_in_article
        FROM vw_streamlit_article_keywords_latest
        {compose_where_clause(filters)}
        ORDER BY event_date DESC, source ASC, rank_in_article ASC, article_score DESC
        LIMIT %s
    """
    params.append(clamp_limit(limit))
    return query, params
