from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a keyword review sample from PostgreSQL for manual quality checks."
    )
    parser.add_argument(
        "--output-path",
        default="data/sample_output/keyword_review_sample.csv",
        help="Local CSV path for the review sample output.",
    )
    parser.add_argument("--event-date", default="", help="Optional exact event_date filter in YYYY-MM-DD format.")
    parser.add_argument("--source", default="", help="Optional exact source filter.")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of rows to export.")
    parser.add_argument("--db-host", default=os.getenv("ANALYTICS_DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("ANALYTICS_DB_PORT", "5433")))
    parser.add_argument("--db-name", default=os.getenv("ANALYTICS_DB_NAME", "analytics"))
    parser.add_argument("--db-user", default=os.getenv("ANALYTICS_DB_USER", "analytics"))
    parser.add_argument("--db-password", default=os.getenv("ANALYTICS_DB_PASSWORD", "analytics"))
    return parser.parse_args()


def main() -> None:
    import psycopg2

    args = parse_args()
    filters: list[str] = []
    params: list[object] = []

    if args.event_date.strip():
        filters.append("event_date = %s")
        params.append(args.event_date.strip())
    if args.source.strip():
        filters.append("source = %s")
        params.append(args.source.strip())

    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)

    params.append(max(args.limit, 1))
    query = f"""
        SELECT
            event_date,
            source,
            keyword,
            keyword_normalized,
            article_count,
            weighted_score,
            final_keyword_score,
            quality_flags,
            keyword_score_version,
            keyword_config_hash
        FROM vw_streamlit_keyword_daily_source_latest
        {where_clause}
        ORDER BY event_date DESC, source ASC, final_keyword_score DESC, rank_in_group ASC
        LIMIT %s
    """

    with psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
        connect_timeout=10,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]

    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(columns + ["review_label", "review_note"])
        for row in rows:
            writer.writerow(list(row) + ["", ""])

    print(f"Exported {len(rows)} keyword review rows to {output_path}")


if __name__ == "__main__":
    main()
