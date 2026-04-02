from __future__ import annotations

from collections import Counter
from typing import Any, Iterable

from common.article_schema import normalize_text


def summarize_article_quality(
    rows: list[dict[str, Any]],
    *,
    expected_sources: Iterable[str] | None = None,
) -> dict[str, Any]:
    total_count = len(rows)
    missing_title_count = sum(1 for row in rows if not normalize_text(row.get("title")))
    missing_link_count = sum(1 for row in rows if not normalize_text(row.get("link")))

    seen_links: set[str] = set()
    duplicate_count = 0
    source_counter: Counter[str] = Counter()

    for row in rows:
        link = normalize_text(row.get("link"))
        source = normalize_text(row.get("source")) or "unknown"
        source_counter[source] += 1

        if link and link in seen_links:
            duplicate_count += 1
        elif link:
            seen_links.add(link)

    normalized_expected_sources = [
        normalize_text(source) for source in (expected_sources or []) if normalize_text(source)
    ]
    zero_article_sources = sorted(
        source for source in normalized_expected_sources if source_counter.get(source, 0) == 0
    )

    return {
        "total_count": total_count,
        "missing_title_count": missing_title_count,
        "missing_title_rate": round((missing_title_count / total_count), 4) if total_count else 0.0,
        "missing_link_count": missing_link_count,
        "missing_link_rate": round((missing_link_count / total_count), 4) if total_count else 0.0,
        "duplicate_count": duplicate_count,
        "articles_by_source": dict(sorted(source_counter.items())),
        "zero_article_sources": zero_article_sources,
    }
