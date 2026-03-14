from datetime import datetime, timezone
from typing import Any, Dict, List

import feedparser


def normalize_entry(entry: Any, source_label: str) -> Dict[str, Any]:
    return {
        "title": entry.get("title", "").strip(),
        "link": entry.get("link", "").strip(),
        "summary": entry.get("summary", "").strip(),
        "published_at": entry.get("published", "").strip(),
        "source": source_label,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def is_valid_article(article: Dict[str, Any]) -> bool:
    return bool(article["title"] and article["link"])


def fetch_articles_from_rss(feed_url: str, source_label: str) -> List[Dict[str, Any]]:
    feed = feedparser.parse(feed_url)
    articles: List[Dict[str, Any]] = []

    for entry in feed.entries:
        article = normalize_entry(entry, source_label)
        if is_valid_article(article):
            articles.append(article)

    return articles


def deduplicate_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen_links = set()
    unique_articles = []

    for article in articles:
        link = article["link"]
        if link not in seen_links:
            seen_links.add(link)
            unique_articles.append(article)

    return unique_articles
