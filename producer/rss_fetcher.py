from typing import Any, Dict, List

import feedparser
from common.article_schema import build_article_record, is_valid_article_record


def normalize_entry(entry: Any, source_label: str) -> Dict[str, Any]:
    return build_article_record(
        title=entry.get("title", ""),
        link=entry.get("link", ""),
        summary=entry.get("summary", ""),
        published_at=entry.get("published", ""),
        source=source_label,
    )


def is_valid_article(article: Dict[str, Any]) -> bool:
    return is_valid_article_record(article)


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
