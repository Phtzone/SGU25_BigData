from typing import Any, Dict, List

from common.article_schema import build_article_record, is_valid_article_record


def normalize_entry(entry: Any, source_label: str) -> Dict[str, Any]:
    return build_article_record(
        title=entry.get("title", ""),
        link=entry.get("link", ""),
        summary=entry.get("summary") or entry.get("description", ""),
        published_at=entry.get("published") or entry.get("updated", ""),
        source=source_label,
    )


def is_valid_article(article: Dict[str, Any]) -> bool:
    return is_valid_article_record(article)


def split_valid_articles(articles: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    valid_articles: List[Dict[str, Any]] = []
    invalid_articles: List[Dict[str, Any]] = []

    for article in articles:
        if is_valid_article(article):
            valid_articles.append(article)
        else:
            invalid_articles.append(article)

    return valid_articles, invalid_articles


def fetch_articles_from_rss_batch(feed_url: str, source_label: str) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    import feedparser

    feed = feedparser.parse(feed_url)
    normalized_articles = [normalize_entry(entry, source_label) for entry in feed.entries]
    valid_articles, _ = split_valid_articles(normalized_articles)
    return normalized_articles, valid_articles


def fetch_articles_from_rss(feed_url: str, source_label: str) -> List[Dict[str, Any]]:
    _, valid_articles = fetch_articles_from_rss_batch(feed_url, source_label)
    return valid_articles


def deduplicate_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen_links = set()
    unique_articles = []

    for article in articles:
        link = article.get("link", "")
        if not link:
            continue
        if link not in seen_links:
            seen_links.add(link)
            unique_articles.append(article)

    return unique_articles
