from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from uuid import uuid4

ARTICLE_FIELDS = (
    "title",
    "link",
    "summary",
    "published_at",
    "published_at_raw",
    "source",
    "fetched_at",
    "ingestion_id",
)

REQUIRED_ARTICLE_FIELDS = ("title", "link", "source")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split())


def parse_datetime_to_utc(value: Any) -> datetime | None:
    text = normalize_text(value)
    if not text:
        return None

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError):
            return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def normalize_datetime_string(value: Any) -> str:
    parsed = parse_datetime_to_utc(value)
    return parsed.isoformat() if parsed else ""


def build_article_record(
    *,
    title: Any,
    link: Any,
    summary: Any,
    published_at: Any,
    source: Any,
    fetched_at: str | None = None,
    ingestion_id: str | None = None,
) -> dict[str, str]:
    normalized_published = normalize_datetime_string(published_at)

    return {
        "title": normalize_text(title),
        "link": normalize_text(link),
        "summary": normalize_text(summary),
        "published_at": normalized_published,
        "published_at_raw": normalize_text(published_at),
        "source": normalize_text(source),
        "fetched_at": fetched_at or utc_now_iso(),
        "ingestion_id": ingestion_id or uuid4().hex,
    }


def normalize_article_record(article: dict[str, Any]) -> dict[str, str]:
    fetched_at = normalize_datetime_string(article.get("fetched_at")) or utc_now_iso()
    published_source = article.get("published_at") or article.get("published_at_raw")

    normalized = build_article_record(
        title=article.get("title"),
        link=article.get("link"),
        summary=article.get("summary"),
        published_at=published_source,
        source=article.get("source"),
        fetched_at=fetched_at,
        ingestion_id=normalize_text(article.get("ingestion_id")) or None,
    )

    published_at_raw = normalize_text(article.get("published_at_raw"))
    if published_at_raw:
        normalized["published_at_raw"] = published_at_raw

    return normalized


def validate_article_record(article: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    for field in REQUIRED_ARTICLE_FIELDS:
        if not normalize_text(article.get(field)):
            errors.append(f"{field} is required")

    fetched_at = normalize_text(article.get("fetched_at"))
    if fetched_at and not normalize_datetime_string(fetched_at):
        errors.append("fetched_at must be a valid datetime")

    return errors


def is_valid_article_record(article: dict[str, Any]) -> bool:
    return not validate_article_record(article)
