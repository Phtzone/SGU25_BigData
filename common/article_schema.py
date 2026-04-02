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
    "source",
    "fetched_at",
    "ingestion_id",
)

REQUIRED_TEXT_FIELDS = ("title", "link", "source", "ingestion_id")
REQUIRED_DATETIME_FIELDS = ("published_at", "fetched_at")


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
    normalized_fetched_at = (
        utc_now_iso() if fetched_at is None else normalize_datetime_string(fetched_at)
    )
    normalized_ingestion_id = normalize_text(ingestion_id) or uuid4().hex

    return {
        "title": normalize_text(title),
        "link": normalize_text(link),
        "summary": normalize_text(summary),
        "published_at": normalized_published,
        "source": normalize_text(source),
        "fetched_at": normalized_fetched_at,
        "ingestion_id": normalized_ingestion_id,
    }


def normalize_article_record(article: dict[str, Any]) -> dict[str, str]:
    return {
        "title": normalize_text(article.get("title")),
        "link": normalize_text(article.get("link")),
        "summary": normalize_text(article.get("summary")),
        "published_at": normalize_datetime_string(article.get("published_at")),
        "source": normalize_text(article.get("source")),
        "fetched_at": normalize_datetime_string(article.get("fetched_at")),
        "ingestion_id": normalize_text(article.get("ingestion_id")),
    }


def validate_article_record(article: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    missing_fields = [field for field in ARTICLE_FIELDS if field not in article]
    if missing_fields:
        errors.append(f"missing fields: {', '.join(missing_fields)}")

    unexpected_fields = [field for field in article if field not in ARTICLE_FIELDS]
    if unexpected_fields:
        errors.append(f"unexpected fields: {', '.join(sorted(unexpected_fields))}")

    for field in REQUIRED_TEXT_FIELDS:
        if not normalize_text(article.get(field)):
            errors.append(f"{field} is required")

    for field in REQUIRED_DATETIME_FIELDS:
        value = normalize_text(article.get(field))
        if not value:
            errors.append(f"{field} is required")
            continue
        if not normalize_datetime_string(value):
            errors.append(f"{field} must be a valid datetime")

    return errors


def is_valid_article_record(article: dict[str, Any]) -> bool:
    return not validate_article_record(article)
