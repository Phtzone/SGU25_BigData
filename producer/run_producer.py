import os
import sys
import time

from common.data_quality import summarize_article_quality
from common.logging_utils import configure_logging, log_event
from config.sources import load_rss_sources
from producer.kafka_producer import NewsKafkaProducer
from producer.rss_fetcher import deduplicate_articles, fetch_articles_from_rss_batch


def configure_console_output() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")


def main() -> None:
    configure_console_output()
    logger = configure_logging("producer")
    started_at = time.perf_counter()
    rss_sources = load_rss_sources()
    producer = NewsKafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"),
        topic=os.getenv("KAFKA_TOPIC", "news_raw"),
    )
    topic = os.getenv("KAFKA_TOPIC", "news_raw")

    all_articles = []
    expected_sources = [source["label"] for source in rss_sources]

    for source in rss_sources:
        source_started_at = time.perf_counter()
        source_label = source["label"]
        log_event(
            logger,
            20,
            "rss_fetch_started",
            source=source_label,
            topic=topic,
        )
        normalized_articles, valid_articles = fetch_articles_from_rss_batch(
            feed_url=source["url"],
            source_label=source_label,
        )
        invalid_count = len(normalized_articles) - len(valid_articles)
        quality = summarize_article_quality(normalized_articles, expected_sources=[source_label])
        log_event(
            logger,
            20,
            "rss_fetch_completed",
            source=source_label,
            topic=topic,
            row_count=len(valid_articles),
            invalid_count=invalid_count,
            duplicate_count=quality["duplicate_count"],
            missing_title_count=quality["missing_title_count"],
            missing_title_rate=quality["missing_title_rate"],
            missing_link_count=quality["missing_link_count"],
            missing_link_rate=quality["missing_link_rate"],
            articles_by_source=quality["articles_by_source"],
            duration_ms=round((time.perf_counter() - source_started_at) * 1000, 2),
        )
        if not valid_articles:
            log_event(
                logger,
                30,
                "source_zero_articles_alert",
                source=source_label,
                topic=topic,
                status="warning",
                duration_ms=round((time.perf_counter() - source_started_at) * 1000, 2),
            )
        all_articles.extend(valid_articles)

    unique_articles = deduplicate_articles(all_articles)
    fetch_quality = summarize_article_quality(all_articles, expected_sources=expected_sources)
    log_event(
        logger,
        20,
        "rss_batch_quality_summary",
        topic=topic,
        row_count=len(all_articles),
        unique_row_count=len(unique_articles),
        duplicate_count=len(all_articles) - len(unique_articles),
        missing_title_count=fetch_quality["missing_title_count"],
        missing_title_rate=fetch_quality["missing_title_rate"],
        missing_link_count=fetch_quality["missing_link_count"],
        missing_link_rate=fetch_quality["missing_link_rate"],
        articles_by_source=fetch_quality["articles_by_source"],
        zero_article_sources=fetch_quality["zero_article_sources"],
    )

    publish_started_at = time.perf_counter()
    for article in unique_articles:
        producer.send_article(article)

    producer.close()
    log_event(
        logger,
        20,
        "kafka_publish_completed",
        topic=topic,
        row_count=len(unique_articles),
        duration_ms=round((time.perf_counter() - publish_started_at) * 1000, 2),
        total_duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
        status="success",
    )


if __name__ == "__main__":
    main()
