import os
import sys

from config.sources import load_rss_sources
from producer.kafka_producer import NewsKafkaProducer
from producer.rss_fetcher import deduplicate_articles, fetch_articles_from_rss


def configure_console_output() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")


def main() -> None:
    configure_console_output()
    rss_sources = load_rss_sources()
    producer = NewsKafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"),
        topic=os.getenv("KAFKA_TOPIC", "news_raw"),
    )

    all_articles = []

    for source in rss_sources:
        print(f"Fetching from {source['label']}...")
        articles = fetch_articles_from_rss(
            feed_url=source["url"],
            source_label=source["label"],
        )
        print(f"Fetched {len(articles)} articles from {source['label']}")
        all_articles.extend(articles)

    unique_articles = deduplicate_articles(all_articles)
    print(f"Total unique articles: {len(unique_articles)}")

    for article in unique_articles:
        producer.send_article(article)
        print(f"Sent: {article['title']}")

    producer.close()
    print("All articles sent to Kafka topic 'news_raw'")


if __name__ == "__main__":
    main()
