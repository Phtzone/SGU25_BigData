import json
from typing import Any, Dict

from common.article_schema import normalize_article_record, normalize_text, validate_article_record


def build_message_key(article: Dict[str, Any]) -> str:
    return normalize_text(article.get("link"))


class NewsKafkaProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9093", topic: str = "news_raw"):
        from kafka import KafkaProducer

        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            retries=3,
            retry_backoff_ms=1000,
            linger_ms=50,
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=lambda value: json.dumps(
                value, ensure_ascii=False
            ).encode("utf-8"),
        )

    def send_article(self, article: Dict[str, Any]) -> Dict[str, Any]:
        normalized = normalize_article_record(article)
        errors = validate_article_record(normalized)
        if errors:
            raise ValueError(f"Invalid article payload for Kafka: {', '.join(errors)}")

        self.producer.send(self.topic, key=build_message_key(normalized), value=normalized)
        return normalized

    def flush(self) -> None:
        self.producer.flush()

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()
