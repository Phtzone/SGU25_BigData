import json
from typing import Any, Dict

from common.article_schema import normalize_article_record, normalize_text, validate_article_record
from common.kafka_utils import create_kafka_client_with_retry


def build_message_key(article: Dict[str, Any]) -> str:
    return normalize_text(article.get("link"))


class NewsKafkaProducer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9093",
        topic: str = "news_raw",
        *,
        logger: Any | None = None,
        timeout_seconds: float | None = None,
        check_interval_seconds: float | None = None,
        producer_factory: Any | None = None,
    ):
        from kafka import KafkaProducer

        producer_factory = producer_factory or KafkaProducer
        self.topic = topic
        self.producer = create_kafka_client_with_retry(
            client_name="producer",
            bootstrap_servers=bootstrap_servers,
            logger=logger,
            timeout_seconds=timeout_seconds,
            check_interval_seconds=check_interval_seconds,
            factory=lambda: producer_factory(
                bootstrap_servers=bootstrap_servers,
                acks="all",
                retries=3,
                retry_backoff_ms=1000,
                linger_ms=50,
                key_serializer=lambda value: value.encode("utf-8"),
                value_serializer=lambda value: json.dumps(
                    value, ensure_ascii=False
                ).encode("utf-8"),
            ),
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
