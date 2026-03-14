import json
from typing import Any, Dict

from kafka import KafkaProducer


class NewsKafkaProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9093", topic: str = "news_raw"):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(
                value, ensure_ascii=False
            ).encode("utf-8"),
        )

    def send_article(self, article: Dict[str, Any]) -> None:
        self.producer.send(self.topic, article)

    def flush(self) -> None:
        self.producer.flush()

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()
