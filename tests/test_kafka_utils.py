import unittest

from common.kafka_utils import create_kafka_client_with_retry


class KafkaUtilsTests(unittest.TestCase):
    def test_create_kafka_client_with_retry_retries_until_success(self) -> None:
        attempts: list[int] = []

        def factory() -> str:
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError("broker not ready")
            return "connected"

        client = create_kafka_client_with_retry(
            client_name="producer",
            bootstrap_servers="kafka:29092",
            factory=factory,
            timeout_seconds=1,
            check_interval_seconds=0,
        )

        self.assertEqual(client, "connected")
        self.assertEqual(len(attempts), 3)

    def test_create_kafka_client_with_retry_raises_timeout(self) -> None:
        attempts: list[int] = []

        def factory() -> str:
            attempts.append(1)
            raise RuntimeError("broker not ready")

        with self.assertRaises(TimeoutError):
            create_kafka_client_with_retry(
                client_name="consumer",
                bootstrap_servers="kafka:29092",
                factory=factory,
                timeout_seconds=0,
                check_interval_seconds=0,
            )

        self.assertEqual(len(attempts), 1)


if __name__ == "__main__":
    unittest.main()
