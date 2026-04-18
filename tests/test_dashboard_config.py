import unittest

from dashboard.config_utils import build_airflow_config, resolve_config_value


class DashboardConfigTests(unittest.TestCase):
    def test_resolve_config_value_reads_airflow_section_keys(self) -> None:
        secrets = {
            "airflow": {
                "api_url": "http://localhost:8080/api/v1",
                "username": "TTT",
                "password": "secret",
            }
        }

        self.assertEqual(
            resolve_config_value(
                "AIRFLOW_API_URL",
                "",
                env_resolver=lambda name, default: default,
                secrets=secrets,
            ),
            "http://localhost:8080/api/v1",
        )
        self.assertEqual(
            resolve_config_value(
                "AIRFLOW_USERNAME",
                "",
                env_resolver=lambda name, default: default,
                secrets=secrets,
            ),
            "TTT",
        )
        self.assertEqual(
            resolve_config_value(
                "AIRFLOW_PASSWORD",
                "",
                env_resolver=lambda name, default: default,
                secrets=secrets,
            ),
            "secret",
        )

    def test_resolve_config_value_reads_analytics_db_section_keys(self) -> None:
        secrets = {
            "analytics_db": {
                "host": "localhost",
                "port": 5433,
                "dbname": "analytics",
            }
        }

        self.assertEqual(
            resolve_config_value(
                "ANALYTICS_DB_HOST",
                "",
                env_resolver=lambda name, default: default,
                secrets=secrets,
            ),
            "localhost",
        )
        self.assertEqual(
            resolve_config_value(
                "ANALYTICS_DB_PORT",
                "",
                env_resolver=lambda name, default: default,
                secrets=secrets,
            ),
            "5433",
        )
        self.assertEqual(
            resolve_config_value(
                "ANALYTICS_DB_NAME",
                "",
                env_resolver=lambda name, default: default,
                secrets=secrets,
            ),
            "analytics",
        )

    def test_build_airflow_config_uses_empty_defaults(self) -> None:
        requested_defaults: list[tuple[str, str]] = []

        def fake_resolve(name: str, default: str) -> str:
            requested_defaults.append((name, default))
            return default

        config = build_airflow_config(fake_resolve)

        self.assertEqual(
            config,
            {
                "base_url": "",
                "username": "",
                "password": "",
            },
        )
        self.assertEqual(
            requested_defaults,
            [
                ("AIRFLOW_API_URL", ""),
                ("AIRFLOW_USERNAME", ""),
                ("AIRFLOW_PASSWORD", ""),
            ],
        )

    def test_build_airflow_config_keeps_explicit_values(self) -> None:
        values = {
            "AIRFLOW_API_URL": "http://localhost:8080/api/v1",
            "AIRFLOW_USERNAME": "TTT",
            "AIRFLOW_PASSWORD": "secret",
        }

        config = build_airflow_config(lambda name, default: values.get(name, default))

        self.assertEqual(
            config,
            {
                "base_url": "http://localhost:8080/api/v1",
                "username": "TTT",
                "password": "secret",
            },
        )

    def test_resolve_config_value_falls_back_when_secrets_access_raises(self) -> None:
        class ExplodingSecrets(dict):
            def get(self, key, default=None):  # type: ignore[override]
                raise RuntimeError("No secrets found.")

        self.assertEqual(
            resolve_config_value(
                "APP_TIMEZONE",
                "Asia/Bangkok",
                env_resolver=lambda name, default: "UTC",
                secrets=ExplodingSecrets(),
            ),
            "UTC",
        )


if __name__ == "__main__":
    unittest.main()
