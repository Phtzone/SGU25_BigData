from __future__ import annotations

from collections.abc import Callable, Mapping

SECRET_KEY_ALIASES: dict[str, tuple[tuple[str, str], ...]] = {
    "ANALYTICS_DB_HOST": (("analytics_db", "host"), ("analytics_db", "analytics_db_host")),
    "ANALYTICS_DB_PORT": (("analytics_db", "port"), ("analytics_db", "analytics_db_port")),
    "ANALYTICS_DB_NAME": (("analytics_db", "dbname"), ("analytics_db", "analytics_db_name")),
    "ANALYTICS_DB_USER": (("analytics_db", "user"), ("analytics_db", "analytics_db_user")),
    "ANALYTICS_DB_PASSWORD": (("analytics_db", "password"), ("analytics_db", "analytics_db_password")),
    "AIRFLOW_API_URL": (("airflow", "api_url"), ("airflow", "base_url"), ("airflow", "airflow_api_url")),
    "AIRFLOW_USERNAME": (("airflow", "username"), ("airflow", "airflow_username")),
    "AIRFLOW_PASSWORD": (("airflow", "password"), ("airflow", "airflow_password")),
    "APP_TIMEZONE": (("app", "timezone"), ("app", "app_timezone")),
}


def _get_mapping_value(mapping: Mapping[str, object], key: str) -> object | None:
    if key in mapping:
        return mapping[key]
    return None


def resolve_config_value(
    name: str,
    default: str,
    *,
    env_resolver: Callable[[str, str], str],
    secrets: Mapping[str, object] | None = None,
) -> str:
    if secrets is not None:
        for section_name, secret_key in SECRET_KEY_ALIASES.get(name, ()):
            section = secrets.get(section_name)
            if isinstance(section, Mapping):
                value = _get_mapping_value(section, secret_key)
                if value is not None and str(value).strip():
                    return str(value)

    return env_resolver(name, default)


def build_airflow_config(resolve_value: Callable[[str, str], str]) -> dict[str, str]:
    return {
        "base_url": resolve_value("AIRFLOW_API_URL", ""),
        "username": resolve_value("AIRFLOW_USERNAME", ""),
        "password": resolve_value("AIRFLOW_PASSWORD", ""),
    }
