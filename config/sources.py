import json
from pathlib import Path
from typing import Any


def load_rss_sources() -> list[dict[str, Any]]:
    config_path = Path(__file__).with_name("sources.json")
    with config_path.open("r", encoding="utf-8") as config_file:
        return json.load(config_file)


RSS_SOURCES = load_rss_sources()
