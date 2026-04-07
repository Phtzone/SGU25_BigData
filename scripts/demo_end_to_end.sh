#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
else
  echo "Python interpreter not found. Install python3 or activate your virtual environment first." >&2
  exit 1
fi

artifact_dir="$(mktemp -d)"
trap 'rm -rf "$artifact_dir"' EXIT

raw_path_file="$artifact_dir/raw_path.txt"
processed_path_file="$artifact_dir/processed_path.txt"
curated_path_file="$artifact_dir/curated_path.txt"
keyword_path_file="$artifact_dir/keyword_path.txt"

extract_json_field() {
  local field="$1"
  local required="${2:-0}"

  "$PYTHON_BIN" - "$field" "$required" <<'PY'
import json
import sys

field = sys.argv[1]
required = sys.argv[2] == "1"
text = sys.stdin.read()
decoder = json.JSONDecoder()
value_found = False
value = None
index = 0

while index < len(text):
    if text[index] not in "{[":
        index += 1
        continue
    try:
        payload, end = decoder.raw_decode(text, index)
    except json.JSONDecodeError:
        index += 1
        continue
    if isinstance(payload, dict) and field in payload:
        value = payload[field]
        value_found = True
    index = end

if not value_found:
    if required:
        raise SystemExit(f"Could not find JSON field {field!r} in command output")
    raise SystemExit(0)

if value is not None:
    print(value)
PY
}

read_output_path() {
  tr -d '\r\n' < "$1"
}

wait_for_kafka_listener() {
  "$PYTHON_BIN" - <<'PY'
import os
import socket
import sys
import time

host = os.getenv("KAFKA_WAIT_HOST", "localhost")
port = int(os.getenv("KAFKA_WAIT_PORT", "9093"))
timeout_seconds = float(os.getenv("KAFKA_STARTUP_TIMEOUT_SECONDS", "90"))
check_interval = float(os.getenv("KAFKA_STARTUP_CHECK_INTERVAL_SECONDS", "3"))
deadline = time.monotonic() + max(timeout_seconds, 0.0)
last_error = None

while time.monotonic() <= deadline:
    try:
        with socket.create_connection((host, port), timeout=5):
            print(f"Kafka listener is ready at {host}:{port}")
            raise SystemExit(0)
    except OSError as exc:
        last_error = exc
        time.sleep(max(check_interval, 0.1))

message = f"Timed out waiting for Kafka listener at {host}:{port}"
if last_error is not None:
    message += f" ({last_error})"
raise SystemExit(message)
PY
}

echo "==> Starting infrastructure and Airflow services..."
docker compose up -d
docker compose --profile airflow up airflow-init --build
docker compose --profile airflow up -d airflow-webserver airflow-scheduler

echo "==> Ensuring Kafka topics exist..."
bash scripts/init_kafka_topics.sh

echo "==> Waiting for Kafka external listener..."
wait_for_kafka_listener

echo "==> Capturing latest known outputs before this demo run..."
before_raw_json="$("$PYTHON_BIN" -m scripts.validate_hdfs_output --path /news/raw --json 2>/dev/null || true)"
before_raw_latest="$(printf '%s' "$before_raw_json" | extract_json_field latest_file)"

before_batch_json="$(docker compose --profile airflow exec -T postgres-analytics psql -U analytics -d analytics -t -A -c 'SELECT batch_path FROM analytics_load_history ORDER BY loaded_at DESC LIMIT 1;' 2>/dev/null || true)"
before_batch_latest="$(printf '%s' "$before_batch_json" | tail -n 1 | tr -d '[:space:]')"

before_keyword_batch_json="$(docker compose --profile airflow exec -T postgres-analytics psql -U analytics -d analytics -t -A -c 'SELECT batch_path FROM analytics_keyword_load_history ORDER BY loaded_at DESC LIMIT 1;' 2>/dev/null || true)"
before_keyword_batch_latest="$(printf '%s' "$before_keyword_batch_json" | tail -n 1 | tr -d '[:space:]')"

run_id="$(date -u +%Y%m%d%H%M%S)"
group_id="news-demo-${run_id}"

echo "==> Running pipeline: RSS -> Kafka -> HDFS raw -> Spark processed -> Spark curated -> Spark keywords..."
"$PYTHON_BIN" -m producer.run_producer
"$PYTHON_BIN" -m consumer.kafka_consumer_to_hdfs --max-messages 50 --group-id "$group_id" --write-output-path-file "$raw_path_file"
after_raw_latest="$(read_output_path "$raw_path_file")"
"$PYTHON_BIN" -m scripts.validate_hdfs_output --path "$after_raw_latest"

"$PYTHON_BIN" -m Spark_jobs.transform_news_raw_to_processed --input-batch-path "$after_raw_latest" --output-path /news/processed --write-output-path-file "$processed_path_file"
processed_latest="$(read_output_path "$processed_path_file")"
"$PYTHON_BIN" -m scripts.validate_processed_output --path "$processed_latest"

"$PYTHON_BIN" -m Spark_jobs.curate_news_processed_to_curated --input-batch-path "$processed_latest" --output-path /news/curated --write-output-path-file "$curated_path_file"
curated_latest="$(read_output_path "$curated_path_file")"
"$PYTHON_BIN" -m scripts.validate_curated_output --path "$curated_latest"

echo "==> Loading latest curated batch into analytics PostgreSQL..."
"$PYTHON_BIN" -m scripts.load_curated_to_postgres --input-batch-path "$curated_latest"

echo "==> Extracting and loading keyword analytics..."
"$PYTHON_BIN" -m Spark_jobs.extract_news_keywords --input-batch-path "$curated_latest" --output-path /news/keywords --write-output-path-file "$keyword_path_file"
keyword_latest="$(read_output_path "$keyword_path_file")"
"$PYTHON_BIN" -m scripts.validate_keyword_output --path "$keyword_latest"
"$PYTHON_BIN" -m scripts.load_keywords_to_postgres --input-batch-path "$keyword_latest"

echo "==> Collecting demo outputs..."
after_batch_json="$(docker compose --profile airflow exec -T postgres-analytics psql -U analytics -d analytics -t -A -c 'SELECT batch_path FROM analytics_load_history ORDER BY loaded_at DESC LIMIT 1;')"
after_batch_latest="$(printf '%s' "$after_batch_json" | tail -n 1 | tr -d '[:space:]')"

after_keyword_batch_json="$(docker compose --profile airflow exec -T postgres-analytics psql -U analytics -d analytics -t -A -c 'SELECT batch_path FROM analytics_keyword_load_history ORDER BY loaded_at DESC LIMIT 1;')"
after_keyword_batch_latest="$(printf '%s' "$after_keyword_batch_json" | tail -n 1 | tr -d '[:space:]')"

ods_count="$(docker compose --profile airflow exec -T postgres-analytics psql -U analytics -d analytics -t -A -c 'SELECT COUNT(*) FROM ods_news_articles;')"
mart_count="$(docker compose --profile airflow exec -T postgres-analytics psql -U analytics -d analytics -t -A -c 'SELECT COUNT(*) FROM mart_news_daily_source;')"
article_keywords_count="$(docker compose --profile airflow exec -T postgres-analytics psql -U analytics -d analytics -t -A -c 'SELECT COUNT(*) FROM mart_article_keywords;')"
keyword_daily_source_count="$(docker compose --profile airflow exec -T postgres-analytics psql -U analytics -d analytics -t -A -c 'SELECT COUNT(*) FROM mart_keyword_daily_source;')"

if [[ -n "$before_raw_latest" && "$before_raw_latest" == "$after_raw_latest" ]]; then
  echo "Expected a new raw HDFS file, but latest file did not change." >&2
  exit 1
fi

if [[ -n "$before_batch_latest" && "$before_batch_latest" == "$after_batch_latest" ]]; then
  echo "Expected a new analytics loaded batch, but latest batch in analytics_load_history did not change." >&2
  exit 1
fi

if [[ -n "$before_keyword_batch_latest" && "$before_keyword_batch_latest" == "$after_keyword_batch_latest" ]]; then
  echo "Expected a new keyword analytics loaded batch, but latest batch in analytics_keyword_load_history did not change." >&2
  exit 1
fi

echo
echo "Demo completed successfully."
echo "Consumer group: $group_id"
echo "New raw file: $after_raw_latest"
echo "Latest processed batch: $processed_latest"
echo "Latest curated batch: $curated_latest"
echo "Latest analytics loaded batch: $after_batch_latest"
echo "Latest keyword batch: $keyword_latest"
echo "Latest keyword analytics loaded batch: $after_keyword_batch_latest"
echo "ODS row count: $(printf '%s' "$ods_count" | tr -d '[:space:]')"
echo "MART row count: $(printf '%s' "$mart_count" | tr -d '[:space:]')"
echo "Article keyword row count: $(printf '%s' "$article_keywords_count" | tr -d '[:space:]')"
echo "Keyword daily source row count: $(printf '%s' "$keyword_daily_source_count" | tr -d '[:space:]')"
echo "Airflow UI: http://localhost:8080 (airflow/airflow)"
echo "PostgreSQL analytics: localhost:5433 (analytics/analytics)"
