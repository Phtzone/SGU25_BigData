#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "==> Starting infrastructure and Airflow services..."
docker compose up -d
docker compose --profile airflow up airflow-init --build
docker compose --profile airflow up -d airflow-webserver airflow-scheduler

echo "==> Ensuring Kafka topics exist..."
bash scripts/init_kafka_topics.sh

echo "==> Capturing latest known outputs before this demo run..."
before_raw_json="$(python -m scripts.validate_hdfs_output --path /news/raw --json 2>/dev/null || true)"
before_raw_latest="$(printf '%s' "$before_raw_json" | python -c "import json,sys; data=sys.stdin.read().strip(); print(json.loads(data)['latest_file'] if data else '')")"

before_batch_json="$(docker compose --profile airflow exec -T postgres-analytics psql -U analytics -d analytics -t -A -c 'SELECT batch_path FROM analytics_load_history ORDER BY loaded_at DESC LIMIT 1;' 2>/dev/null || true)"
before_batch_latest="$(printf '%s' "$before_batch_json" | tail -n 1 | tr -d '[:space:]')"

run_id="$(date -u +%Y%m%d%H%M%S)"
group_id="news-demo-${run_id}"

echo "==> Running pipeline: RSS -> Kafka -> HDFS raw -> Spark processed -> Spark curated..."
python -m producer.run_producer
python -m consumer.kafka_consumer_to_hdfs --max-messages 50 --group-id "$group_id"
python -m Spark_jobs.transform_news_raw_to_processed --input-path /news/raw --output-path /news/processed
python -m scripts.validate_processed_output --path /news/processed
python -m Spark_jobs.curate_news_processed_to_curated --input-path /news/processed --output-path /news/curated
python -m scripts.validate_curated_output --path /news/curated

echo "==> Loading latest curated batch into analytics PostgreSQL..."
python -m scripts.load_curated_to_postgres --input-path /news/curated

echo "==> Collecting demo outputs..."
after_raw_json="$(python -m scripts.validate_hdfs_output --path /news/raw --json)"
after_raw_latest="$(printf '%s' "$after_raw_json" | python -c "import json,sys; print(json.load(sys.stdin)['latest_file'])")"
processed_json="$(python -m scripts.validate_processed_output --path /news/processed --json)"
processed_latest="$(printf '%s' "$processed_json" | python -c "import json,sys; print(json.load(sys.stdin)['latest_batch'])")"
curated_json="$(python -m scripts.validate_curated_output --path /news/curated --json)"
curated_latest="$(printf '%s' "$curated_json" | python -c "import json,sys; print(json.load(sys.stdin)['latest_batch'])")"

after_batch_json="$(docker compose --profile airflow exec -T postgres-analytics psql -U analytics -d analytics -t -A -c 'SELECT batch_path FROM analytics_load_history ORDER BY loaded_at DESC LIMIT 1;')"
after_batch_latest="$(printf '%s' "$after_batch_json" | tail -n 1 | tr -d '[:space:]')"

ods_count="$(docker compose --profile airflow exec -T postgres-analytics psql -U analytics -d analytics -t -A -c 'SELECT COUNT(*) FROM ods_news_articles;')"
mart_count="$(docker compose --profile airflow exec -T postgres-analytics psql -U analytics -d analytics -t -A -c 'SELECT COUNT(*) FROM mart_news_daily_source;')"

if [[ -n "$before_raw_latest" && "$before_raw_latest" == "$after_raw_latest" ]]; then
  echo "Expected a new raw HDFS file, but latest file did not change." >&2
  exit 1
fi

if [[ -n "$before_batch_latest" && "$before_batch_latest" == "$after_batch_latest" ]]; then
  echo "Expected a new analytics loaded batch, but latest batch in analytics_load_history did not change." >&2
  exit 1
fi

echo
echo "Demo completed successfully."
echo "Consumer group: $group_id"
echo "New raw file: $after_raw_latest"
echo "Latest processed batch: $processed_latest"
echo "Latest curated batch: $curated_latest"
echo "Latest analytics loaded batch: $after_batch_latest"
echo "ODS row count: $(printf '%s' "$ods_count" | tr -d '[:space:]')"
echo "MART row count: $(printf '%s' "$mart_count" | tr -d '[:space:]')"
echo "Airflow UI: http://localhost:8080 (airflow/airflow)"
echo "PostgreSQL analytics: localhost:5433 (analytics/analytics)"
