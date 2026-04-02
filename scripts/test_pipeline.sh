#!/usr/bin/env bash
set -euo pipefail

bash scripts/init_kafka_topics.sh

before_json="$(python -m scripts.validate_hdfs_output --path /news/raw --json 2>/dev/null || true)"
before_latest="$(printf '%s' "$before_json" | python -c "import json,sys; data=sys.stdin.read().strip(); print(json.loads(data)['latest_file'] if data else '')")"

run_id="$(date -u +%Y%m%d%H%M%S)"

python -m producer.run_producer
python -m consumer.kafka_consumer_to_hdfs --max-messages 20 --group-id "news-hdfs-test-${run_id}"
python -m Spark_jobs.transform_news_raw_to_processed --input-path /news/raw --output-path /news/processed
python -m Spark_jobs.curate_news_processed_to_curated --input-path /news/processed --output-path /news/curated

after_json="$(python -m scripts.validate_hdfs_output --path /news/raw --json)"
after_latest="$(printf '%s' "$after_json" | python -c "import json,sys; print(json.load(sys.stdin)['latest_file'])")"
processed_json="$(python -m scripts.validate_processed_output --path /news/processed --json)"
processed_latest="$(printf '%s' "$processed_json" | python -c "import json,sys; print(json.load(sys.stdin)['latest_batch'])")"
curated_json="$(python -m scripts.validate_curated_output --path /news/curated --json)"
curated_latest="$(printf '%s' "$curated_json" | python -c "import json,sys; print(json.load(sys.stdin)['latest_batch'])")"

if [[ -n "$before_latest" && "$before_latest" == "$after_latest" ]]; then
  echo "Expected a new HDFS file, but the latest file did not change." >&2
  exit 1
fi

echo "Pipeline test passed."
echo "New HDFS file: $after_latest"
echo "Latest processed batch: $processed_latest"
echo "Latest curated batch: $curated_latest"
