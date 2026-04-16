#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_pipeline_common.sh"

PYTHON_BIN="$(resolve_python_bin)"

artifact_dir="$(create_artifact_dir)"
trap 'rm -rf "$artifact_dir"' EXIT

raw_path_file="$artifact_dir/raw_path.txt"
processed_path_file="$artifact_dir/processed_path.txt"
curated_path_file="$artifact_dir/curated_path.txt"
keyword_path_file="$artifact_dir/keyword_path.txt"

bash scripts/init_kafka_topics.sh
wait_for_kafka_listener

before_json="$("$PYTHON_BIN" -m scripts.validate_hdfs_output --path /news/raw --json 2>/dev/null || true)"
before_latest="$(printf '%s' "$before_json" | extract_json_field latest_file)"

run_id="$(date -u +%Y%m%d%H%M%S)"

"$PYTHON_BIN" -m producer.run_producer
"$PYTHON_BIN" -m consumer.kafka_consumer_to_hdfs --max-messages 20 --group-id "news-hdfs-test-${run_id}" --write-output-path-file "$raw_path_file"
after_latest="$(read_output_path "$raw_path_file")"
"$PYTHON_BIN" -m scripts.validate_hdfs_output --path "$after_latest"

"$PYTHON_BIN" -m Spark_jobs.transform_news_raw_to_processed --input-batch-path "$after_latest" --output-path /news/processed --write-output-path-file "$processed_path_file"
processed_latest="$(read_output_path "$processed_path_file")"
"$PYTHON_BIN" -m scripts.validate_processed_output --path "$processed_latest"

"$PYTHON_BIN" -m Spark_jobs.curate_news_processed_to_curated --input-batch-path "$processed_latest" --output-path /news/curated --write-output-path-file "$curated_path_file"
curated_latest="$(read_output_path "$curated_path_file")"
"$PYTHON_BIN" -m scripts.validate_curated_output --path "$curated_latest"

"$PYTHON_BIN" -m Spark_jobs.extract_news_keywords --input-batch-path "$curated_latest" --output-path /news/keywords --write-output-path-file "$keyword_path_file"
keyword_latest="$(read_output_path "$keyword_path_file")"
"$PYTHON_BIN" -m scripts.validate_keyword_output --path "$keyword_latest"

if [[ -n "$before_latest" && "$before_latest" == "$after_latest" ]]; then
  echo "Expected a new HDFS file, but the latest file did not change." >&2
  exit 1
fi

echo "Pipeline test passed."
echo "New HDFS file: $after_latest"
echo "Latest processed batch: $processed_latest"
echo "Latest curated batch: $curated_latest"
echo "Latest keyword batch: $keyword_latest"
