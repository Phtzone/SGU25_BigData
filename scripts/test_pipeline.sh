#!/usr/bin/env bash
set -euo pipefail

bash scripts/init_kafka_topics.sh

before_json="$(python scripts/validate_hdfs_output.py --path /news/raw --json 2>/dev/null || true)"
before_latest="$(printf '%s' "$before_json" | python -c "import json,sys; data=sys.stdin.read().strip(); print(json.loads(data)['latest_file'] if data else '')")"

run_id="$(date -u +%Y%m%d%H%M%S)"

python -m producer.run_producer
python -m consumer.kafka_consumer_to_hdfs --max-messages 20 --group-id "news-hdfs-test-${run_id}"

after_json="$(python scripts/validate_hdfs_output.py --path /news/raw --json)"
after_latest="$(printf '%s' "$after_json" | python -c "import json,sys; print(json.load(sys.stdin)['latest_file'])")"

if [[ -n "$before_latest" && "$before_latest" == "$after_latest" ]]; then
  echo "Expected a new HDFS file, but the latest file did not change." >&2
  exit 1
fi

echo "Pipeline test passed."
echo "New HDFS file: $after_latest"
