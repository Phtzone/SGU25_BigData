#!/usr/bin/env bash
set -euo pipefail

docker compose exec kafka kafka-topics \
  --create \
  --if-not-exists \
  --bootstrap-server kafka:29092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic news_raw

docker compose exec kafka kafka-topics \
  --create \
  --if-not-exists \
  --bootstrap-server kafka:29092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic news_dead_letter
