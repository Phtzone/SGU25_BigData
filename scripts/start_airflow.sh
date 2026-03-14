#!/usr/bin/env bash
set -euo pipefail

docker compose up -d
docker compose --profile airflow up airflow-init --build
docker compose --profile airflow up -d airflow-webserver airflow-scheduler

echo "Airflow UI: http://localhost:8080"
echo "Username: airflow"
echo "Password: airflow"
