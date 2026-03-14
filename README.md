# Real-Time News Ingestion Pipeline

This repository is organized around the Big Data MVP:

`RSS -> Kafka -> HDFS`

Airflow orchestration is implemented as an optional Docker Compose profile on top of the working MVP stack.

## Target Environment

Use `WSL/Linux + Docker Desktop`.

- Run Python from WSL or a Linux shell.
- Run infrastructure with `docker compose`.
- Keep one Linux virtual environment for the app code.

## Project Layout

```text
.
|- docker-compose.yml
|- requirements.txt
|- requirements-airflow.txt
|- producer/
|- consumer/
|- config/
|- dags/
|- scripts/
|- data/
`- docs/
```

## Services in Docker Compose

The core stack includes:

- `zookeeper`
- `kafka`
- `namenode`
- `datanode`

The optional `airflow` profile adds:

- `postgres`
- `airflow-init`
- `airflow-webserver`
- `airflow-scheduler`

Exposed ports:

- Kafka external listener: `localhost:9093`
- Kafka internal listener: `kafka:29092`
- NameNode UI: `localhost:9870`
- NameNode RPC: `localhost:9000`
- DataNode UI: `localhost:9864`
- Airflow UI: `localhost:8080` when the `airflow` profile is enabled

## Setup in WSL/Linux

Create and activate a virtual environment:

```bash
python3 -m venv ~/venvs/sgu25_bigdata
source ~/venvs/sgu25_bigdata/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Using a venv inside `/mnt/d/...` can fail on WSL because `ensurepip` is unreliable on mounted Windows paths. A Linux-home venv such as `~/venvs/sgu25_bigdata` is the recommended setup for this repo.

Start the core infrastructure:

```bash
docker compose up -d
```

If you previously ran an older Kafka/ZooKeeper stack on `9092` or `2181`, stop it first or keep it isolated. This compose file now exposes Kafka on `9093` and does not publish ZooKeeper to the host.

When you enable Airflow in Docker, allocate at least 4 GB of Docker memory. The official Airflow Docker guide warns that lower memory often causes unstable startup.

Create the Kafka topic:

```bash
bash scripts/init_kafka_topics.sh
```

## Run the Core Pipeline

Publish RSS items into Kafka:

```bash
python -m producer.run_producer
```

Consume Kafka messages and write them to HDFS:

```bash
python -m consumer.kafka_consumer_to_hdfs --max-messages 50
```

When the consumer runs from WSL/local and `HDFS_URL` points to `localhost`, it automatically rewrites WebHDFS redirects back to `localhost` so it can upload to the exposed DataNode port.

Validate HDFS output:

```bash
python scripts/validate_hdfs_output.py --path /news/raw
```

Run the full MVP smoke test:

```bash
bash scripts/test_pipeline.sh
```

Preview the latest HDFS file in a readable format:

```bash
python scripts/preview_hdfs_data.py --path /news/raw --limit 5
```

You can also preview a specific file:

```bash
python scripts/preview_hdfs_data.py --path /news/raw/2026/03/14/news_145545.jsonl --limit 3
```

## HDFS Output Layout

The consumer writes files under:

```text
/news/raw/YYYY/MM/DD/news_HHMMSS.jsonl
```

## Airflow Phase

Airflow now runs as an optional Docker Compose profile on top of the working MVP stack.

Files involved:

- `Dockerfile.airflow`
- `dags/news_pipeline_dag.py`
- `requirements-airflow.txt`
- `scripts/start_airflow.sh`

Start Airflow:

```bash
bash scripts/start_airflow.sh
```

Or run the steps manually:

```bash
docker compose up -d
docker compose --profile airflow up airflow-init --build
docker compose --profile airflow up -d airflow-webserver airflow-scheduler
```

Open the UI at `http://localhost:8080`.

Default login:

- username: `airflow`
- password: `airflow`

The DAG runs the same commands you already verified manually, but inside Docker using internal service names:

- Kafka: `kafka:29092`
- HDFS NameNode: `namenode:9870`
- WebHDFS redirect host: `datanode`
