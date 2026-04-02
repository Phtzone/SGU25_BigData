# Report Notes

## MVP scope

- 1 to 3 RSS news feeds
- 1 Kafka topic: `news_raw`
- 1 producer
- 1 consumer
- HDFS raw storage in JSON Lines
- Spark transform from raw JSONL to processed Parquet
- Spark curation from processed Parquet to curated analytics-ready Parquet
- Structured logging with stable operational fields
- Data quality metrics and Kafka dead-letter handling

## Demo checklist

1. Show producer logs fetching RSS items.
2. Show Kafka topic creation and message flow.
3. Show HDFS directory structure under `/news/raw/YYYY/MM/DD/`.
4. Show processed Parquet batches under `/news/processed/YYYY/MM/DD/`.
5. Show curated Parquet batches under `/news/curated/YYYY/MM/DD/`.
6. Show JSON logs with row counts, output paths, and durations.
7. Show the `news_dead_letter` topic if invalid messages are produced.
8. Show the Airflow web UI and `news_pipeline` DAG task order.

## Suggested screenshots

- Docker containers running
- Kafka topic list
- HDFS NameNode UI
- Terminal logs for producer and consumer
- Terminal logs for Spark transform
- Terminal logs for Spark curated job
- Structured JSON log examples with quality metrics
- Airflow DAG graph view and task logs
