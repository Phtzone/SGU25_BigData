# Report Notes

## MVP scope

- 1 to 3 RSS news feeds
- 1 Kafka topic: `news_raw`
- 1 producer
- 1 consumer
- HDFS raw storage in JSON Lines

## Demo checklist

1. Show producer logs fetching RSS items.
2. Show Kafka topic creation and message flow.
3. Show HDFS directory structure under `/news/raw/YYYY/MM/DD/`.
4. Show the Airflow web UI and `news_pipeline` DAG task order.

## Suggested screenshots

- Docker containers running
- Kafka topic list
- HDFS NameNode UI
- Terminal logs for producer and consumer
- Airflow DAG graph view and task logs
