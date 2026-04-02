# News Article Data Contract

## Raw event schema

Each article published to Kafka and stored in HDFS raw files uses the following schema:

- `title`: normalized non-empty article title
- `link`: normalized non-empty article URL
- `summary`: cleaned summary text, included for every article and may be empty
- `published_at`: UTC ISO 8601 timestamp
- `source`: normalized source label
- `fetched_at`: UTC ISO 8601 timestamp for ingestion time
- `ingestion_id`: unique identifier for the ingestion event

## Validation rules

- The raw event contract contains exactly the 7 fields listed above
- `title`, `link`, `source`, and `ingestion_id` are required non-empty strings
- `published_at` and `fetched_at` must be valid datetimes before Kafka publish and before HDFS raw writes

## Kafka contract

- Raw topic: `news_raw`
- Dead-letter topic: `news_dead_letter`
- Message key: normalized article `link`
- Invalid consumed messages are routed to the dead-letter topic with error details and payload context

## Storage zones

- Raw zone: `/news/raw/YYYY/MM/DD/news_HHMMSS.jsonl`
- Processed zone: `/news/processed/YYYY/MM/DD/news_HHMMSS/`
- Curated zone: `/news/curated/YYYY/MM/DD/news_HHMMSS/`

## Processed schema

The processed zone stores Spark-generated Parquet with:

- `title`, `link`, `summary`, `source`, `ingestion_id`
- `published_at`: Spark `timestamp`
- `fetched_at`: Spark `timestamp`
- `event_date`: Spark `date`

## Curated schema

The curated zone stores analytics-ready Parquet partitioned by:

- `event_date`
- `source`
