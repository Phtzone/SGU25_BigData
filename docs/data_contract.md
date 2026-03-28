# News Article Data Contract

## Raw event schema

Each article published to Kafka and stored in HDFS raw files uses the following schema:

- `title`: normalized non-empty article title
- `link`: normalized non-empty article URL
- `summary`: cleaned summary text, may be empty
- `published_at`: UTC ISO 8601 timestamp when the feed timestamp is parseable
- `published_at_raw`: original published timestamp string from RSS
- `source`: normalized source label
- `fetched_at`: UTC ISO 8601 timestamp for ingestion time
- `ingestion_id`: unique identifier for the ingestion event

## Validation rules

- `title`, `link`, and `source` are required
- `fetched_at` must be a valid datetime
- `published_at` is optional and may be empty when the RSS timestamp cannot be normalized

## Storage zones

- Raw zone: `/news/raw/YYYY/MM/DD/news_HHMMSS.jsonl`
- Processed zone: `/news/processed/YYYY/MM/DD/news_HHMMSS/`

The processed zone stores Spark-generated Parquet files and a `_SUCCESS` marker for each batch.
