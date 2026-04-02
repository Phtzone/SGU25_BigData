import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/project")

default_args = {
    "owner": "codex",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}


with DAG(
    dag_id="news_pipeline",
    default_args=default_args,
    description="Fetch news from RSS and promote it across raw, processed, and curated HDFS zones.",
    start_date=datetime(2026, 3, 14),
    schedule="*/30 * * * *",
    catchup=False,
    tags=["big-data", "kafka", "hdfs", "news"],
) as dag:
    fetch_and_publish_articles = BashOperator(
        task_id="fetch_and_publish_articles",
        bash_command=f"cd {PROJECT_ROOT} && python -m producer.run_producer",
    )

    consume_kafka_to_raw_zone = BashOperator(
        task_id="consume_kafka_to_raw_zone",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m consumer.kafka_consumer_to_hdfs --max-messages 100"
        ),
    )

    transform_raw_to_processed_zone = BashOperator(
        task_id="transform_raw_to_processed_zone",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m Spark_jobs.transform_news_raw_to_processed "
            "--input-path /news/raw --output-path /news/processed"
        ),
    )

    validate_processed_zone = BashOperator(
        task_id="validate_processed_zone",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m scripts.validate_processed_output --path /news/processed"
        ),
    )

    curate_processed_to_curated_zone = BashOperator(
        task_id="curate_processed_to_curated_zone",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m Spark_jobs.curate_news_processed_to_curated "
            "--input-path /news/processed --output-path /news/curated"
        ),
    )

    validate_curated_zone = BashOperator(
        task_id="validate_curated_zone",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m scripts.validate_curated_output --path /news/curated"
        ),
    )

    fetch_and_publish_articles >> consume_kafka_to_raw_zone >> transform_raw_to_processed_zone
    transform_raw_to_processed_zone >> validate_processed_zone
    validate_processed_zone >> curate_processed_to_curated_zone >> validate_curated_zone
