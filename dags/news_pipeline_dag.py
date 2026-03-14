import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/project")

default_args = {
    "owner": "codex",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="news_pipeline",
    default_args=default_args,
    description="Fetch news from RSS, publish to Kafka, and store batches in HDFS.",
    start_date=datetime(2026, 3, 14),
    schedule="*/30 * * * *",
    catchup=False,
    tags=["big-data", "kafka", "hdfs", "news"],
) as dag:
    publish_to_kafka = BashOperator(
        task_id="publish_to_kafka",
        bash_command=f"cd {PROJECT_ROOT} && python -m producer.run_producer",
    )

    consume_and_store_hdfs = BashOperator(
        task_id="consume_and_store_hdfs",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m consumer.kafka_consumer_to_hdfs --max-messages 100"
        ),
    )

    validate_hdfs_output = BashOperator(
        task_id="validate_hdfs_output",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python scripts/validate_hdfs_output.py --path /news/raw"
        ),
    )

    publish_to_kafka >> consume_and_store_hdfs >> validate_hdfs_output
