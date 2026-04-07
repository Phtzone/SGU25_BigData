import os
import shlex
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/project")
AIRFLOW_RUN_DIR = os.getenv("AIRFLOW_RUN_DIR", "/tmp")
DAG_RUN_ARTIFACT_DIR = f"{AIRFLOW_RUN_DIR.rstrip('/')}/news_pipeline/{{{{ run_id }}}}"

default_args = {
    "owner": "codex",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}


def run_project_command(command: str) -> str:
    return " && ".join(
        [
            f"mkdir -p {shlex.quote(DAG_RUN_ARTIFACT_DIR)}",
            f"cd {shlex.quote(AIRFLOW_RUN_DIR)}",
            f"PYTHONPATH={PROJECT_ROOT}:$PYTHONPATH {command}",
        ]
    )


def artifact_path(filename: str) -> str:
    return f"{DAG_RUN_ARTIFACT_DIR}/{filename}"


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
        bash_command=run_project_command("python -m producer.run_producer"),
    )

    consume_kafka_to_raw_zone = BashOperator(
        task_id="consume_kafka_to_raw_zone",
        bash_command=run_project_command(
            "python -m consumer.kafka_consumer_to_hdfs "
            "--max-messages 100 "
            "--auto-offset-reset latest "
            f"--write-output-path-file {shlex.quote(artifact_path('raw_path.txt'))}"
        ),
    )

    transform_raw_to_processed_zone = BashOperator(
        task_id="transform_raw_to_processed_zone",
        bash_command=run_project_command(
            "python -m Spark_jobs.transform_news_raw_to_processed "
            f'--input-batch-path "$(cat {shlex.quote(artifact_path("raw_path.txt"))})" '
            "--output-path /news/processed "
            f"--write-output-path-file {shlex.quote(artifact_path('processed_path.txt'))}"
        ),
    )

    validate_processed_zone = BashOperator(
        task_id="validate_processed_zone",
        bash_command=run_project_command(
            f'python -m scripts.validate_processed_output --path "$(cat {shlex.quote(artifact_path("processed_path.txt"))})"'
        ),
    )

    curate_processed_to_curated_zone = BashOperator(
        task_id="curate_processed_to_curated_zone",
        bash_command=run_project_command(
            "python -m Spark_jobs.curate_news_processed_to_curated "
            f'--input-batch-path "$(cat {shlex.quote(artifact_path("processed_path.txt"))})" '
            "--output-path /news/curated "
            f"--write-output-path-file {shlex.quote(artifact_path('curated_path.txt'))}"
        ),
    )

    validate_curated_zone = BashOperator(
        task_id="validate_curated_zone",
        bash_command=run_project_command(
            f'python -m scripts.validate_curated_output --path "$(cat {shlex.quote(artifact_path("curated_path.txt"))})"'
        ),
    )

    load_curated_to_analytics_db = BashOperator(
        task_id="load_curated_to_analytics_db",
        bash_command=run_project_command(
            f'python -m scripts.load_curated_to_postgres --input-batch-path "$(cat {shlex.quote(artifact_path("curated_path.txt"))})"'
        ),
    )

    extract_keywords_from_curated_zone = BashOperator(
        task_id="extract_keywords_from_curated_zone",
        bash_command=run_project_command(
            "python -m Spark_jobs.extract_news_keywords "
            f'--input-batch-path "$(cat {shlex.quote(artifact_path("curated_path.txt"))})" '
            "--output-path /news/keywords "
            f"--write-output-path-file {shlex.quote(artifact_path('keyword_path.txt'))}"
        ),
    )

    validate_keyword_zone = BashOperator(
        task_id="validate_keyword_zone",
        bash_command=run_project_command(
            f'python -m scripts.validate_keyword_output --path "$(cat {shlex.quote(artifact_path("keyword_path.txt"))})"'
        ),
    )

    load_keywords_to_analytics_db = BashOperator(
        task_id="load_keywords_to_analytics_db",
        bash_command=run_project_command(
            f'python -m scripts.load_keywords_to_postgres --input-batch-path "$(cat {shlex.quote(artifact_path("keyword_path.txt"))})"'
        ),
    )

    fetch_and_publish_articles >> consume_kafka_to_raw_zone >> transform_raw_to_processed_zone
    transform_raw_to_processed_zone >> validate_processed_zone
    validate_processed_zone >> curate_processed_to_curated_zone >> validate_curated_zone
    validate_curated_zone >> load_curated_to_analytics_db
    validate_curated_zone >> extract_keywords_from_curated_zone
    extract_keywords_from_curated_zone >> validate_keyword_zone >> load_keywords_to_analytics_db
