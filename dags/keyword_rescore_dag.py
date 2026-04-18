import os
import shlex
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/project")
AIRFLOW_RUN_DIR = os.getenv("AIRFLOW_RUN_DIR", "/tmp")
DAG_RUN_ARTIFACT_DIR = f"{AIRFLOW_RUN_DIR.rstrip('/')}/keyword_rescore/{{{{ run_id }}}}"

default_args = {
    "owner": "TTT",
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
    dag_id="keyword_rescore_pipeline",
    default_args=default_args,
    description="Re-score keywords from the latest curated batch and refresh keyword marts.",
    start_date=datetime(2026, 4, 17),
    schedule=None,
    is_paused_upon_creation=False,
    catchup=False,
    tags=["big-data", "keywords", "rescoring"],
) as dag:
    extract_keywords_from_curated_zone = BashOperator(
        task_id="extract_keywords_from_curated_zone",
        bash_command=run_project_command(
            "python -m Spark_jobs.extract_news_keywords "
            "--input-path /news/curated "
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
            f'python -m scripts.load_keywords_to_postgres --force-reload --input-batch-path "$(cat {shlex.quote(artifact_path("keyword_path.txt"))})"'
        ),
    )

    extract_keywords_from_curated_zone >> validate_keyword_zone >> load_keywords_to_analytics_db
