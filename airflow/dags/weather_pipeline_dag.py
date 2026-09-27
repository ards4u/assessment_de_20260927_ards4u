"""Daily weather pipeline: extract/load -> dbt run -> dbt test.

Every task is idempotent for a given logical date (`ds`): re-running a task,
or backfilling with `airflow dags backfill`, upserts in place rather than
duplicating rows or duplicating dbt model output.
"""
from __future__ import annotations
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Repo root as mounted into the Airflow container/image.
# your actual deployment (or replace with `pip install -e .` on the image
# and drop the sys.path hack entirely).
REPO_ROOT = "/opt/airflow/dags/repo"
sys.path.insert(0, REPO_ROOT)

from extract_load.pipeline import run as run_extract_load  # noqa: E402

DBT_PROJECT_DIR = f"{REPO_ROOT}/dbt/weather_pipeline"
DBT_PROFILES_DIR = "/opt/airflow/dbt"  # directory containing profiles.yml

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


def _extract_and_load(logical_date: str, **_context) -> None:
    n = run_extract_load(logical_date)
    print(f"Upserted {n} rows for {logical_date}")


with DAG(
    dag_id="daily_weather_pipeline",
    description="Open-Meteo -> Postgres -> dbt, one logical day at a time.",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=True,        # `airflow dags backfill` can safely fill history
    max_active_runs=1,   # keep same-DB runs from racing each other
    tags=["weather", "take-home"],
) as dag:

    extract_and_load = PythonOperator(
        task_id="extract_and_load",
        python_callable=_extract_and_load,
        op_kwargs={"logical_date": "{{ ds }}"},
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

