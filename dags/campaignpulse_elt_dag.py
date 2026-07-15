"""
CampaignPulse ELT pipeline.

Extracts a full year of session level data from the Google Merchandise
Store sample dataset (Universal Analytics export), lands it in a Bronze
dataset, then hands off to dbt for Silver and Gold transformations.
Includes a synthetic ad spend generation step used downstream for CAC
and ROAS calculations.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup

import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BRONZE_DATASET = os.environ.get("BQ_DATASET_BRONZE", "campaignpulse_bronze")
DBT_DIR = "/opt/airflow/dbt"
LOOKBACK_DAYS = 365


def alert_on_failure(context):
    """
    Failure callback placeholder.
    Wire this to Slack webhook or SMTP in production.
    Kept as a log statement here so the DAG runs without external secrets.
    """
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    print(
        f"ALERT: Task {task_instance.task_id} failed in DAG "
        f"{dag_run.dag_id} at {dag_run.execution_date}. "
        f"Replace alert_on_failure with a real Slack or email hook."
    )


default_args = {
    "owner": "krithika",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": alert_on_failure,
    "sla": timedelta(hours=1),
}

with DAG(
    dag_id="campaignpulse_elt",
    description="GA4 marketing analytics ELT pipeline with dbt on BigQuery",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["campaignpulse", "marketing", "bigquery", "dbt"],
) as dag:

    with TaskGroup("extract_bronze") as extract_bronze:

        load_ga4_events = BigQueryInsertJobOperator(
            task_id="load_ga4_events_to_bronze",
            project_id=PROJECT_ID,
            configuration={
                "query": {
                    "query": "{% include 'sql/load_ga_sessions_bronze.sql' %}",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": BRONZE_DATASET,
                        "tableId": "ga_sessions_raw",
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                    "timePartitioning": {"type": "DAY", "field": "event_date"},
                    "maximumBytesBilled": "8000000000",
                }
            },
            location="US",
        )

        generate_synthetic_spend = BashOperator(
            task_id="generate_synthetic_spend",
            bash_command=(
                "python /opt/airflow/scripts/synthetic_spend_generator.py "
                f"--project {PROJECT_ID} --dataset {BRONZE_DATASET} "
                "--lookback-days {{ params.lookback_days }}"
            ),
            params={"lookback_days": LOOKBACK_DAYS},
        )

        load_ga4_events >> generate_synthetic_spend

    with TaskGroup("transform_dbt") as transform_dbt:

        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir {DBT_DIR}",
        )

        dbt_seed = BashOperator(
            task_id="dbt_seed",
            bash_command=f"cd {DBT_DIR} && dbt seed --profiles-dir {DBT_DIR}",
        )

        dbt_snapshot = BashOperator(
            task_id="dbt_snapshot",
            bash_command=f"cd {DBT_DIR} && dbt snapshot --profiles-dir {DBT_DIR}",
        )

        dbt_run_silver = BashOperator(
            task_id="dbt_run_silver",
            bash_command=(
                f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR} "
                "--select staging intermediate"
            ),
        )

        dbt_test_silver = BashOperator(
            task_id="dbt_test_silver",
            bash_command=(
                f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR} "
                "--select staging intermediate"
            ),
        )

        dbt_run_gold = BashOperator(
            task_id="dbt_run_gold",
            bash_command=(
                f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR} --select marts"
            ),
        )

        dbt_test_gold = BashOperator(
            task_id="dbt_test_gold",
            bash_command=(
                f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR} --select marts"
            ),
        )

        dbt_docs_generate = BashOperator(
            task_id="dbt_docs_generate",
            bash_command=f"cd {DBT_DIR} && dbt docs generate --profiles-dir {DBT_DIR}",
        )

        dbt_deps >> dbt_seed >> dbt_snapshot >> dbt_run_silver >> dbt_test_silver
        dbt_test_silver >> dbt_run_gold >> dbt_test_gold >> dbt_docs_generate

    extract_bronze >> transform_dbt