import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, "/opt/airflow/scripts")

GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "jkia-flight-data-dev")
BQ_PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "news-sentiment-pipeline")
BQ_RAW_DATASET = os.environ.get("BQ_RAW_DATASET", "jkia_raw")

DEFAULT_ARGS = {
    "owner": "jkia-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def _backfill_partition(**context):
    from bq_loader import load_gcs_parquet_to_bigquery
    execution_date = context["ds_nodash"]
    result = load_gcs_parquet_to_bigquery(
        bucket_name=GCS_BUCKET_NAME,
        project_id=BQ_PROJECT_ID,
        dataset_id=BQ_RAW_DATASET,
        partition_date=execution_date,
    )
    return result

def _run_dbt_full(**context):
    from bq_loader import run_dbt_models
    return_code = run_dbt_models(
        dbt_project_dir="/opt/airflow/dbt",
        profiles_dir="/opt/airflow/dbt",
        select="staging+ marts",
        command="build"
    )
    if return_code != 0:
        raise RuntimeError(f"dbt build failed with return code {return_code}")

with DAG(
    dag_id="jkia_backfill_pipeline",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=True,
    default_args=DEFAULT_ARGS,
    max_active_runs=10, 
    tags=["jkia", "backfill"],
) as dag:

    load_partition = PythonOperator(
        task_id="load_partition_to_bq",
        python_callable=_backfill_partition,
    )

    dbt_refresh = PythonOperator(
        task_id="run_dbt_full_refresh",
        python_callable=_run_dbt_full,
    )

    load_partition >> dbt_refresh
