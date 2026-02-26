import logging
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/scripts")

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "jkia-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
}

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "jkia-flight-data-dev")
BQ_PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "news-sentiment-pipeline")
BQ_RAW_DATASET = os.environ.get("BQ_RAW_DATASET", "jkia_raw")
OPENSKY_CREDENTIALS_PATH = os.environ.get(
    "OPENSKY_CREDENTIALS_PATH", "/opt/airflow/secrets/credentials.json"
)

DBT_PROJECT_DIR = "/opt/airflow/dbt"

def _poll_and_produce(**context):
    from opensky_client import poll_with_retry
    from kafka_producer import produce_batch
    records, poll_timestamp = poll_with_retry(OPENSKY_CREDENTIALS_PATH)
    if not records:
        logger.info("No aircraft states returned from OpenSky — zero traffic or API issue")
        context["task_instance"].xcom_push(key="records_produced", value=0)
        return
    result = produce_batch(
        records=records,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )
    context["task_instance"].xcom_push(key="records_produced", value=result["success_count"])
    context["task_instance"].xcom_push(key="poll_timestamp", value=poll_timestamp)

def _consume_to_gcs(**context):
    from kafka_consumer import consume_and_sink
    result = consume_and_sink(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        bucket_name=GCS_BUCKET_NAME,
        batch_size=50,
        poll_timeout_ms=8000,
        max_idle_cycles=6,
    )
    context["task_instance"].xcom_push(key="records_written", value=result["records_written"])

def _load_to_bigquery(**context):
    from bq_loader import load_gcs_parquet_to_bigquery
    execution_date = context["ds_nodash"]
    result = load_gcs_parquet_to_bigquery(
        bucket_name=GCS_BUCKET_NAME,
        project_id=BQ_PROJECT_ID,
        dataset_id=BQ_RAW_DATASET,
        partition_date=execution_date,
    )

def _run_dbt(**context):
    from bq_loader import run_dbt_models
    return_code = run_dbt_models(
        dbt_project_dir="/opt/airflow/dbt",
        profiles_dir="/opt/airflow/dbt",
        select="staging+ marts",
        command="build",
    )
    if return_code != 0:
        raise RuntimeError(f"dbt build failed with return code {return_code}")

with DAG(
    dag_id="jkia_flight_pipeline",
    schedule="*/1 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
) as dag:
    poll_and_produce = PythonOperator(
        task_id="poll_opensky_and_produce_to_kafka",
        python_callable=_poll_and_produce,
    )
    consume_to_gcs = PythonOperator(
        task_id="consume_kafka_and_sink_to_gcs",
        python_callable=_consume_to_gcs,
    )
    load_to_bq = PythonOperator(
        task_id="load_gcs_parquet_to_bigquery",
        python_callable=_load_to_bigquery,
    )
    run_dbt = PythonOperator(
        task_id="run_dbt_transformations",
        python_callable=_run_dbt,
    )

    poll_and_produce >> consume_to_gcs >> load_to_bq >> run_dbt