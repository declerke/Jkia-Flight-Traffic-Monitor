import logging
import os
import io
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Optional
from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)

def get_bq_client() -> bigquery.Client:
    return bigquery.Client()

def load_gcs_parquet_to_bigquery(
    bucket_name: Optional[str] = None,
    project_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    table_id: str = "aircraft_states",
    partition_date: Optional[str] = None,
) -> dict:
    if not bucket_name:
        bucket_name = os.environ.get("GCS_BUCKET_NAME")
    if not project_id:
        project_id = os.environ.get("BQ_PROJECT_ID")
    if not dataset_id:
        dataset_id = os.environ.get("BQ_RAW_DATASET", "jkia_raw")

    if not all([bucket_name, project_id, dataset_id]):
        raise ValueError("bucket_name, project_id, and dataset_id are required")

    if not partition_date:
        partition_date = datetime.now(timezone.utc).strftime("%Y%m%d")

    dt = datetime.strptime(partition_date, "%Y%m%d")
    prefix = f"raw/aircraft_states/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    parquet_blobs = [b for b in blobs if b.name.endswith('.parquet')]
    
    if not parquet_blobs:
        logger.warning(f"No parquet files found in GCS at prefix: {prefix}")
        return {"rows_loaded": 0, "status": "skipped_no_files"}

    logger.info(f"Downloading {len(parquet_blobs)} files from GCS...")
    dfs = []
    for blob in parquet_blobs:
        content = blob.download_as_bytes()
        dfs.append(pd.read_parquet(io.BytesIO(content)))
    
    final_df = pd.concat(dfs, ignore_index=True)

    if 'ingested_at' in final_df.columns:
        final_df['ingested_at'] = pd.to_datetime(final_df['ingested_at'])

    cols_to_string = ['sensors', 'squawk']
    for col in cols_to_string:
        if col in final_df.columns:
            final_df[col] = final_df[col].replace([np.nan, None], "").astype(str).replace("", None)

    client = get_bq_client()
    table_ref = f"{project_id}.{dataset_id}.{table_id}${partition_date}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("sensors", "STRING"),
            bigquery.SchemaField("squawk", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ingested_at",
        ),
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        ],
    )

    logger.info(f"Loading dataframe ({len(final_df)} rows) into {table_ref}")
    load_job = client.load_table_from_dataframe(final_df, table_ref, job_config=job_config)
    load_job.result()

    result = {
        "rows_loaded": load_job.output_rows,
        "table": f"{project_id}.{dataset_id}.{table_id}",
        "partition": partition_date,
        "job_id": load_job.job_id,
        "method": "strict_datetime_dataframe_load"
    }
    logger.info(f"BigQuery load result: {result}")
    return result

def run_dbt_models(
    dbt_project_dir: Optional[str] = None,
    profiles_dir: Optional[str] = None,
    select: Optional[str] = None,
    command: str = "run"
) -> int:
    import subprocess
    
    target_path = "/opt/airflow/dbt"
    
    # Environment check to ensure BQ vars are visible to dbt
    env_vars = ["BQ_PROJECT_ID", "BQ_RAW_DATASET", "GOOGLE_APPLICATION_CREDENTIALS"]
    for var in env_vars:
        val = os.environ.get(var)
        logger.info(f"ENV CHECK: {var} = {'SET' if val else 'MISSING'}")
    
    cmd = [
        "dbt", command, 
        "--project-dir", target_path, 
        "--profiles-dir", target_path, 
        "--select", select if select else "staging+ marts",
        "--no-partial-parse"
    ]

    logger.info(f"Starting dbt {command}: {' '.join(cmd)}")
    
    result = subprocess.run(
        cmd, 
        capture_output=True, 
        text=True, 
        env=os.environ.copy()
    )
    
    if result.stdout:
        logger.info(f"DBT STDOUT:\n{result.stdout}")
    if result.stderr:
        logger.error(f"DBT STDERR:\n{result.stderr}")
        
    return result.returncode