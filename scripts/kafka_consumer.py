import io
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "jkia-aircraft-states"
KAFKA_GROUP_ID = "jkia-gcs-sink"

PARQUET_SCHEMA = pa.schema([
    pa.field("icao24",          pa.string()),
    pa.field("callsign",        pa.string()),
    pa.field("origin_country",  pa.string()),
    pa.field("time_position",   pa.int64()),
    pa.field("last_contact",    pa.int64()),
    pa.field("longitude",       pa.float64()),
    pa.field("latitude",        pa.float64()),
    pa.field("baro_altitude",   pa.float64()),
    pa.field("on_ground",       pa.bool_()),
    pa.field("velocity",        pa.float64()),
    pa.field("true_track",      pa.float64()),
    pa.field("vertical_rate",   pa.float64()),
    pa.field("sensors",         pa.string()),
    pa.field("geo_altitude",    pa.float64()),
    pa.field("squawk",          pa.string()),
    pa.field("spi",             pa.bool_()),
    pa.field("position_source", pa.int64()),
    pa.field("poll_timestamp",  pa.int64()),
    pa.field("ingested_at",     pa.string()),
])

def build_gcs_path(bucket_name: str, ingested_at: str) -> str:
    dt = datetime.fromisoformat(ingested_at.replace("Z", "+00:00"))
    return (
        f"raw/aircraft_states/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        f"hour={dt.hour:02d}/"
        f"batch_{int(dt.timestamp())}.parquet"
    )

def records_to_parquet_bytes(records: list[dict]) -> bytes:
    df = pd.DataFrame(records)
    
    cols_to_str = ["icao24", "callsign", "origin_country", "sensors", "squawk", "ingested_at"]
    for col in cols_to_str:
        if col in df.columns:
            df[col] = df[col].astype(str).mask(df[col].isna(), None)
    
    cols_to_int = ["time_position", "last_contact", "poll_timestamp", "position_source"]
    for col in cols_to_int:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    cols_to_float = ["longitude", "latitude", "baro_altitude", "velocity", 
                     "true_track", "vertical_rate", "geo_altitude"]
    for col in cols_to_float:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    table = pa.Table.from_pandas(df, schema=PARQUET_SCHEMA, safe=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()

def upload_to_gcs(bucket_name: str, gcs_path: str, data: bytes, gcs_client: storage.Client) -> None:
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(data, content_type="application/octet-stream")
    logger.info(f"UPLOAD SUCCESS: {len(data)} bytes to gs://{bucket_name}/{gcs_path}")

def consume_and_sink(
    bootstrap_servers: Optional[str] = None,
    bucket_name: Optional[str] = None,
    topic: str = KAFKA_TOPIC,
    group_id: str = KAFKA_GROUP_ID,
    batch_size: int = 100,
    poll_timeout_ms: int = 5000,
    max_idle_cycles: int = 10,
) -> dict:
    if not bootstrap_servers:
        bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    if not bucket_name:
        bucket_name = os.environ.get("GCS_BUCKET_NAME")
    
    gcs_client = storage.Client()
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        max_poll_records=batch_size,
        consumer_timeout_ms=poll_timeout_ms * max_idle_cycles,
    )

    total_written = 0
    total_files = 0
    buffer = []

    try:
        idle_cycles = 0
        while idle_cycles < max_idle_cycles:
            messages = consumer.poll(timeout_ms=poll_timeout_ms, max_records=batch_size)
            if not messages:
                idle_cycles += 1
                continue
            idle_cycles = 0

            for tp, partition_messages in messages.items():
                for msg in partition_messages:
                    buffer.append(msg.value)

            if len(buffer) >= batch_size:
                _flush_buffer(buffer, bucket_name, gcs_client)
                total_written += len(buffer)
                total_files += 1
                buffer.clear()
                consumer.commit()

        if buffer:
            _flush_buffer(buffer, bucket_name, gcs_client)
            total_written += len(buffer)
            total_files += 1
            consumer.commit()

    finally:
        consumer.close()

    return {
        "records_written": total_written,
        "files_created": total_files,
        "bucket": bucket_name,
    }

def _flush_buffer(buffer: list[dict], bucket_name: str, gcs_client: storage.Client) -> None:
    if not buffer: return
    ingested_at = buffer[-1].get("ingested_at", datetime.now(timezone.utc).isoformat())
    gcs_path = build_gcs_path(bucket_name, ingested_at)
    parquet_bytes = records_to_parquet_bytes(buffer)
    upload_to_gcs(bucket_name, gcs_path, parquet_bytes, gcs_client)