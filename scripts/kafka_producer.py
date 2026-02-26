import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "jkia-aircraft-states"

def create_producer(bootstrap_servers: str, retries: int = 5) -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=retries,
        retry_backoff_ms=300,
        request_timeout_ms=30000,
        max_block_ms=60000,
        compression_type="lz4",
    )
    logger.info(f"Kafka producer connected to {bootstrap_servers}")
    return producer

def produce_aircraft_states(
    producer: KafkaProducer,
    records: list[dict],
    topic: str = KAFKA_TOPIC,
) -> dict:
    ingested_at = datetime.now(timezone.utc).isoformat()
    success_count = 0
    error_count = 0
    for record in records:
        record["ingested_at"] = ingested_at
        key = record.get("icao24") or "unknown"
        try:
            future = producer.send(topic, key=key, value=record)
            future.get(timeout=10)
            success_count += 1
        except KafkaError as e:
            logger.error(f"Failed to produce record for icao24={key}: {e}")
            error_count += 1
    producer.flush()
    result = {
        "topic": topic,
        "success_count": success_count,
        "error_count": error_count,
        "ingested_at": ingested_at,
        "total_records": len(records),
    }
    logger.info(f"Producer result: {result}")
    return result

def produce_batch(
    records: list[dict],
    bootstrap_servers: Optional[str] = None,
    topic: str = KAFKA_TOPIC,
) -> dict:
    if not bootstrap_servers:
        bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    producer = create_producer(bootstrap_servers)
    try:
        return produce_aircraft_states(producer, records, topic)
    finally:
        producer.close()