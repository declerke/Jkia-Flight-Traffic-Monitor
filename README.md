# ✈️ JKIA Flight Monitor: Cloud-Native ELT & Analytics Pipeline

**JKIA Flight Monitor** is a production-grade, end-to-end data engineering solution that tracks near-real-time aircraft movements around **Jomo Kenyatta International Airport (JKIA)** in Nairobi. It transforms raw, high-frequency ADS-B state vectors from the OpenSky Network into validated, actionable insights using a modern "Medallion" architecture.

---

## 🎯 Project Goal
To provide a resilient, automated pipeline that captures streaming aviation data, ensures multi-layer data quality through **dbt tests**, and visualizes airport efficiency metrics—such as **Holding Patterns** and **Peak Traffic Waves**—in an executive-grade dashboard.

---

## 🧬 System Architecture
The pipeline follows an **ELT (Extract, Load, Transform)** pattern optimized for Google Cloud Platform:

1. **Ingestion Layer:** Python-based polling of the **OpenSky Network API** (OAuth2) for the JKIA bounding box (−2.5° to 1.5°N, 35.5° to 38.5°E).
2. **Streaming Buffer:** **Apache Kafka** decouples API polling from storage, providing a durable, replayable message queue that protects against downstream failures.
3. **Data Lake (GCS):** A Kafka consumer batches records, converts them to **Parquet** (Snappy compressed) via PyArrow, and sinks them to **Google Cloud Storage** using Hive-style partitioning (`year/month/day/hour`).
4. **Warehouse & Transformation:** **BigQuery** serves as the compute engine. **dbt** (Data Build Tool) performs deduplication, coordinate validation, and complex event modeling.
5. **Quality & BI:** Automated **dbt build** runs 22+ integrity tests before updating the **Looker Studio** analytics layer.

---

## 🛠️ Technical Stack
| Layer | Tools | Purpose |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow 2.9.1 | Managing live (1-min) and backfill DAG schedules |
| **Streaming** | Apache Kafka & Zookeeper | Buffer for real-time state vector streams |
| **Storage** | GCS & BigQuery | Cloud-native Data Lake and Warehouse |
| **Transformation** | dbt (Data Build Tool) | SQL modeling, testing, and documentation |
| **Data Processing** | Python 3.11, Pandas, PyArrow | Schema enforcement and Parquet conversion |
| **Business Intelligence** | Google Looker Studio | Interactive geospatial and traffic dashboards |

---

## 📊 Performance & Results
* **Data Quality:** Achieved a **100% test pass rate** (22/22) across all models, ensuring zero nulls in critical fields like `icao24`.
* **Operational Resilience:** Successfully implemented a **Kafka buffer** that maintained data flow during transient GCS API timeouts.
* **Traffic Insights:** Identified consistent **Morning (04:00-10:00 UTC)** and **Evening (14:00-22:00 UTC)** peak traffic waves at JKIA.
* **Pattern Recognition:** Automated detection of **Holding Patterns** (altitude 1k-4.5k meters, speed <250kts) to classify aircraft delay proxies.

---

## 📂 Project Structure
```text
jkia-flight-traffic-monitor/
├── dags/
│   ├── jkia_flight_pipeline.py      # Live 1-minute interval DAG
│   └── jkia_backfill_pipeline.py    # Historical catchup DAG
├── dbt/
│   ├── models/
│   │   ├── staging/                 # Deduplication & cleaning (stg_aircraft_states)
│   │   └── marts/                   # Hourly, Country, & Holding models
│   └── schema.yml                   # 22+ Data quality tests
├── scripts/
│   ├── kafka_consumer.py            # Kafka-to-GCS Parquet sink with .mask() logic
│   └── bq_loader.py                 # BigQuery loading & dbt orchestration
├── docker-compose.yml               # Multi-container Airflow/Kafka environment
├── requirements.txt                 # Python dependencies (Airflow, Pandas 2.2.2, dbt)
└── README.md
```

---

## ⚙️ Installation & Setup

### 1. Environment & Infrastructure
```bash
git clone https://github.com/declerke/Jkia-Flight-Traffic-Monitor.git
cd jkia-flight-traffic-monitor
cp .env.example .env   # fill in GCS_BUCKET_NAME, BQ_PROJECT_ID, OpenSky credentials
```

### 2. Deploy Containerized Stack
```bash
# Launch Airflow, Kafka, Zookeeper, and Postgres
docker-compose up -d

# Initialize Airflow DB and Admin User
docker-compose run airflow-init
```

### 3. Data Warehouse Initialization
1. Place your GCP Service Account Key in `credentials/gcp.json`.
2. Configure your `.env` with `GCS_BUCKET_NAME` and `BQ_PROJECT_ID`.
3. Trigger the `jkia_flight_pipeline` in the Airflow UI to begin ingestion.

---

## 🧠 Key Design Decisions

- **Kafka as buffer:** OpenSky API is rate-limited and occasionally unavailable. Kafka decouples the 1-minute poller from GCS writes — if GCS has a transient timeout, messages stay in the queue and are consumed on the next cycle without data loss.
- **Hive-style GCS partitioning:** Parquet files land under `year/month/day/hour/` so BigQuery external tables and `LOAD DATA` jobs can prune partitions efficiently, keeping query costs predictable as the data lake grows.
- **dbt `build` over `run + test`:** The DAG calls `dbt build --select staging+ marts`, which runs models and tests together in dependency order. A test failure halts the DAG before bad data reaches downstream marts.
- **Holding pattern proxy:** True ATC holding data is unavailable from ADS-B. The `mart_holding_events` model approximates holding patterns using altitude (1,000–4,500 m), speed (<250 kts), and duration thresholds — a pragmatic domain heuristic that produces interpretable delay signals.
- **`max_active_runs=1` on the live DAG:** The 1-minute schedule can overlap if a run takes >60 seconds. Setting `max_active_runs=1` prevents concurrent GCS writes to the same partition.

---

## 🗄️ dbt Layer Detail

```
jkia_raw.aircraft_states         ← BigQuery source (GCS Parquet → BQ load)
        │
analytics.stg_aircraft_states    ← deduplication, coordinate validation, altitude_band classification
        │
        ├── analytics.mart_hourly_traffic      ← hourly aircraft counts, peak-period flags (morning/evening/off-peak)
        ├── analytics.mart_country_distribution ← daily aircraft by origin country with traffic share %
        ├── analytics.mart_holding_events      ← inferred holding events: brief/standard/extended/significant
        └── analytics.mart_active_aircraft     ← latest known position per aircraft (last 10 min, powers live map)
```

22 dbt tests across source, staging, and mart layers — `not_null`, `unique`, and `accepted_values` constraints on all critical fields.

---

## 🎓 Skills Demonstrated

- **Stream processing** — Kafka producer/consumer pipeline handling 1-minute ADS-B polling cycles with retry and exponential backoff
- **Cloud-native ELT** — GCS Parquet data lake → BigQuery → dbt transformation chain on GCP
- **dbt modelling** — staging deduplication, window functions, domain heuristics encoded as SQL mart logic, 22/22 tests passing
- **Apache Airflow orchestration** — 4-task DAG with `max_active_runs=1`, retry logic, XCom for inter-task metrics, separate backfill DAG
- **Kafka resilience** — buffer maintained data flow during transient GCS API timeouts; self-healing consumer with idle-cycle detection
- **Aviation domain** — ADS-B state vector processing, holding pattern classification, peak traffic wave detection (04:00–10:00 UTC / 14:00–22:00 UTC)
- **Containerised infrastructure** — multi-service Docker Compose (Airflow, Kafka, Zookeeper, Postgres) with health checks and dependency ordering
