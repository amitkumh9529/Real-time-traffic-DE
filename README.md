# Real-Time Mobility & Traffic Data Engineering Lakehouse

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2.0-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7.0-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Hive](https://img.shields.io/badge/Hive%20Metastore-3.1.3-FDEE21?logo=apachehive&logoColor=black)](https://hive.apache.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Compatible-F2C811?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![CI](https://github.com/amitkumh9529/Real-time-traffic-DE/actions/workflows/ci.yml/badge.svg)](https://github.com/amitkumh9529/Real-time-traffic-DE/actions)

A streaming Lakehouse pipeline for vehicular mobility and telemetry events. Built using **Apache Kafka**, **Apache Spark Structured Streaming**, **Delta Lake**, **Apache Hive Metastore**, and **Power BI**, following the **Medallion Lakehouse Architecture (Bronze → Silver/Quarantine → Gold)**.

The project demonstrates production data engineering patterns: lossless raw ingestion, permissive schema parsing with corrupt-record isolation, a dedicated Delta quarantine table with granular reason codes, watermark-bounded deduplication, idempotent Star Schema dimensional modeling via Delta `MERGE`, and comprehensive automated testing.

---

## Architecture

```
                       +---------------------------------------+
                       |       IoT Traffic Telemetry           |
                       |    (producer/traffic_dirty_producer)  |
                       +-------------------+-------------------+
                                           |
                                           v  (JSON / Raw bytes over Kafka)
                       +-------------------+-------------------+
                       |      Apache Kafka (KRaft Mode)        |
                       |         Topic: traffic-topic          |
                       +-------------------+-------------------+
                                           |
                                           v  (Kafka Source: offsets, timestamps)
                       +-------------------+-------------------+
                       |   BRONZE: Lossless Ingestion Job      |
                       |   - Stores raw bytes & metadata       |
                       |   - Mode: PERMISSIVE corrupt capture  |
                       +-------------------+-------------------+
                                           |
                                           v  (Delta Lake)
                       +-------------------+-------------------+
                       |            traffic_bronze             |
                       +-------------------+-------------------+
                                           |
                                           v  (foreachBatch validation)
                       +-------------------+-------------------+
                       |   SILVER: Data Quality & Routing      |
                       |   - Schema validation & type casting  |
                       |   - 15-min Watermark & Deduplication  |
                       +---------+-------------------+---------+
                                 |                   |
               (Valid Records)   v                   v   (Rejected Records)
     +---------------------------+----+         +----+---------------------------+
     |         traffic_silver         |         |       traffic_quarantine       |
     |  Clean, typed, enriched events |         |  Reason codes & error details  |
     +---------------------------+----+         +--------------------------------+
                                 |
                                 v  (foreachBatch Idempotent MERGE)
     +---------------------------+-----------------------------------------------+
     |                         GOLD: Dimensional Model                           |
     |                                                                           |
     |   [ dim_zone ]            [ dim_road ]            [ fact_traffic ]        |
     |   MERGE on city_zone      MERGE on road_id        MERGE on event_id       |
     |   (SCD Type 1)            (SCD Type 1)            Partitioned by date     |
     +-----------------------------------+---------------------------------------+
                                         |
                                         v
     +-----------------------------------+---------------------------------------+
     |      Hive Metastore (PostgreSQL Backend) -> Spark Thrift Server           |
     |                 Power BI DirectQuery / Import Views                       |
     +---------------------------------------------------------------------------+
```

---

## Medallion Layer Design

| Layer | Storage Path | Deduplication Key / Target | Primary Responsibilities |
| :--- | :--- | :--- | :--- |
| **Bronze** | `warehouse/traffic_bronze` | Append-only | • Lossless raw ingestion retaining `topic`, `partition`, `offset`, and timestamps.<br>• Spark `PERMISSIVE` parsing captures malformed bytes into `_corrupt_record`.<br>• Preserves unmapped fields in `raw_payload`. Zero silent data loss. |
| **Silver** | `warehouse/traffic_silver` | `event_id` | • Validates physical boundaries (`0 <= speed <= 160 km/h`) and required fields.<br>• Filters out malformed JSON, clock skew (>10m future), and type errors.<br>• 15-minute watermark on `event_ts` for bounded state deduplication.<br>• Feature engineering: `speed_band`, `peak_flag`, `hour`. |
| **Quarantine**| `warehouse/traffic_quarantine` | Append-only audit | • Dedicated reject store for all invalid/corrupt records.<br>• Stores `quarantine_reason` code, `error_details`, raw payload, and Kafka metadata.<br>• Emits microbatch audit metrics (valid, quarantined, rejection breakdown). |
| **Gold** | `warehouse/dim_zone`<br>`warehouse/dim_road`<br>`warehouse/fact_traffic` | `city_zone`<br>`road_id`<br>`event_id` | • Star Schema dimensional model with natural key constraints.<br>• Idempotent Delta `MERGE` via `foreachBatch` (prevents duplicate facts and dimensions on re-run).<br>• Fact table partitioned by `event_date`. |

---

## Data Quality Contract & Reason Codes

Every incoming record is evaluated through the validation engine (`apps/common/validation.py`). Valid records are routed to Silver; non-conforming records are routed to `traffic_quarantine` with one of the following deterministic reason codes:

| Reason Code | Condition | Action Taken |
| :--- | :--- | :--- |
| `CORRUPT_JSON` | Payload failed JSON parsing (`_corrupt_record` is populated). | Routed to quarantine with corrupt byte payload. |
| `MISSING_REQUIRED_FIELD` | One or more mandatory fields (`event_id`, `vehicle_id`, `road_id`, `city_zone`, `event_time`) are null or blank. | Quarantined; identified fields listed. |
| `INVALID_TIMESTAMP_FORMAT` | `event_time` string cannot be parsed into an ISO-8601 UTC timestamp. | Quarantined with raw timestamp text. |
| `INVALID_SPEED_TYPE` | `speed` is present but contains non-numeric text (e.g. `"fast"`, `"eighty"`). | Quarantined; type cast failure logged. |
| `INVALID_SPEED_RANGE` | `speed` integer is outside physical limits (`< 0` or `> 160 km/h`). | Quarantined with out-of-bound speed value. |
| `FUTURE_TIMESTAMP_SKEW` | `event_ts` is more than 10 minutes ahead of current processing time. | Quarantined; timestamp skew flagged. |
| `DUPLICATE_EVENT` | Record with an identical `event_id` already ingested within the watermark window. | Dropped from Silver during deduplication. |

---

## Streaming Semantics & Guarantees

### 1. Delivery Semantics
- **Kafka Source**: At-least-once ingestion. Messages are committed by Spark Structured Streaming via internal checkpointing after the microbatch is successfully written.
- **Silver Deduplication**: Deduplicated on `event_id` within a **15-minute watermark window** (`withWatermark("event_ts", "15 minutes").dropDuplicates(["event_id"])`).
- **Gold Sinks**: Idempotent Delta `MERGE` writes. Even if a microbatch is replayed (e.g., node failure, streaming restart), Delta Lake merges matching `event_id` records in `fact_traffic`, ensuring **effectively exactly-once** end-to-end semantics.
- *Note:* This pipeline does not claim native two-phase commit. End-to-end consistency is achieved through **at-least-once transport combined with idempotent Delta MERGE operations**.

### 2. Watermark Policy & Late Data
- The pipeline applies a **15-minute watermark delay** (`WATERMARK_DELAY_MINUTES = 15`) on `event_ts`.
- Events arriving more than 15 minutes after the watermark advances are dropped from stateful streaming deduplication memory.

### 3. Checkpointing & State Isolation
- Streaming checkpoints are stored in isolated directories (`chk_bronze`, `chk_silver`, `chk_gold`).
- Rebuilding or resetting a layer requires cleaning only that specific layer's checkpoint and Delta directory.

---

## Directory Structure

```text
mobility-analytics/
├── .env.example                 # Standardized environment template
├── .github/workflows/ci.yml     # Automated CI (linting, tests)
├── docker-compose.yml           # Pinned image infrastructure definition
├── Makefile                     # Linux / macOS CLI runner
├── run.ps1                      # Windows PowerShell CLI runner
├── requirements.txt             # Pipeline Python dependencies
├── requirements-dev.txt         # Dev, linting, and testing dependencies
├── README.md                    # System architecture and operations manual
│
├── apps/                        # Spark Structured Streaming Applications
│   ├── common/                  # Reusable production modules
│   │   ├── config.py            # Centralized settings and warehouse paths
│   │   ├── reference.py         # Domain lookup dictionaries and validation bounds
│   │   ├── spark_session.py     # Local / cluster SparkSession factory
│   │   └── validation.py        # Reusable Data Quality validation and metrics
│   ├── traffic_bronze.py        # Bronze ingestion (Kafka -> Delta)
│   ├── traffic_silver.py        # Silver & Quarantine routing (Delta -> Delta)
│   └── traffic_gold.py          # Gold Star Schema (idempotent Delta MERGE)
│
├── config/                      # Service configuration templates
│   └── hive/hive-site.xml       # Hive Metastore JDBC and warehouse paths
│
├── producer/                    # IoT Telemetry Generator
│   ├── requirements.txt         # Producer dependencies
│   └── traffic_dirty_producer.py# Emits clean events + 9 anomaly classes
│
├── sql/                         # Hive / Spark SQL Scripts
│   ├── 01_init_database.sql     # Database initialization (mobility)
│   ├── 02_create_delta_tables.sql# Registers Bronze, Silver, Quarantine, Gold
│   ├── 03_create_bi_views.sql   # Creates typed BI reporting views
│   ├── 04_validation_queries.sql# Table health checks and KPI validation
│   └── init_all.sql             # Unified master SQL registration script
│
├── scripts/                     # Operational automation scripts
│   ├── verify_local.py          # Zero-Docker local end-to-end verification
│   ├── 01_create_kafka_topic.sh # Kafka topic initialization
│   ├── 02_submit_bronze.sh      # Submits Bronze streaming job
│   ├── 03_submit_silver.sh      # Submits Silver streaming job
│   ├── 04_submit_gold.sh        # Submits Gold dimensional modeling job
│   ├── 05_init_metastore.sh     # Registers tables in Hive Metastore
│   └── 06_start_thrift_server.sh# Launches Spark Thrift Server (JDBC/ODBC)
│
└── tests/                       # Automated Test Suite
    ├── test_producer.py         # Producer anomalies and seed determinism tests
    ├── test_validation.py       # Reusable Data Quality rules and routing tests
    └── test_spark_pipeline.py   # Bronze parsing, Quarantine, and Gold MERGE tests
```

---

## Local Verification (Zero-Docker Mode)

You can verify the entire pipeline locally without Docker. The verification script generates deterministic telemetry, parses Bronze records, routes valid events to Silver and invalid events to Quarantine, and verifies Gold Delta `MERGE` idempotency.

### 1. Install Dependencies
```bash
pip install -r requirements.txt -r requirements-dev.txt
```

### 2. Run All Automated Tests
```powershell
# Windows PowerShell
.\run.ps1 test

# Linux / Mac / WSL
make test
```
*Direct command:*
```bash
python -m unittest discover tests
```

### 3. Run End-to-End Pipeline Verification
```powershell
# Windows PowerShell
.\run.ps1 verify

# Linux / Mac / WSL
make verify
```

#### Verified Pipeline Output:
```text
================================================================================
MOBILITY ANALYTICS - LOCAL END-TO-END PIPELINE VERIFICATION
================================================================================

[Step 1] Generating deterministic telemetry stream (seed=42)...
[Step 2] Executing Bronze Layer parsing with PERMISSIVE corrupt-record capture...
 -> Bronze Delta Table: 50 raw records persisted.

[Step 3] Executing Silver Layer Data Quality validation & Quarantine routing...
 -> Silver Valid Records: 38
 -> Quarantine Rejections: 11
 -> Rejection Breakdown: {'INVALID_SPEED_TYPE': 3, 'INVALID_SPEED_RANGE': 2, 'CORRUPT_JSON': 3, 'MISSING_REQUIRED_FIELD': 3}

[Step 4] Executing Gold Layer Star Schema idempotent MERGE operations...
 -> Initial Gold Facts: 38 | Zones: 5 | Roads: 4

[Step 5] Re-executing Gold MERGE with the EXACT same batch to verify idempotency...
 -> Post-Replay Gold Facts: 38 | Zones: 5 | Roads: 4
 -> Idempotency Verified: Exactly 0 duplicates accumulated upon re-run.

================================================================================
ALL END-TO-END PIPELINE CHECKS PASSED SUCCESSFULLY!
================================================================================
```

---

## Dockerized Cluster Execution

When running with Docker Compose, the complete infrastructure stack is orchestrated:

### 1. Start Infrastructure
```powershell
# Windows
.\run.ps1 up

# Linux / Mac
make up
```
Services started:
- **PostgreSQL 13** (`postgres:13.16-alpine`, port `5435`)
- **Apache Hive Metastore 3.1.3** (`rtdt/hive-metastore:3.1.3`, port `9083`)
- **Apache Spark 3.5.1 Master & Worker** (`rtdt/spark:3.5.1`, ports `8080`, `8081`, `7077`, `10000`)
- **Apache Kafka 3.7.0 (KRaft)** (`apache/kafka:3.7.0`, port `9092`, `29092`)
- **Kafka UI** (`provectuslabs/kafka-ui:v0.7.2`, `http://localhost:8090`)

### 2. Create Topic
```powershell
.\run.ps1 topic
```

### 3. Launch Streaming Jobs
In separate terminals:
```powershell
# Terminal 1: Bronze
.\run.ps1 bronze

# Terminal 2: Silver & Quarantine
.\run.ps1 silver

# Terminal 3: Gold
.\run.ps1 gold
```

### 4. Start Producer
```powershell
# Terminal 4: Telemetry Producer
.\run.ps1 producer
```

To run with a deterministic seed for reproducible debugging:
```bash
python producer/traffic_dirty_producer.py --seed 42 --clean-ratio 0.80
```

### 5. Register Metastore Tables & Start Thrift Server
```powershell
.\run.ps1 init-db
.\run.ps1 thrift
```

### 6. Reset Local Warehouse Data
To reset generated Delta Lake tables and streaming checkpoints cleanly:
```powershell
.\run.ps1 reset
```

---

## Inspecting Quarantined Records & Metrics

To inspect rejected records and review data-quality metrics:

```sql
-- Query quarantine Delta table via Spark SQL / Thrift Server
SELECT 
    quarantine_reason,
    COUNT(*) AS total_rejected,
    MIN(ingest_timestamp) AS earliest_failure,
    MAX(ingest_timestamp) AS latest_failure
FROM mobility.traffic_quarantine
GROUP BY quarantine_reason
ORDER BY total_rejected DESC;

-- Inspect sample poison pills
SELECT 
    quarantine_reason,
    error_details,
    raw_payload,
    kafka_partition,
    kafka_offset
FROM mobility.traffic_quarantine
LIMIT 10;
```

---

## Power BI Integration

1. In **Power BI Desktop**, select **Get Data** → **Spark** (or **ODBC** / **Hive LLAP**).
2. Enter connection details:
   - **Server**: `localhost`
   - **Port**: `10000`
   - **Protocol**: `HTTP` or `Binary`
   - **Database**: `mobility`
3. Select analytical views:
   - `bi_fact_traffic`: Enriched vehicular observations.
   - `bi_dim_zone`: Zone reference, land use category, and congestion risk.
   - `bi_dim_road`: Road classification and speed limits.
   - `bi_traffic_quarantine`: Real-time data quality failure monitoring.
4. Set relationships:
   - `bi_fact_traffic[road_id]` → `bi_dim_road[road_id]` (Many-to-One)
   - `bi_fact_traffic[city_zone]` → `bi_dim_zone[city_zone]` (Many-to-One)

---

## Local Development Notes & Known Limitations

1. **Windows Native Hadoop I/O**: Local Windows Spark execution requires `winutils.exe` and `hadoop.dll` matching the Hadoop version (`3.3.4`). Both `run.ps1` and `tests/test_spark_pipeline.py` automatically detect and resolve `HADOOP_HOME` and `PATH`.
2. **Hive Metastore Port**: Postgres backend is mapped to host port `5435` to avoid conflicting with default local PostgreSQL instances on port `5432`.
3. **Kafka in Docker vs Host**: Internal Docker network connects via `kafka:9092`; host-based scripts connect via `localhost:29092`.
4. **Watermark State Eviction**: In local standalone microbatches without an active continuous stream, late-data assertions are validated deterministically via unit tests in `tests/test_validation.py`.

---

## License
Distributed under the Apache 2.0 License. See `LICENSE` for details.
