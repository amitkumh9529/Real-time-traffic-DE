# Real-Time Mobility & Traffic Lakehouse Pipeline

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2.0-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7.0-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Hive](https://img.shields.io/badge/Hive%20Metastore-3.1.3-FDEE21?logo=apachehive&logoColor=black)](https://hive.apache.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Compatible-F2C811?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)

A streaming data engineering lakehouse pipeline for high-frequency vehicular telemetry and traffic congestion intelligence. Built with **Apache Kafka**, **Apache Spark Structured Streaming**, **Delta Lake**, **Apache Hive Metastore**, and **Power BI**, implementing the **Medallion Architecture (Bronze → Silver/Quarantine → Gold)**.

The repository demonstrates intermediate-to-advanced data engineering practices: genuinely lossless raw Kafka byte ingestion, streaming watermark deduplication, replay-idempotent Delta MERGE operations across all layers, true SCD Type 1 dimension updates, microbatch data-quality metrics persistence, and comprehensive automated test coverage without Docker dependencies.

---

## Architecture & Data Flow

```
                         +-----------------------------------------------+
                         |           IoT Traffic Telemetry               |
                         |     (producer/traffic_dirty_producer.py)      |
                         +-----------------------+-----------------------+
                                                 |
                                                 v  (JSON / Raw Corrupt Bytes)
                         +-----------------------+-----------------------+
                         |       Apache Kafka 3.7.0 (KRaft Mode)         |
                         |             Topic: traffic-topic              |
                         +-----------------------+-----------------------+
                                                 |
                                                 v  (Kafka Source: offsets, raw bytes)
                         +-----------------------+-----------------------+
                         |      BRONZE: Lossless Raw Ingestion           |
                         |  - Persists raw_payload_bytes (BinaryType)    |
                         |  - Encodes raw_payload_base64 & raw_payload   |
                         |  - Stores topic, partition, offset, timestamp |
                         |  - Mode: PERMISSIVE corrupt-record capture    |
                         +-----------------------+-----------------------+
                                                 |
                                                 v  (Delta Lake: Append-only)
                         +-----------------------+-----------------------+
                         |             traffic_bronze                    |
                         +-----------------------+-----------------------+
                                                 |
                                                 v  (Spark Structured Streaming)
                         +-----------------------+-----------------------+
                         |       SILVER: Quality, Watermark, & Routing   |
                         |  - Rule-based validation engine               |
                         |  - Invalid / corrupt records -> Quarantine    |
                         |  - Valid stream: 15-min Watermark & Stateful  |
                         |    Deduplication on event_id                  |
                         +-----------+-----------------------+-----------+
                                     |                       |
                     (Valid Events)  v                       v  (Rejections)
            +------------------------+---+       +-----------+------------------------+
            |      traffic_silver        |       |       traffic_quarantine           |
            | - Idempotent MERGE on      |       | - Idempotent MERGE on              |
            |   event_id                 |       |   (kafka_topic, partition, offset) |
            | - Enriched speed bands     |       | - Deterministic reason codes       |
            +------------------------+---+       +------------------------------------+
                                     |
                                     |----------> +-----------------------------------+
                                     |            |         pipeline_metrics          |
                                     |            | - Idempotent MERGE on             |
                                     |            |   (pipeline_name, batch_id)       |
                                     |            | - Audit counts & failure reasons  |
                                     |            +-----------------------------------+
                                     v  (foreachBatch Idempotent Star Schema)
            +------------------------+------------------------------------------------+
            |                      GOLD: Dimensional Model                            |
            |                                                                         |
            |     [ dim_zone ]              [ dim_road ]            [ fact_traffic ]  |
            |   True SCD Type 1           True SCD Type 1           Idempotent MERGE  |
            |   MERGE on city_zone        MERGE on road_id          MERGE on event_id |
            |   (whenMatchedUpdate)       (whenMatchedUpdate)       Partitioned date  |
            +--------------------------------+----------------------------------------+
                                             |
                                             v
            +--------------------------------+----------------------------------------+
            |      Hive Metastore (PostgreSQL Backend) -> Spark Thrift Server         |
            |           Power BI DirectQuery / Import Analytics Views                 |
            +-------------------------------------------------------------------------+
```

---

## Medallion Layer Design

| Layer | Target Delta Path | Natural / Deduplication Key | Responsibilities & Idempotency Guarantees |
| :--- | :--- | :--- | :--- |
| **Bronze** | `warehouse/traffic_bronze` | Append-only (Source lineage) | • **Lossless Raw Ingestion**: Retains original Kafka `value` as binary (`raw_payload_bytes`), Base64 representation (`raw_payload_base64`), and safely decoded UTF-8 string (`raw_payload`).<br>• Captures Kafka metadata (`kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `ingest_timestamp`, `kafka_key`).<br>• **Production Safeguard**: Configured with `failOnDataLoss = true` by default to halt and alert upon Kafka offset gaps or retention dropouts.<br>• Spark `PERMISSIVE` mode with `columnNameOfCorruptRecord` isolates unparseable payloads without dropping them. |
| **Silver** | `warehouse/traffic_silver` | `event_id` | • **Stateful Streaming Deduplication**: Executes `.withWatermark("event_ts", "15 minutes").dropDuplicates(["event_id"])` in Structured Streaming before writing.<br>• **Replay Idempotency**: Writes via Delta `MERGE` on `event_id` inside `foreachBatch`. Replaying a failed microbatch never duplicates Silver rows.<br>• **Live Metrics**: `process_silver_valid_microbatch()` automatically persists microbatch volume metrics to `pipeline_metrics`.<br>• Enriches events with physical speed categorizations (`LOW`, `MEDIUM`, `HIGH`) and peak transit flags. |
| **Quarantine**| `warehouse/traffic_quarantine` | `(kafka_topic, kafka_partition, kafka_offset)` | • Dedicated audit sink for malformed JSON, out-of-bounds speed, clock skew, and missing contract fields.<br>• **Replay Idempotency**: Deduplicated on Kafka source coordinate identity `(kafka_topic, kafka_partition, kafka_offset)`, ensuring poison pills without an `event_id` are never duplicated on replay.<br>• **Live Metrics**: `process_quarantine_microbatch()` persists failure counts and granular reason code breakdowns on every microbatch. |
| **Metrics** | `warehouse/pipeline_metrics` | `(pipeline_name, batch_id)` | • Persists audit statistics per live microbatch: `pipeline_name`, `batch_id`, `processing_timestamp`, `total_records`, `valid_records`, `quarantined_records`, and JSON rejection breakdown.<br>• **Replay Idempotency**: Merges on `(pipeline_name, batch_id)` so microbatch retries update metrics in-place. |
| **Gold** | `warehouse/dim_zone`<br>`warehouse/dim_road`<br>`warehouse/fact_traffic` | `city_zone`<br>`road_id`<br>`event_id` | • **Star Schema Dimensional Model**: Fact table linked to zone and road dimensions.<br>• **True SCD Type 1**: `dim_zone` and `dim_road` execute `whenMatchedUpdate` on changed attributes (e.g. speed limit adjustments or zone re-classifications).<br>• **First-Write-Wins Fact Idempotency**: Facts merge on `event_id` alone. If an incoming record reuses an existing `event_id` with conflicting attributes, the pipeline detects the conflict, logs a warning, and preserves the canonical first-write fact. |

---

## Data Quality Contract & Reason Codes

Every incoming record is evaluated by `apps/common/validation.py`. Clean records route to Silver; non-conforming records route to `traffic_quarantine` with deterministic audit codes:

| Quarantine Reason Code | Condition | Behavior |
| :--- | :--- | :--- |
| `CORRUPT_JSON` | Payload failed JSON schema parsing (`_corrupt_record` is populated). | Quarantined with raw corrupt byte payload and Kafka source offsets. |
| `MISSING_REQUIRED_FIELD` | One or more mandatory fields (`event_id`, `vehicle_id`, `road_id`, `city_zone`, `event_time`) are null or blank. | Quarantined; missing fields cataloged in `error_details`. |
| `INVALID_TIMESTAMP_FORMAT`| `event_time` cannot be parsed into an ISO-8601 UTC timestamp. | Quarantined with unparseable timestamp text. |
| `INVALID_SPEED_TYPE` | `speed` contains non-numeric text or unsupported datatype. | Quarantined; conversion exception logged. |
| `INVALID_SPEED_RANGE` | `speed` is outside physical vehicular limits (`< 0` or `> 160 km/h`). | Quarantined with out-of-bound speed integer. |
| `FUTURE_TIMESTAMP_SKEW` | `event_ts` is more than 10 minutes ahead of processing time. | Quarantined to prevent streaming watermark corruption. |
| `DUPLICATE_EVENT` | Duplicate `event_id` arriving within the active watermark window. | Dropped by stateful streaming engine; audited in metrics. |

---

## System Semantics, Idempotency, and Failure Recovery

In a production streaming lakehouse interview discussion, precise delivery semantics and failure boundaries are paramount.

### 1. Delivery Semantics Across Layers

- **Kafka Ingestion**: At-least-once transport. Consumers pull messages from Kafka partitions and track offsets in Spark streaming checkpoints.
- **Fail-On-Data-Loss**: Configured with `failOnDataLoss = true` by default in `apps/common/config.py` to prevent silent data loss if Kafka retention trims unread offsets. Can be overridden with `KAFKA_FAIL_ON_DATA_LOSS=false` when deliberate development resets are needed.
- **Spark Structured Streaming Engine**: Checkpointed state store tracks microbatch IDs and stateful deduplication buffers in RocksDB / HDFS state stores.
- **Delta Lake Storage**: ACID transactions ensure atomic commits per table with optimistic concurrency control.
- **End-to-End Result**: **Effectively Exactly-Once**. While transport is at-least-once, downstream stateful deduplication and Delta `MERGE` statements guarantee that duplicate messages or re-run microbatches yield zero duplicate records in Silver, Quarantine, Metrics, or Gold.

### 2. Watermark Eviction & Observable Metrics

- The streaming pipeline defines a 15-minute watermark delay (`WATERMARK_DELAY_MINUTES = 15`) on `event_ts`.
- **In-Window Duplicates**: If a duplicate `event_id` arrives within 15 minutes of the watermark, Spark's state store identifies the key and evicts the duplicate record from the valid stream.
- **Late Arrivals (> 15 Minutes)**: Telemetry events arriving with `event_ts < watermark` are dropped by the streaming engine to bound memory consumption.
- **Engine-Level Suppression vs. Observable Metrics**: In Spark Structured Streaming, `.withWatermark().dropDuplicates()` executes via `StreamingDeduplicateExec`. Duplicate rows and late arrivals past the watermark are dropped directly by the state store at the physical execution plan level and are never emitted to downstream `foreachBatch` sinks. Therefore, `pipeline_metrics` honestly records observable microbatch quantities (`total_records`, `valid_records`, `quarantined_records`, and `rejection_reasons`). Tracking exact dropped duplicate counts at row-level would require custom stateful operators (`flatMapGroupsWithState`) or a query listener parsing engine progress.

### 3. Gold First-Write-Wins Idempotency & Conflict Guard

- In `fact_traffic`, the business key is `event_id` alone.
- Delta `MERGE` applies **first-write-wins idempotency**: if an `event_id` is replayed or re-transmitted, the existing fact in the Delta Lake remains canonical.
- **Content-Consistency Conflict Detection**: Before issuing the merge, `apps/traffic_gold.py` checks incoming records against existing facts. If an `event_id` arrives with materially conflicting attributes (different vehicle, road, or speed), a warning is explicitly logged to flag potential upstream key collision or schema drift without corrupting canonical analytical history.

### 4. Limitations of Cross-Table Atomicity

In Apache Spark and Delta Lake (as of Delta Lake 3.2), multi-table atomic transactions across separate Delta paths do not exist. In `traffic_silver.py`, the microbatch writes to distinct Delta sinks:
1. `traffic_silver` (Valid records)
2. `traffic_quarantine` (Invalid records)
3. `pipeline_metrics` (Batch statistics)

Because these are separate Delta commits, a crash could theoretically occur after Quarantine commits but before Silver or Metrics commits.

### 5. Failure and Recovery Procedures

The pipeline is architected so that **partial batch failures are self-healing upon restart**:
- **Recovery Mechanism**: When the streaming job restarts, Spark's checkpoint log re-executes the uncommitted microbatch with the identical `batch_id`.
- **Silver Idempotency**: Clean records are upserted via `MERGE INTO traffic_silver USING ... ON target.event_id = source.event_id`. Re-processing an already-inserted event results in a no-op.
- **Quarantine Idempotency**: Rejection records are upserted via `MERGE INTO traffic_quarantine USING ... ON target.kafka_topic = source.kafka_topic AND target.kafka_partition = source.kafka_partition AND target.kafka_offset = source.kafka_offset`. Re-running the batch updates existing records without duplication, even when `event_id` is null or corrupt.
- **Metrics Idempotency**: Batch metrics are upserted via `MERGE INTO pipeline_metrics USING ... ON target.pipeline_name = source.pipeline_name AND target.batch_id = source.batch_id`. Re-running updates the batch summary without duplicate rows.

---

## Environment Prerequisites

The project supports both containerized Docker deployment and zero-Docker local execution:

| Prerequisite | Minimum Version | Verification / Discovery Notes |
| :--- | :--- | :--- |
| **Python** | `3.10` - `3.14` | Tested on Python 3.14. Virtual environment recommended. |
| **Java JDK / JRE** | `Java 8`, `11`, or `17` | `JAVA_HOME` must be set and on `PATH`. Required for PySpark JVM runtime. |
| **Apache Spark** | `3.5.1` (bundled via PySpark) | `SPARK_HOME` is discovered dynamically at runtime via `os.path.dirname(pyspark.__file__)`. |
| **Delta Lake** | `3.2.0` | Configured automatically via `delta-spark` package and Ivy resolution. |
| **Windows Hadoop Binaries** | `Hadoop 3.3.4` | Windows local execution requires `winutils.exe` and `hadoop.dll` in `C:\hadoop\bin`. Automatically configured if present. |
| **Docker & Compose** | `Docker 24.0+`, `Compose v2` | Required only for multi-container cluster mode (Kafka, Metastore, Postgres, Thrift). |

> **Note on Docker-Free Local Verification**: Zero-Docker local verification runs native PySpark jobs locally against temporary directories. It requires local Java (JDK 8/11/17) and Python dependencies (`pip install -r requirements.txt -r requirements-dev.txt`), with `winutils.exe` present on Windows systems.

---

## Verification & Execution Guide

### 1. Code Quality & Linting
Run Ruff check across all application code, producer, tests, and scripts:
```bash
python -m ruff check apps producer tests scripts
```
*Expected result: Exits with code 0 (All checks passed).*

### 2. Automated Test Suite
Execute the 23-test unit and integration test suite locally:
```powershell
# Windows PowerShell
.\run.ps1 test

# Linux / Mac / WSL
python -m unittest discover tests
```

#### Test Suite Coverage:
- `test_producer.py`: Deterministic seed reproducibility, schema contract conformance, 9 distinct edge anomaly classes, raw byte serialization.
- `test_validation.py`: Null/corrupt payload routing, speed boundary limits (0-160 km/h), future clock skew detection, metric calculation.
- `test_spark_pipeline.py`:
  - Bronze lossless binary bytes retention and corrupt-record isolation.
  - Silver & Quarantine replay idempotency (assert zero row accumulation on duplicate execution).
  - Stateful streaming deduplication across microbatches within the 15-minute watermark window.
  - Delta metrics table persistence and idempotent update on `(pipeline_name, batch_id)`.
  - Gold true SCD Type 1 in-place attribute updates and global `event_id` fact uniqueness.

*Expected result: `Ran 23 tests ... OK`.*

### 3. Local End-to-End Pipeline Verification
Run the automated end-to-end simulation script:
```powershell
# Windows PowerShell
.\run.ps1 verify

# Linux / Mac / WSL
python scripts/verify_local.py
```

#### Verification Terminal Output:
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

[Step 5] Re-executing ALL MERGE operations with the EXACT same batch to verify replay idempotency...
 -> Post-Replay Silver Count: 38 (expected: 38)
 -> Post-Replay Quarantine Count: 11 (expected: 11)
 -> Post-Replay Metrics Records: 1 (expected: 1)
 -> Post-Replay Gold Facts: 38 | Zones: 5 | Roads: 4
 -> Idempotency Verified: Exactly 0 duplicates accumulated across all layers upon re-run.

================================================================================
ALL END-TO-END PIPELINE CHECKS PASSED SUCCESSFULLY!
================================================================================
```

### 4. Dockerized Cluster Execution

When running with full infrastructure:
```powershell
# 1. Start containers (Kafka, Postgres, Hive Metastore, Spark Master/Worker)
.\run.ps1 up

# 2. Initialize Kafka topic
.\run.ps1 topic

# 3. Submit Spark streaming jobs in separate terminals
.\run.ps1 bronze
.\run.ps1 silver
.\run.ps1 gold

# 4. Launch deterministic IoT telemetry producer
.\run.ps1 producer

# 5. Initialize Hive Metastore tables & launch Spark Thrift Server for Power BI
.\run.ps1 init-db
.\run.ps1 thrift
```

---

## Data Quality Monitoring & SQL Queries

To inspect quality metrics, poison pills, and pipeline health via Hive/Spark SQL (`sql/04_validation_queries.sql`):

```sql
-- 1. Inspect Microbatch Processing Trends & Quality Rates
SELECT 
    batch_id,
    processing_timestamp,
    total_records,
    valid_records,
    quarantined_records,
    duplicate_records,
    late_records,
    ROUND((valid_records * 100.0) / NULLIF(total_records, 0), 2) AS clean_data_pct
FROM mobility.pipeline_metrics
ORDER BY CAST(batch_id AS BIGINT) DESC
LIMIT 20;

-- 2. Quarantine Rejection Breakdown
SELECT 
    quarantine_reason,
    COUNT(*) AS total_rejected,
    MIN(ingest_timestamp) AS earliest_rejection,
    MAX(ingest_timestamp) AS latest_rejection
FROM mobility.traffic_quarantine
GROUP BY quarantine_reason
ORDER BY total_rejected DESC;

-- 3. Verify Zero Duplicate Facts
SELECT 
    event_id,
    COUNT(*) AS occurrences
FROM mobility.fact_traffic
GROUP BY event_id
HAVING COUNT(*) > 1;
```

---

## Power BI Integration

1. In **Power BI Desktop**, select **Get Data** → **Spark** (or **ODBC** / **Hive LLAP**).
2. Enter connection coordinates:
   - **Server**: `localhost`
   - **Port**: `10000`
   - **Protocol**: `HTTP` or `Binary`
   - **Database**: `mobility`
3. Reporting Views:
   - `bi_fact_traffic`: Vehicular observations with time/speed dimensions.
   - `bi_dim_zone`: Zone geography, land use classification, and congestion risk.
   - `bi_dim_road`: Road hierarchy, corridor identifiers, and speed limit thresholds.
   - `bi_pipeline_metrics`: Microbatch ingestion SLAs, throughput, and clean record ratios.
   - `bi_traffic_quarantine`: Real-time data quality failure diagnostics.
4. Schema Relationships:
   - `bi_fact_traffic[road_id]` → `bi_dim_road[road_id]` (Many-to-One)
   - `bi_fact_traffic[city_zone]` → `bi_dim_zone[city_zone]` (Many-to-One)

---

## Production Limitations & Next Steps

For a production deployment in an enterprise cloud environment (AWS, GCP, or Azure), the following architectural enhancements should be planned:

1. **Dead-Letter Alerting & PagerDuty/Slack Integration**: Currently, quarantine records are persisted to Delta. In production, a streaming monitor or threshold alert on `pipeline_metrics` should trigger alerts when `quarantined_records / total_records` exceeds 5% over a 5-minute rolling window.
2. **Secrets Management**: Configuration in `.env` should be replaced with managed secret stores (e.g., AWS Secrets Manager, HashiCorp Vault, Azure Key Vault) with IAM role-based authentication rather than static credentials.
3. **Table Optimization & Lifecycle Compaction**: Streaming microbatches generate small Delta files. A production deployment requires scheduled `OPTIMIZE ... ZORDER BY (event_ts, road_id)` maintenance jobs alongside `VACUUM` policies (e.g., 7-day retention) to manage storage costs and maintain query performance.
4. **Distributed Orchestration**: Moving from standalone Docker to managed Kubernetes (Amazon EKS / Google GKE) or managed platforms (Databricks, AWS EMR Serverless) with autoscaling driver/executor pools and cluster spot termination handling.
5. **Schema Evolution Policies**: Currently, schema drift is captured in `raw_payload`. Implementing schema evolution with explicit contract versioning and automated Avro/Protobuf schema registries (e.g., Confluent Schema Registry) prevents silent field semantic shifts.

---

## License
Distributed under the Apache 2.0 License.
