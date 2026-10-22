# Real-Time Mobility & Traffic Data Engineering Lakehouse

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2.0-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft%20Mode-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Hive](https://img.shields.io/badge/Hive%20Metastore-3.1.3-FDEE21?logo=apachehive&logoColor=black)](https://hive.apache.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Serving-F2C811?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Docker](https://img.shields.io/badge/Docker%20Compose-Orchestrated-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

An enterprise-grade, end-to-end streaming Lakehouse that processes high-velocity IoT vehicular telemetry data with sub-second latency. Built using **Apache Kafka**, **Apache Spark Structured Streaming**, **Delta Lake**, **Apache Hive Metastore**, and **Power BI**, following the **Medallion Lakehouse Architecture (Bronze → Silver → Gold)**.

---

## 🏛️ Architecture Overview

```
                      +---------------------------------------+
                      |       IoT Traffic Telemetry           |
                      |   (traffic_dirty_producer.py)        |
                      +-------------------+-------------------+
                                          |
                                          v (JSON over TCP:29092)
                      +-------------------+-------------------+
                      |      Apache Kafka (KRaft Mode)        |
                      |         Topic: traffic-topic          |
                      +-------------------+-------------------+
                                          |
                      +-------------------v-------------------+
                      |       SPARK STRUCTURED STREAMING      |
                      +-------------------+-------------------+
                                          |
      +-----------------------------------v-----------------------------------+
      |                            DELTA LAKE                                 |
      |                                                                       |
      |   [ Bronze Layer ]   ==>  [ Silver Layer ]   ==>  [ Gold Layer ]      |
      |   - Raw JSON              - Corrupt JSON DQ       - Star Schema       |
      |   - Kafka Metadata        - 15-min Watermark      - dim_zone          |
      |   - Append-only           - Deduplication         - dim_road          |
      |   - Full replayability    - Feature Eng.          - fact_traffic      |
      +-----------------------------------+-----------------------------------+
                                          |
                                          v
                      +-------------------+-------------------+
                      |       Apache Hive Metastore           |
                      |      (PostgreSQL 13 Backend)          |
                      +-------------------+-------------------+
                                          |
                                          v
                      +-------------------+-------------------+
                      |        Spark Thrift Server            |
                      |          (Port 10000 JDBC)            |
                      +-------------------+-------------------+
                                          |
                                          v
                      +-------------------+-------------------+
                      |       Power BI Dashboards             |
                      |  (Real-Time Analytics & KPIs)         |
                      +---------------------------------------+
```

---

## 📂 Production Folder Structure

```text
mobility-analytics/
├── .env.example                 # Standardized environment variables template
├── .gitignore                   # Ignores warehouse files, checkpoints, caches
├── docker-compose.yml           # Multi-service infrastructure orchestration
├── Makefile                     # Linux / macOS / WSL automation runner
├── run.ps1                      # Windows PowerShell automation orchestrator
├── README.md                    # Project documentation & operational handbook
│
├── apps/                        # PySpark Structured Streaming Jobs
│   ├── common/                  # Modular utilities & shared configurations
│   │   ├── __init__.py
│   │   ├── config.py            # Warehouse paths, Kafka topics, broker URLs
│   │   └── spark_session.py     # Delta Lake & Hive-enabled SparkSession builder
│   ├── traffic_bronze.py        # Bronze: Raw Kafka ingestion to Delta
│   ├── traffic_silver.py        # Silver: DQ sanitization, watermarking, dedup
│   └── traffic_gold.py          # Gold: Dimensional modeling (Star Schema)
│
├── config/                      # Service configurations
│   └── hive/
│       └── hive-site.xml        # Hive Metastore JDBC & warehouse settings
│
├── producer/                    # Telemetry generator simulating IoT sensors
│   ├── requirements.txt         # Producer Python dependencies
│   └── traffic_dirty_producer.py# Emits clean & 9 edge anomaly patterns
│
├── scripts/                     # Operational Bash automation scripts
│   ├── 01_create_kafka_topic.sh # Topic initialization (3 partitions)
│   ├── 02_submit_bronze.sh      # Submits Bronze streaming job
│   ├── 03_submit_silver.sh      # Submits Silver streaming job
│   ├── 04_submit_gold.sh        # Submits Gold dimensional modeling job
│   ├── 05_init_metastore.sh     # Registers Delta tables & BI views
│   ├── 06_start_thrift_server.sh# Starts Thrift server on port 10000
│   └── run_producer.sh          # Installs dependencies & runs producer
│
└── sql/                         # SQL DDL & DML Scripts
    ├── 01_init_database.sql     # Database initialization (mobility)
    ├── 02_create_delta_tables.sql# Registers external Delta tables
    ├── 03_create_bi_views.sql   # Creates typed BI reporting views
    ├── 04_validation_queries.sql# Health checks and KPI validation
    └── init_all.sql             # Unified master SQL setup script
```

---

## ⚡ Quick Start Guide

### Prerequisites
- [Docker & Docker Desktop](https://www.docker.com/) (with 4GB+ RAM allocated)
- Python 3.9+ (for the local producer)
- [Power BI Desktop](https://powerbi.microsoft.com/) (optional, for dashboarding)

---

### Step 1: Start the Infrastructure

#### On Windows (PowerShell):
```powershell
.\run.ps1 up
```

#### On Linux / macOS / WSL:
```bash
make up
```

This starts:
- **PostgreSQL 13** (`hive-metastore-db` on port `5435`)
- **Apache Hive Metastore 3.1.3** (`hive-metastore` on port `9083`)
- **Apache Spark 3.5.1 Master** (`spark-master` on port `8080`, `7077`)
- **Apache Spark 3.5.1 Worker** (`spark-worker` on port `8081`, `4040`, `10000`)
- **Apache Kafka (KRaft)** (`kafka` on port `9092`, `29092`)
- **Kafka UI** (`kafka-ui` on `http://localhost:8090`)

---

### Step 2: Create Kafka Topic
```powershell
# Windows
.\run.ps1 topic

# Linux / Mac
make topic
```

---

### Step 3: Launch Spark Streaming Jobs (Bronze, Silver, Gold)

Open separate terminal tabs for each streaming layer:

```powershell
# Tab 1: Bronze Ingestion
.\run.ps1 bronze

# Tab 2: Silver Cleaning & Watermarking
.\run.ps1 silver

# Tab 3: Gold Dimensional Modeling
.\run.ps1 gold
```

*(On Linux / Mac, use `make bronze`, `make silver`, `make gold`)*

---

### Step 4: Start the Telemetry Producer
In another terminal, start the synthetic edge telemetry producer:
```powershell
# Windows
.\run.ps1 producer

# Linux / Mac
make producer
```
The producer generates 70% clean data and 30% realistic dirty anomalies (corrupt JSON, speed spikes, negative values, clock skew, and schema drift).

---

### Step 5: Initialize Hive Metastore & BI Views
Once streaming data starts landing in the Gold tables:
```powershell
# Windows
.\run.ps1 init-db

# Linux / Mac
make init-db
```

---

### Step 6: Start Spark Thrift Server (for Power BI)
```powershell
# Windows
.\run.ps1 thrift

# Linux / Mac
make thrift
```

---

## 📊 Medallion Architecture Transformation Details

| Layer | Input Source | Primary Responsibilities | Output Target |
| :--- | :--- | :--- | :--- |
| **Bronze** | Kafka Topic (`traffic-topic`) | - Lossless append-only raw JSON ingestion<br>- Retains Kafka partition metadata & timestamps<br>- Permissive schema absorption | `/opt/spark/warehouse/traffic_bronze` |
| **Silver** | Bronze Delta Table | - DQ Flagging (`MISSING_VEHICLE`, `CORRUPT_JSON`, etc.)<br>- Safe type casting (`speed_int`)<br>- Boundary validation (`0 <= speed <= 160`)<br>- **15-Minute Event-Time Watermarking**<br>- **Stateful Deduplication** on `[vehicle_id, event_ts]`<br>- Feature Engineering (`peak_flag`, `speed_band`) | `/opt/spark/warehouse/traffic_silver` |
| **Gold** | Silver Delta Table | - **Star Schema Dimensional Modeling**:<br>  * `dim_zone`: Classification (`Commercial`, `IT HUB`, etc.)<br>  * `dim_road`: Corridor limits & road types<br>  * `fact_traffic`: Granular traffic metrics & dates | `/opt/spark/warehouse/dim_zone`<br>`/opt/spark/warehouse/dim_road`<br>`/opt/spark/warehouse/fact_traffic` |

---

## 📈 Power BI Integration Guide

1. Open **Power BI Desktop**.
2. Click **Get Data** → Select **Spark** (or **ODBC** / **Hive LLAP / Spark ODBC Driver**).
3. Enter connection parameters:
   - **Server**: `localhost` (or `host.docker.internal`)
   - **Port**: `10000`
   - **Protocol**: `HTTP` or `Binary` (Standard Thrift)
   - **Database**: `mobility`
4. Choose **Import** or **DirectQuery**.
5. Select the curated analytical views:
   - `bi_fact_traffic`
   - `bi_dim_zone`
   - `bi_dim_road`
6. Create model relationships:
   - `bi_fact_traffic[road_id]` ➔ `bi_dim_road[road_id]` (Many-to-One)
   - `bi_fact_traffic[city_zone]` ➔ `bi_dim_zone[city_zone]` (Many-to-One)

---

## 🛠️ Data Engineering Challenges Tackled

1. **Out-of-Order & Late Data**: Solved using Spark Structured Streaming **15-minute event-time watermarks**, bounding memory and evicting stale state safely.
2. **At-Least-Once Kafka Duplication**: Eradicated using watermark-bounded **stateful deduplication** on `["vehicle_id", "event_ts"]`.
3. **Corrupt & Poison Pill Payloads**: Multi-tier defense with a permissive Bronze schema, safe type casting, and a Silver layer quarantine flag (`dq_flag`).
4. **Concurrent Streaming & BI Access**: Ensured via **Delta Lake ACID transactions** and snapshot isolation, eliminating dirty reads and file locking.

---

## 📝 License
This project is licensed under the Apache 2.0 License.
