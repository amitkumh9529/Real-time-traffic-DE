"""Centralized configuration for Mobility Analytics Lakehouse streaming jobs."""

import os

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic-topic")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
KAFKA_FAIL_ON_DATA_LOSS = os.getenv("KAFKA_FAIL_ON_DATA_LOSS", "true").lower() == "true"

# Spark Cluster & File Paths
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
WAREHOUSE_BASE = os.getenv("WAREHOUSE_BASE", "/opt/spark/warehouse")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", f"{WAREHOUSE_BASE}/chk")

# Streaming Trigger Intervals
STREAMING_TRIGGER_SECONDS = int(os.getenv("STREAMING_TRIGGER_SECONDS", "5"))

# Delta Lake Table Storage Paths
BRONZE_TABLE_PATH = os.getenv("BRONZE_TABLE_PATH", f"{WAREHOUSE_BASE}/traffic_bronze")
BRONZE_CHECKPOINT_PATH = os.getenv("BRONZE_CHECKPOINT_PATH", f"{CHECKPOINT_BASE}/traffic_bronze")

SILVER_TABLE_PATH = os.getenv("SILVER_TABLE_PATH", f"{WAREHOUSE_BASE}/traffic_silver")
SILVER_CHECKPOINT_PATH = os.getenv("SILVER_CHECKPOINT_PATH", f"{CHECKPOINT_BASE}/traffic_silver")

QUARANTINE_TABLE_PATH = os.getenv("QUARANTINE_TABLE_PATH", f"{WAREHOUSE_BASE}/traffic_quarantine")
QUARANTINE_CHECKPOINT_PATH = os.getenv("QUARANTINE_CHECKPOINT_PATH", f"{CHECKPOINT_BASE}/traffic_quarantine")

GOLD_FACT_PATH = os.getenv("GOLD_FACT_PATH", f"{WAREHOUSE_BASE}/fact_traffic")
GOLD_FACT_CHECKPOINT_PATH = os.getenv("GOLD_FACT_CHECKPOINT_PATH", f"{CHECKPOINT_BASE}/fact_traffic")

GOLD_DIM_ZONE_PATH = os.getenv("GOLD_DIM_ZONE_PATH", f"{WAREHOUSE_BASE}/dim_zone")
GOLD_DIM_ZONE_CHECKPOINT_PATH = os.getenv("GOLD_DIM_ZONE_CHECKPOINT_PATH", f"{CHECKPOINT_BASE}/dim_zone")

GOLD_DIM_ROAD_PATH = os.getenv("GOLD_DIM_ROAD_PATH", f"{WAREHOUSE_BASE}/dim_road")
GOLD_DIM_ROAD_CHECKPOINT_PATH = os.getenv("GOLD_DIM_ROAD_CHECKPOINT_PATH", f"{CHECKPOINT_BASE}/dim_road")

METRICS_TABLE_PATH = os.getenv("METRICS_TABLE_PATH", f"{WAREHOUSE_BASE}/pipeline_metrics")
