#!/usr/bin/env bash
# ==============================================================================
# 02_submit_bronze.sh - Submit Bronze Layer PySpark Structured Streaming Job
# ==============================================================================
set -euo pipefail

DELTA_PACKAGE="io.delta:delta-spark_2.12:3.2.0"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

echo "[INFO] Submitting Bronze Streaming Job to Spark cluster..."
docker exec -it spark-worker /opt/spark/bin/spark-submit \
    --conf spark.jars.ivy=/tmp/.ivy \
    --packages "${DELTA_PACKAGE},${KAFKA_PACKAGE}" \
    /opt/spark-apps/traffic_bronze.py
