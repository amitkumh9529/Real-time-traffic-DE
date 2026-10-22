#!/usr/bin/env bash
# ==============================================================================
# 04_submit_gold.sh - Submit Gold Layer PySpark Dimensional Modeling Job
# ==============================================================================
set -euo pipefail

DELTA_PACKAGE="io.delta:delta-spark_2.12:3.2.0"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

echo "[INFO] Submitting Gold Streaming Job to Spark cluster..."
docker exec -it spark-worker /opt/spark/bin/spark-submit \
    --conf spark.jars.ivy=/tmp/.ivy \
    --packages "${DELTA_PACKAGE},${KAFKA_PACKAGE}" \
    /opt/spark-apps/traffic_gold.py
