#!/usr/bin/env bash
# ==============================================================================
# 06_start_thrift_server.sh - Start Spark Thrift Server for Power BI / JDBC / ODBC
# ==============================================================================
set -euo pipefail

DELTA_PACKAGES="io.delta:delta-spark_2.12:3.2.0,io.delta:delta-storage:3.2.0"

echo "[INFO] Starting Spark Thrift Server on port 10000 with Delta Lake packages..."
docker exec -it spark-worker /opt/spark/sbin/start-thriftserver.sh \
    --master spark://spark-master:7077 \
    --packages "${DELTA_PACKAGES}" \
    --conf spark.jars.ivy=/tmp/.ivy \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
    --conf spark.sql.warehouse.dir=/opt/spark/warehouse

echo "[INFO] Spark Thrift Server started successfully. Listening on port 10000."
