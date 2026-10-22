#!/usr/bin/env bash
# ==============================================================================
# 05_init_metastore.sh - Initialize Hive Metastore tables and BI views
# ==============================================================================
set -euo pipefail

DELTA_PACKAGE="io.delta:delta-spark_2.12:3.2.0"

echo "[INFO] Running Metastore initialization SQL script via spark-sql..."
docker exec -i spark-worker /opt/spark/bin/spark-sql \
    --packages "${DELTA_PACKAGE}" \
    --conf spark.jars.ivy=/tmp/.ivy \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
    --conf spark.sql.warehouse.dir=/opt/spark/warehouse \
    -f /opt/spark-sql/init_all.sql

echo "[INFO] Hive Metastore tables and BI views created successfully."
