#!/usr/bin/env bash
# ==============================================================================
# 01_create_kafka_topic.sh - Create the Kafka topic for traffic telemetry
# ==============================================================================
set -euo pipefail

TOPIC_NAME=${1:-traffic-topic}
PARTITIONS=${2:-3}
REPLICATION=${3:-1}

echo "[INFO] Creating Kafka topic: ${TOPIC_NAME} (partitions: ${PARTITIONS}, replication: ${REPLICATION})..."
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
    --create \
    --topic "${TOPIC_NAME}" \
    --bootstrap-server kafka:9092 \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}" \
    --if-not-exists

echo "[INFO] Topic created / verified successfully."
