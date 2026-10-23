"""Bronze Layer: Lossless Raw Ingestion from Kafka to Delta Lake.

Ingests raw Kafka streams retaining full partition metadata (topic, partition, offset,
Kafka timestamp), adds ingestion timestamp, preserves the complete raw payload, and
parses known schema attributes in PERMISSIVE mode with explicit _corrupt_record support.
"""

import os
import sys

from pyspark.sql.functions import (
    base64,
    col,
    current_timestamp,
    expr,
    from_json,
    lit,
    when,
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Support imports in container and local execution
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common.config import (
    BRONZE_CHECKPOINT_PATH,
    BRONZE_TABLE_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_FAIL_ON_DATA_LOSS,
    KAFKA_STARTING_OFFSETS,
    KAFKA_TOPIC,
    STREAMING_TRIGGER_SECONDS,
)
from common.spark_session import get_spark_session


def get_traffic_schema() -> StructType:
    """Returns permissive schema with corrupt record capture column."""
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("road_id", StringType(), True),
        StructField("city_zone", StringType(), True),
        StructField("speed", StringType(), True),
        StructField("congestion_level", IntegerType(), True),
        StructField("weather", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("road_condition", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])


def build_bronze_stream(spark, kafka_bootstrap=None, topic=None):
    """Builds and returns the parsed Bronze DataFrame stream."""
    bootstrap = kafka_bootstrap or KAFKA_BOOTSTRAP_SERVERS
    target_topic = topic or KAFKA_TOPIC

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", target_topic)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", str(KAFKA_FAIL_ON_DATA_LOSS).lower())
        .load()
    )

    # 1. Capture Raw Metadata & Binary Values Losslessly
    base_stream = raw_stream.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingest_timestamp"),
        col("key").alias("kafka_key"),
        col("value").alias("raw_payload_bytes"),
        base64(col("value")).alias("raw_payload_base64"),
        expr("CAST(value AS STRING)").alias("raw_payload"),
    )

    # 2. Parse JSON using PERMISSIVE mode with _corrupt_record capture
    schema = get_traffic_schema()
    json_options = {
        "mode": "PERMISSIVE",
        "columnNameOfCorruptRecord": "_corrupt_record",
    }

    parsed = base_stream.withColumn(
        "parsed",
        from_json(col("raw_payload"), schema, json_options),
    )

    # 3. Derive Parse Status & Flatten
    bronze_flattened = parsed.select(
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        col("ingest_timestamp"),
        col("kafka_key"),
        col("raw_payload_bytes"),
        col("raw_payload_base64"),
        col("raw_payload"),
        when(col("parsed._corrupt_record").isNotNull(), lit("CORRUPT"))
        .otherwise(lit("SUCCESS")).alias("parse_status"),
        col("parsed._corrupt_record").alias("_corrupt_record"),
        col("parsed.event_id").alias("event_id"),
        col("parsed.vehicle_id").alias("vehicle_id"),
        col("parsed.road_id").alias("road_id"),
        col("parsed.city_zone").alias("city_zone"),
        col("parsed.speed").alias("speed"),
        col("parsed.congestion_level").alias("congestion_level"),
        col("parsed.weather").alias("weather"),
        col("parsed.event_time").alias("event_time"),
        col("parsed.road_condition").alias("road_condition"),
    )

    return bronze_flattened


def main():
    spark = get_spark_session("TrafficBronzeIngestion")
    bronze_stream = build_bronze_stream(spark)

    query = (
        bronze_stream.writeStream
        .format("delta")
        .outputMode("append")
        .trigger(processingTime=f"{STREAMING_TRIGGER_SECONDS} seconds")
        .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
        .option("path", BRONZE_TABLE_PATH)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()