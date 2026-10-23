"""Data Quality Validation and Quarantine Routing Engine.

Contains pure, reusable transformation logic for validating raw Bronze traffic events,
partitioning records into clean Silver candidates and a dedicated Quarantine stream,
and calculating per-microbatch audit metrics.
"""

import json
from datetime import datetime, timezone
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    base64,
    coalesce,
    col,
    count,
    current_timestamp,
    encode,
    expr,
    hour,
    lit,
    to_date,
    to_timestamp,
    when,
)

from common.reference import (
    MAX_FUTURE_TIME_TOLERANCE_MINUTES,
    MAX_VALID_SPEED_KMH,
    MIN_VALID_SPEED_KMH,
)


def apply_data_quality_rules(bronze_df: DataFrame) -> DataFrame:
    """Evaluates Bronze records against business rules and assigns an explicit quarantine code.

    Returns a unified DataFrame enriched with:
      - typed columns (speed_int, event_ts, event_date)
      - is_valid (boolean flag)
      - quarantine_reason (string reason code if is_valid is False, else 'OK')
      - error_details (descriptive diagnosis for monitoring)
      - lossless metadata (raw_payload_bytes, raw_payload_base64)
    """
    df = bronze_df

    # 0. Defensive Metadata Column Fallbacks
    if "kafka_topic" not in df.columns:
        df = df.withColumn("kafka_topic", lit("traffic-topic"))
    if "kafka_partition" not in df.columns:
        df = df.withColumn("kafka_partition", lit(0).cast("int"))
    if "kafka_offset" not in df.columns:
        df = df.withColumn("kafka_offset", lit(0).cast("bigint"))
    if "kafka_timestamp" not in df.columns:
        df = df.withColumn("kafka_timestamp", current_timestamp())
    if "ingest_timestamp" not in df.columns:
        df = df.withColumn("ingest_timestamp", current_timestamp())
    if "kafka_key" not in df.columns:
        df = df.withColumn("kafka_key", lit(None).cast("binary"))
    if "raw_payload" not in df.columns:
        df = df.withColumn("raw_payload", lit(None).cast("string"))
    if "raw_payload_bytes" not in df.columns:
        if "raw_payload" in df.columns:
            df = df.withColumn("raw_payload_bytes", encode(col("raw_payload"), "utf-8"))
        else:
            df = df.withColumn("raw_payload_bytes", lit(None).cast("binary"))
    if "raw_payload_base64" not in df.columns:
        df = df.withColumn("raw_payload_base64", base64(col("raw_payload_bytes")))
    if "parse_status" not in df.columns:
        df = df.withColumn("parse_status", lit("SUCCESS"))
    if "_corrupt_record" not in df.columns:
        df = df.withColumn("_corrupt_record", lit(None).cast("string"))

    # 1. Safe Type Conversions
    typed_df = (
        df.withColumn("speed_int", col("speed").cast("int"))
        .withColumn("event_ts", to_timestamp("event_time"))
        .withColumn("event_date", to_date("event_ts"))
    )

    # 2. Sequential Validation & Reason Code Assignment
    validated_df = typed_df.withColumn(
        "quarantine_reason",
        # Check A: Malformed / unparseable JSON payload
        when(
            (col("parse_status") == "CORRUPT") | col("_corrupt_record").isNotNull(),
            lit("CORRUPT_JSON"),
        )
        # Check B: Missing mandatory fields
        .when(
            col("event_id").isNull() | (col("event_id") == "") |
            col("vehicle_id").isNull() | (col("vehicle_id") == "") |
            col("road_id").isNull() | (col("road_id") == "") |
            col("city_zone").isNull() | (col("city_zone") == "") |
            col("event_time").isNull() | (col("event_time") == ""),
            lit("MISSING_REQUIRED_FIELD"),
        )
        # Check C: Invalid timestamp formatting
        .when(
            col("event_ts").isNull(),
            lit("INVALID_TIMESTAMP_FORMAT"),
        )
        # Check D: Speed field contains non-numeric text
        .when(
            col("speed").isNotNull() & col("speed_int").isNull(),
            lit("INVALID_SPEED_TYPE"),
        )
        # Check E: Speed out of physical boundaries (0 - 160 km/h)
        .when(
            col("speed_int").isNull() |
            (col("speed_int") < MIN_VALID_SPEED_KMH) |
            (col("speed_int") > MAX_VALID_SPEED_KMH),
            lit("INVALID_SPEED_RANGE"),
        )
        # Check F: Clock skew / future timestamp tolerance
        .when(
            col("event_ts") > current_timestamp() + expr(f"INTERVAL {MAX_FUTURE_TIME_TOLERANCE_MINUTES} MINUTES"),
            lit("FUTURE_TIMESTAMP_SKEW"),
        )
        .otherwise(lit("OK")),
    ).withColumn(
        "is_valid",
        col("quarantine_reason") == "OK",
    ).withColumn(
        "error_details",
        when(col("quarantine_reason") == "CORRUPT_JSON", coalesce(col("_corrupt_record"), col("raw_payload")))
        .when(col("quarantine_reason") == "MISSING_REQUIRED_FIELD", lit("One or more required fields are null or empty"))
        .when(col("quarantine_reason") == "INVALID_TIMESTAMP_FORMAT", lit("Failed to parse event_time as UTC ISO-8601 timestamp"))
        .when(col("quarantine_reason") == "INVALID_SPEED_TYPE", lit("Speed payload cannot be cast to integer"))
        .when(col("quarantine_reason") == "INVALID_SPEED_RANGE", lit("Speed is null or outside permitted [0, 160] km/h bounds"))
        .when(col("quarantine_reason") == "FUTURE_TIMESTAMP_SKEW", lit("Event timestamp is more than 10 minutes in future"))
        .otherwise(lit(None).cast("string")),
    )

    return validated_df


def split_clean_and_quarantine(evaluated_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Splits evaluated records into valid Silver candidates and Quarantine rejects."""
    # Clean stream: Apply feature engineering
    clean_df = (
        evaluated_df
        .filter(col("is_valid") == True)
        .withColumn("hour", hour("event_ts"))
        .withColumn(
            "peak_flag",
            when((col("hour").between(8, 11)) | (col("hour").between(17, 20)), 1).otherwise(0),
        )
        .withColumn(
            "speed_band",
            when(col("speed_int") < 30, "LOW")
            .when(col("speed_int") < 70, "MEDIUM")
            .otherwise("HIGH"),
        )
        .select(
            "event_id",
            "vehicle_id",
            "road_id",
            "city_zone",
            "speed_int",
            "congestion_level",
            "weather",
            "event_ts",
            "event_date",
            "hour",
            "peak_flag",
            "speed_band",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingest_timestamp",
        )
    )

    # Quarantine stream: Complete lineage and rejection context
    quarantine_df = (
        evaluated_df
        .filter(col("is_valid") == False)
        .withColumn("rejection_timestamp", current_timestamp())
        .select(
            coalesce(col("event_id"), lit("UNKNOWN")).alias("event_id"),
            col("vehicle_id"),
            col("road_id"),
            col("city_zone"),
            col("speed"),
            col("raw_payload"),
            col("raw_payload_bytes"),
            col("raw_payload_base64"),
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
            col("ingest_timestamp"),
            col("rejection_timestamp"),
            col("quarantine_reason"),
            col("error_details"),
        )
    )

    return clean_df, quarantine_df


def extract_batch_metrics(
    clean_df: DataFrame,
    quarantine_df: DataFrame,
    batch_id: int | str,
    pipeline_name: str = "TrafficSilverLayer",
) -> dict[str, Any]:
    """Calculates observable audit metrics for a microbatch.

    Note on Spark Structured Streaming deduplication:
    Spark's stateful .withWatermark().dropDuplicates() manages state in an internal state
    store. Duplicates and records past the watermark are dropped at the physical plan level
    (StreamingDeduplicateExec) before downstream sinks receive rows. Therefore, metrics
    measure observable inputs: valid records accepted, records quarantined, and rejection breakdown.
    """
    valid_count = clean_df.count()
    invalid_count = quarantine_df.count()
    total_count = valid_count + invalid_count

    reason_breakdown = {}
    if invalid_count > 0:
        reasons = (
            quarantine_df
            .groupBy("quarantine_reason")
            .agg(count("*").alias("count"))
            .collect()
        )
        reason_breakdown = {row["quarantine_reason"]: row["count"] for row in reasons}

    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "pipeline_name": pipeline_name,
        "batch_id": str(batch_id),
        "processing_timestamp": now_iso,
        "total_records": total_count,
        "valid_records": valid_count,
        "quarantined_records": invalid_count,
        "rejection_reasons": reason_breakdown,
    }


def persist_batch_metrics(spark, metrics: dict[str, Any], metrics_table_path: str):
    """Idempotently writes one summary metrics row per microbatch into Delta Lake."""
    reasons_json = json.dumps(metrics.get("rejection_reasons", {}))
    escaped_reasons = reasons_json.replace("'", "''")
    pipeline_name = str(metrics.get("pipeline_name", "TrafficSilver")).replace("'", "''")
    batch_id = str(metrics.get("batch_id", "0")).replace("'", "''")
    total_records = int(metrics.get("total_records", 0))
    valid_records = int(metrics.get("valid_records", 0))
    quarantined_records = int(metrics.get("quarantined_records", 0))

    metrics_df = spark.sql(f"""
        SELECT 
            '{pipeline_name}' AS pipeline_name,
            '{batch_id}' AS batch_id,
            current_timestamp() AS processing_timestamp,
            CAST({total_records} AS BIGINT) AS total_records,
            CAST({valid_records} AS BIGINT) AS valid_records,
            CAST({quarantined_records} AS BIGINT) AS quarantined_records,
            '{escaped_reasons}' AS rejection_reasons
    """)

    if DeltaTable.isDeltaTable(spark, metrics_table_path):
        target = DeltaTable.forPath(spark, metrics_table_path)
        (
            target.alias("t")
            .merge(
                metrics_df.alias("s"),
                "t.pipeline_name = s.pipeline_name AND t.batch_id = s.batch_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            metrics_df.write
            .format("delta")
            .mode("append")
            .save(metrics_table_path)
        )
