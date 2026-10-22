"""Gold Layer: Idempotent Star Schema Dimensional Modeling for Analytics.

Reads validated telemetry events from the Silver Delta table and utilizes foreachBatch
with Delta Lake MERGE operations to guarantee:
  1. dim_zone: Strictly unique natural key (city_zone) enriched with reference metadata.
  2. dim_road: Strictly unique natural key (road_id) enriched with reference metadata.
  3. fact_traffic: Strictly deduplicated event facts by (event_id, event_date), partitioned by event_date.

Re-running batches or restarting streaming queries produces zero duplicate facts or dimensions.
"""

import logging
import os
import sys
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from delta.tables import DeltaTable

# Support imports in container and local execution
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common.config import (
    GOLD_DIM_ROAD_CHECKPOINT_PATH,
    GOLD_DIM_ROAD_PATH,
    GOLD_DIM_ZONE_CHECKPOINT_PATH,
    GOLD_DIM_ZONE_PATH,
    GOLD_FACT_CHECKPOINT_PATH,
    GOLD_FACT_PATH,
    SILVER_TABLE_PATH,
    STREAMING_TRIGGER_SECONDS,
)
from common.reference import (
    DEFAULT_ROAD_CONFIG,
    DEFAULT_ZONE_CONFIG,
    ROAD_REFERENCE,
    ZONE_REFERENCE,
)
from common.spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [TrafficGold] %(message)s"
)
logger = logging.getLogger("TrafficGold")


def build_zone_type_expr():
    expr_col = None
    for zone, meta in ZONE_REFERENCE.items():
        if expr_col is None:
            expr_col = when(col("city_zone") == zone, lit(meta["zone_type"]))
        else:
            expr_col = expr_col.when(col("city_zone") == zone, lit(meta["zone_type"]))
    return expr_col.otherwise(lit(DEFAULT_ZONE_CONFIG["zone_type"]))


def build_traffic_risk_expr():
    expr_col = None
    for zone, meta in ZONE_REFERENCE.items():
        if expr_col is None:
            expr_col = when(col("city_zone") == zone, lit(meta["traffic_risk"]))
        else:
            expr_col = expr_col.when(col("city_zone") == zone, lit(meta["traffic_risk"]))
    return expr_col.otherwise(lit(DEFAULT_ZONE_CONFIG["traffic_risk"]))


def build_road_type_expr():
    expr_col = None
    for road, meta in ROAD_REFERENCE.items():
        if expr_col is None:
            expr_col = when(col("road_id") == road, lit(meta["road_type"]))
        else:
            expr_col = expr_col.when(col("road_id") == road, lit(meta["road_type"]))
    return expr_col.otherwise(lit(DEFAULT_ROAD_CONFIG["road_type"]))


def build_speed_limit_expr():
    expr_col = None
    for road, meta in ROAD_REFERENCE.items():
        if expr_col is None:
            expr_col = when(col("road_id") == road, lit(meta["speed_limit"]))
        else:
            expr_col = expr_col.when(col("road_id") == road, lit(meta["speed_limit"]))
    return expr_col.otherwise(lit(DEFAULT_ROAD_CONFIG["speed_limit"]))


def upsert_dim_zone(spark, microbatch_df: DataFrame):
    """Idempotently upserts city zones into dim_zone Delta table."""
    distinct_zones = (
        microbatch_df
        .select("city_zone")
        .filter(col("city_zone").isNotNull())
        .dropDuplicates(["city_zone"])
        .withColumn("zone_type", build_zone_type_expr())
        .withColumn("traffic_risk", build_traffic_risk_expr())
    )

    if distinct_zones.isEmpty():
        return

    if DeltaTable.isDeltaTable(spark, GOLD_DIM_ZONE_PATH):
        target = DeltaTable.forPath(spark, GOLD_DIM_ZONE_PATH)
        (
            target.alias("t")
            .merge(distinct_zones.alias("s"), "t.city_zone = s.city_zone")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            distinct_zones.write
            .format("delta")
            .mode("append")
            .save(GOLD_DIM_ZONE_PATH)
        )


def upsert_dim_road(spark, microbatch_df: DataFrame):
    """Idempotently upserts road corridors into dim_road Delta table."""
    distinct_roads = (
        microbatch_df
        .select("road_id")
        .filter(col("road_id").isNotNull())
        .dropDuplicates(["road_id"])
        .withColumn("road_type", build_road_type_expr())
        .withColumn("speed_limit", build_speed_limit_expr())
    )

    if distinct_roads.isEmpty():
        return

    if DeltaTable.isDeltaTable(spark, GOLD_DIM_ROAD_PATH):
        target = DeltaTable.forPath(spark, GOLD_DIM_ROAD_PATH)
        (
            target.alias("t")
            .merge(distinct_roads.alias("s"), "t.road_id = s.road_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            distinct_roads.write
            .format("delta")
            .mode("append")
            .save(GOLD_DIM_ROAD_PATH)
        )


def upsert_fact_traffic(spark, microbatch_df: DataFrame):
    """Idempotently merges event facts into fact_traffic partitioned by event_date."""
    fact_records = (
        microbatch_df
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
            "speed_band"
        )
        .dropDuplicates(["event_id"])
    )

    if fact_records.isEmpty():
        return

    if DeltaTable.isDeltaTable(spark, GOLD_FACT_PATH):
        target = DeltaTable.forPath(spark, GOLD_FACT_PATH)
        (
            target.alias("t")
            .merge(
                fact_records.alias("s"),
                "t.event_id = s.event_id AND t.event_date = s.event_date"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            fact_records.write
            .format("delta")
            .partitionBy("event_date")
            .mode("append")
            .save(GOLD_FACT_PATH)
        )


def process_gold_microbatch(microbatch_df: DataFrame, batch_id: int):
    """Orchestrates idempotent upserts across Star Schema dimensions and facts."""
    if microbatch_df.isEmpty():
        logger.info(f"Gold Microbatch {batch_id}: Empty. Skipping.")
        return

    spark = microbatch_df.sparkSession

    # 1. Upsert Dimensions
    upsert_dim_zone(spark, microbatch_df)
    upsert_dim_road(spark, microbatch_df)

    # 2. Upsert Partitioned Facts
    upsert_fact_traffic(spark, microbatch_df)

    logger.info(f"Gold Microbatch {batch_id}: Idempotent Star Schema update completed.")


def main():
    spark = get_spark_session("TrafficGoldLayer")

    silver_stream = (
        spark.readStream
        .format("delta")
        .load(SILVER_TABLE_PATH)
    )

    query = (
        silver_stream.writeStream
        .trigger(processingTime=f"{STREAMING_TRIGGER_SECONDS} seconds")
        .option("checkpointLocation", GOLD_FACT_CHECKPOINT_PATH)
        .foreachBatch(process_gold_microbatch)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()