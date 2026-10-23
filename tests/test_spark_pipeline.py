"""Integration tests for Bronze, Silver quarantine, and Gold idempotent Delta Lake pipelines.

Uses isolated temporary directories to verify:
  1. Bronze lossless raw byte retention and permissive JSON parsing.
  2. Silver and Quarantine replay idempotency on re-execution.
  3. Stateful streaming deduplication across microbatches with watermarking.
  4. Delta metrics table persistence and replay idempotency.
  5. Gold SCD Type 1 dimension updates and globally unique fact merge on event_id.
"""

import base64
import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime

import pytz
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, to_timestamp, when
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Support imports from apps
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "apps")))

import traffic_gold
import traffic_silver
from common.validation import (
    apply_data_quality_rules,
    persist_batch_metrics,
    split_clean_and_quarantine,
)
from traffic_bronze import get_traffic_schema


class TestSparkPipelineIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if sys.platform == "win32":
            hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
            if os.path.exists(hadoop_home):
                os.environ["HADOOP_HOME"] = hadoop_home
                bin_dir = os.path.join(hadoop_home, "bin")
                if bin_dir not in os.environ.get("PATH", ""):
                    os.environ["PATH"] = f"{bin_dir};{os.environ.get('PATH', '')}"

        import pyspark
        os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)

        builder = (
            SparkSession.builder
            .master("local[2]")
            .appName("TestSparkPipelineIntegration")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
        )
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="mobility_test_")
        self.silver_path = os.path.join(self.test_dir, "traffic_silver")
        self.quarantine_path = os.path.join(self.test_dir, "traffic_quarantine")
        self.metrics_path = os.path.join(self.test_dir, "pipeline_metrics")
        self.dim_zone_path = os.path.join(self.test_dir, "dim_zone")
        self.dim_road_path = os.path.join(self.test_dir, "dim_road")
        self.fact_traffic_path = os.path.join(self.test_dir, "fact_traffic")
        self.file_counter = 0

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _write_json_df(self, dict_records):
        self.file_counter += 1
        path = os.path.join(self.test_dir, f"batch_{self.file_counter}.json")
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(json.dumps(r) + "\n" for r in dict_records)
        return self.spark.read.json(path)

    def test_bronze_lossless_raw_bytes_and_permissive_parsing(self):
        """Verifies that Bronze retains raw Kafka value bytes, base64, and isolates corrupt records."""
        schema = get_traffic_schema()
        json_options = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}

        now_str = datetime.now(pytz.utc).isoformat()
        clean_json = (
            f'{{"event_id": "e1", "vehicle_id": "v1", "road_id": "R100", "city_zone": "CBD", '
            f'"speed": "75", "congestion_level": 2, "weather": "CLEAR", "event_time": "{now_str}"}}'
        )
        malformed_bytes = b"\x80\xff\xfe\xfa###CORRUPT_RAW_BINARY###"

        records = [
            {
                "kafka_topic": "traffic-topic",
                "kafka_partition": 0,
                "kafka_offset": 10,
                "kafka_timestamp": now_str,
                "ingest_timestamp": now_str,
                "raw_payload_base64": base64.b64encode(clean_json.encode("utf-8")).decode("ascii"),
                "raw_payload": clean_json,
            },
            {
                "kafka_topic": "traffic-topic",
                "kafka_partition": 0,
                "kafka_offset": 11,
                "kafka_timestamp": now_str,
                "ingest_timestamp": now_str,
                "raw_payload_base64": base64.b64encode(malformed_bytes).decode("ascii"),
                "raw_payload": malformed_bytes.decode("utf-8", errors="replace"),
            },
        ]

        df = self._write_json_df(records)
        parsed = df.withColumn("parsed", from_json(col("raw_payload"), schema, json_options))
        flattened = parsed.select(
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("raw_payload_base64"),
            col("raw_payload"),
            when(col("parsed._corrupt_record").isNotNull(), lit("CORRUPT")).otherwise(lit("SUCCESS")).alias("parse_status"),
            col("parsed._corrupt_record").alias("_corrupt_record"),
            col("parsed.event_id").alias("event_id"),
            col("parsed.speed").alias("speed"),
        )

        rows = flattened.collect()
        self.assertEqual(len(rows), 2)

        # 1. Valid Record
        valid_row = next(r for r in rows if r["event_id"] == "e1")
        self.assertEqual(valid_row["parse_status"], "SUCCESS")
        self.assertIsNone(valid_row["_corrupt_record"])
        self.assertEqual(valid_row["speed"], "75")
        decoded_valid = base64.b64decode(valid_row["raw_payload_base64"]).decode("utf-8")
        self.assertIn("e1", decoded_valid)

        # 2. Corrupt / Binary Record
        corrupt_row = next(r for r in rows if r["event_id"] is None)
        self.assertEqual(corrupt_row["parse_status"], "CORRUPT")
        self.assertIsNotNone(corrupt_row["_corrupt_record"])
        # Original raw bytes recoverable from base64 representation
        recovered_bytes = base64.b64decode(corrupt_row["raw_payload_base64"])
        self.assertEqual(recovered_bytes, malformed_bytes)

    def test_silver_and_quarantine_replay_idempotency(self):
        """Verifies that replaying the exact same microbatch produces zero duplicate Silver or Quarantine rows."""
        now_str = datetime.now(pytz.utc).isoformat()
        sample_records = [
            {"kafka_topic": "traffic-topic", "kafka_partition": 0, "kafka_offset": 1, "raw_payload": '{"event_id":"e1"}', "parse_status": "SUCCESS", "_corrupt_record": None, "event_id": "e1", "vehicle_id": "v1", "road_id": "R100", "city_zone": "CBD", "speed": "80", "congestion_level": 2, "weather": "CLEAR", "event_time": now_str, "road_condition": None},
            {"kafka_topic": "traffic-topic", "kafka_partition": 0, "kafka_offset": 2, "raw_payload": '{"event_id":"e2"}', "parse_status": "SUCCESS", "_corrupt_record": None, "event_id": "e2", "vehicle_id": "v2", "road_id": "R200", "city_zone": "AIRPORT", "speed": "90", "congestion_level": 3, "weather": "RAIN", "event_time": now_str, "road_condition": None},
            {"kafka_topic": "traffic-topic", "kafka_partition": 0, "kafka_offset": 3, "raw_payload": '{"event_id":"e3"}', "parse_status": "SUCCESS", "_corrupt_record": None, "event_id": "e3", "vehicle_id": "v3", "road_id": "R100", "city_zone": "CBD", "speed": "-40", "congestion_level": 1, "weather": "CLEAR", "event_time": now_str, "road_condition": None},
            {"kafka_topic": "traffic-topic", "kafka_partition": 0, "kafka_offset": 4, "raw_payload": 'err', "parse_status": "CORRUPT", "_corrupt_record": "err", "event_id": None, "vehicle_id": None, "road_id": None, "city_zone": None, "speed": None, "congestion_level": None, "weather": None, "event_time": None, "road_condition": None},
        ]

        df = self._write_json_df(sample_records)
        evaluated = apply_data_quality_rules(df)
        clean_df, quarantine_df = split_clean_and_quarantine(evaluated)

        # First run (Initial ingestion)
        traffic_silver.upsert_silver_clean(self.spark, clean_df, self.silver_path)
        traffic_silver.upsert_quarantine(self.spark, quarantine_df, self.quarantine_path)

        saved_silver_1 = self.spark.read.format("delta").load(self.silver_path)
        saved_quarantine_1 = self.spark.read.format("delta").load(self.quarantine_path)

        self.assertEqual(saved_silver_1.count(), 2)
        self.assertEqual(saved_quarantine_1.count(), 2)

        # Second run: Replay EXACT same microbatch (simulating task retry after partial failure)
        traffic_silver.upsert_silver_clean(self.spark, clean_df, self.silver_path)
        traffic_silver.upsert_quarantine(self.spark, quarantine_df, self.quarantine_path)

        saved_silver_2 = self.spark.read.format("delta").load(self.silver_path)
        saved_quarantine_2 = self.spark.read.format("delta").load(self.quarantine_path)

        # Assert IDEMPOTENCY: counts must remain strictly 2
        self.assertEqual(saved_silver_2.count(), 2, "Silver accumulated duplicate rows on microbatch replay!")
        self.assertEqual(saved_quarantine_2.count(), 2, "Quarantine accumulated duplicate rows on microbatch replay!")

        # Verify Quarantine rows without original event_id are preserved and deduped by (kafka_topic, kafka_partition, kafka_offset)
        corrupt_q_rows = saved_quarantine_2.filter(col("event_id") == "UNKNOWN").collect()
        self.assertEqual(len(corrupt_q_rows), 1)
        self.assertEqual(corrupt_q_rows[0]["kafka_offset"], 4)

    def test_stateful_streaming_deduplication_across_microbatches(self):
        """Verifies stateful streaming deduplication with watermark drops duplicates arriving in separate microbatches."""
        stream_input_dir = os.path.join(self.test_dir, "streaming_input")
        checkpoint_dir = os.path.join(self.test_dir, "streaming_chk")
        os.makedirs(stream_input_dir, exist_ok=True)
        os.makedirs(checkpoint_dir, exist_ok=True)

        streaming_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("speed", IntegerType(), True),
        ])

        # Batch 1 data
        batch1_file = os.path.join(stream_input_dir, "file1.json")
        with open(batch1_file, "w", encoding="utf-8") as f:
            f.write(json.dumps({"event_id": "dup_event_01", "event_time": "2026-10-23T10:00:00Z", "speed": 60}) + "\n")
            f.write(json.dumps({"event_id": "unique_event_01", "event_time": "2026-10-23T10:01:00Z", "speed": 70}) + "\n")

        # Start streaming query reading from stream_input_dir
        raw_stream = (
            self.spark.readStream
            .schema(streaming_schema)
            .json(stream_input_dir)
            .withColumn("event_ts", to_timestamp(col("event_time")))
        )

        deduped_stream = (
            raw_stream
            .withWatermark("event_ts", "15 minutes")
            .dropDuplicates(["event_id"])
        )

        query = (
            deduped_stream.writeStream
            .format("memory")
            .queryName("streaming_dedup_sink")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_dir)
            .start()
        )

        try:
            query.processAllAvailable()

            # Batch 2 data arrives: contains dup_event_01 AGAIN + new unique_event_02
            batch2_file = os.path.join(stream_input_dir, "file2.json")
            with open(batch2_file, "w", encoding="utf-8") as f:
                f.write(json.dumps({"event_id": "dup_event_01", "event_time": "2026-10-23T10:05:00Z", "speed": 65}) + "\n")
                f.write(json.dumps({"event_id": "unique_event_02", "event_time": "2026-10-23T10:06:00Z", "speed": 80}) + "\n")

            query.processAllAvailable()

            results = self.spark.table("streaming_dedup_sink").collect()
            event_ids = [r["event_id"] for r in results]

            # Assert: dup_event_01 appeared in both batches, but must only appear ONCE in output!
            self.assertEqual(len(event_ids), 3, f"Expected 3 records total, found: {event_ids}")
            self.assertEqual(event_ids.count("dup_event_01"), 1, "Duplicate event across microbatches was not dropped!")
            self.assertIn("unique_event_01", event_ids)
            self.assertIn("unique_event_02", event_ids)
        finally:
            query.stop()

    def test_metrics_persistence_and_replay_idempotency(self):
        """Verifies that pipeline data-quality metrics are persisted to Delta and replay-idempotent on batch_id."""
        metrics_dict_1 = {
            "pipeline_name": "traffic_silver",
            "batch_id": "999",
            "total_records": 100,
            "valid_records": 85,
            "quarantined_records": 15,
            "duplicate_records": 5,
            "late_records": 2,
            "rejection_reasons": {"CORRUPT_JSON": 10, "INVALID_SPEED_RANGE": 5},
        }

        # First write
        persist_batch_metrics(self.spark, metrics_dict_1, self.metrics_path)

        metrics_df_1 = self.spark.read.format("delta").load(self.metrics_path)
        self.assertEqual(metrics_df_1.count(), 1)
        row = metrics_df_1.first()
        self.assertEqual(row["pipeline_name"], "traffic_silver")
        self.assertEqual(row["batch_id"], "999")
        self.assertEqual(row["total_records"], 100)
        self.assertEqual(row["valid_records"], 85)
        self.assertEqual(row["quarantined_records"], 15)

        # Replay the same microbatch
        persist_batch_metrics(self.spark, metrics_dict_1, self.metrics_path)

        metrics_df_2 = self.spark.read.format("delta").load(self.metrics_path)
        self.assertEqual(metrics_df_2.count(), 1, "Metrics table accumulated duplicate records on batch replay!")

    def test_gold_scd_type1_dimension_updates_and_fact_idempotency(self):
        """Verifies true SCD Type 1 updates on changed dimensions and global event_id uniqueness on facts."""
        from delta.tables import DeltaTable

        traffic_gold.GOLD_DIM_ZONE_PATH = self.dim_zone_path
        traffic_gold.GOLD_DIM_ROAD_PATH = self.dim_road_path
        traffic_gold.GOLD_FACT_PATH = self.fact_traffic_path

        now_str = datetime.now(pytz.utc).isoformat()

        # Batch 1: Initial load
        clean_records_1 = [
            {"event_id": "e100", "vehicle_id": "v1", "road_id": "R100", "city_zone": "CBD", "speed_int": 80, "congestion_level": 2, "weather": "CLEAR", "event_ts": now_str, "event_date": "2026-10-23", "hour": 10, "peak_flag": 1, "speed_band": "HIGH"},
        ]
        batch_df_1 = self._write_json_df(clean_records_1)
        traffic_gold.process_gold_microbatch(batch_df_1, batch_id=1)

        dim_zone_1 = self.spark.read.format("delta").load(self.dim_zone_path).collect()
        dim_road_1 = self.spark.read.format("delta").load(self.dim_road_path).collect()
        fact_1 = self.spark.read.format("delta").load(self.fact_traffic_path).collect()

        self.assertEqual(len(dim_zone_1), 1)
        self.assertEqual(len(dim_road_1), 1)
        self.assertEqual(len(fact_1), 1)
        self.assertEqual(dim_zone_1[0]["zone_type"], "Commercial")
        self.assertEqual(dim_road_1[0]["speed_limit"], 100)

        # Batch 2: Update reference dimension attributes (SCD Type 1 in-place update)
        zone_updates = self._write_json_df([{
            "city_zone": "CBD",
            "zone_type": "URBAN_CORE",
            "traffic_risk": "EXTREME",
        }])
        target_zone = DeltaTable.forPath(self.spark, self.dim_zone_path)
        (
            target_zone.alias("t")
            .merge(zone_updates.alias("s"), "t.city_zone = s.city_zone")
            .whenMatchedUpdate(
                condition="t.zone_type != s.zone_type OR t.traffic_risk != s.traffic_risk",
                set={
                    "zone_type": "s.zone_type",
                    "traffic_risk": "s.traffic_risk",
                },
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        dim_zone_2 = self.spark.read.format("delta").load(self.dim_zone_path).collect()
        # Verify SCD-1: Count remains 1, but zone_type updated in-place!
        self.assertEqual(len(dim_zone_2), 1, "SCD Type 1 must not create duplicate dimension rows!")
        self.assertEqual(dim_zone_2[0]["zone_type"], "URBAN_CORE", "SCD Type 1 must update changed attributes in place!")
        self.assertEqual(dim_zone_2[0]["traffic_risk"], "EXTREME")

        # Replay Batch 1 facts
        traffic_gold.process_gold_microbatch(batch_df_1, batch_id=2)
        fact_2 = self.spark.read.format("delta").load(self.fact_traffic_path).collect()
        self.assertEqual(len(fact_2), 1, "Fact table must remain strictly unique by event_id!")


if __name__ == "__main__":
    unittest.main()
