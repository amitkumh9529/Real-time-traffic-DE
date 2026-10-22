"""Integration tests for Bronze, Silver quarantine, and Gold idempotent Delta Lake pipelines.

Uses isolated temporary directories to verify:
  1. Bronze parsing and _corrupt_record capture.
  2. Silver valid vs. quarantine Delta table routing.
  3. Silver deduplication on event_id.
  4. Gold Delta MERGE idempotency (zero duplicates on re-execution).
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, when
from delta import configure_spark_with_delta_pip

# Support imports from apps
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "apps")))

from common.validation import apply_data_quality_rules, split_clean_and_quarantine
from traffic_bronze import get_traffic_schema


class TestSparkPipelineIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if "HADOOP_HOME" not in os.environ and os.path.exists(r"C:\hadoop"):
            os.environ["HADOOP_HOME"] = r"C:\hadoop"
        if os.path.exists(r"C:\hadoop\bin") and r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
            os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
        if "SPARK_HOME" not in os.environ or "spark-4" in os.environ.get("SPARK_HOME", ""):
            os.environ["SPARK_HOME"] = r"C:\Users\amith\AppData\Local\Programs\Python\Python314\Lib\site-packages\pyspark"
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
            for r in dict_records:
                f.write(json.dumps(r) + "\n")
        return self.spark.read.json(path)

    def _write_text_df(self, lines):
        self.file_counter += 1
        path = os.path.join(self.test_dir, f"lines_{self.file_counter}.txt")
        with open(path, "w", encoding="utf-8") as f:
            for l in lines:
                f.write(l + "\n")
        return self.spark.read.text(path).select(col("value").alias("raw_payload"))

    def test_bronze_permissive_json_parsing(self):
        """Verifies that Bronze parses valid JSON and isolates malformed payloads into _corrupt_record."""
        schema = get_traffic_schema()
        json_options = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}

        now_str = datetime.now(pytz.utc).isoformat()
        lines = [
            f'{{"event_id": "e1", "vehicle_id": "v1", "road_id": "R100", "city_zone": "CBD", "speed": "75", "congestion_level": 2, "weather": "CLEAR", "event_time": "{now_str}"}}',
            "###CORRUPTED_RAW_BYTE_STREAM###"
        ]

        df = self._write_text_df(lines)
        parsed = df.withColumn("parsed", from_json(col("raw_payload"), schema, json_options))
        flattened = parsed.select(
            col("raw_payload"),
            when(col("parsed._corrupt_record").isNotNull(), lit("CORRUPT")).otherwise(lit("SUCCESS")).alias("parse_status"),
            col("parsed._corrupt_record").alias("_corrupt_record"),
            col("parsed.event_id").alias("event_id"),
            col("parsed.speed").alias("speed")
        )

        records = flattened.collect()
        self.assertEqual(len(records), 2)

        # First record: Success
        valid_rec = next(r for r in records if r["event_id"] == "e1")
        self.assertEqual(valid_rec["parse_status"], "SUCCESS")
        self.assertIsNone(valid_rec["_corrupt_record"])
        self.assertEqual(valid_rec["speed"], "75")

        # Second record: Corrupt
        corrupt_rec = next(r for r in records if r["event_id"] is None)
        self.assertEqual(corrupt_rec["parse_status"], "CORRUPT")
        self.assertEqual(corrupt_rec["_corrupt_record"], "###CORRUPTED_RAW_BYTE_STREAM###")

    def test_silver_and_quarantine_delta_routing(self):
        """Verifies that valid events land in Silver Delta and rejected events land in Quarantine Delta."""
        now_str = datetime.now(pytz.utc).isoformat()
        sample_records = [
            {"kafka_topic": "traffic-topic", "kafka_partition": 0, "kafka_offset": 1, "raw_payload": '{"event_id":"e1"}', "parse_status": "SUCCESS", "_corrupt_record": None, "event_id": "e1", "vehicle_id": "v1", "road_id": "R100", "city_zone": "CBD", "speed": "80", "congestion_level": 2, "weather": "CLEAR", "event_time": now_str, "road_condition": None},
            {"kafka_topic": "traffic-topic", "kafka_partition": 0, "kafka_offset": 2, "raw_payload": '{"event_id":"e2"}', "parse_status": "SUCCESS", "_corrupt_record": None, "event_id": "e2", "vehicle_id": "v2", "road_id": "R100", "city_zone": "CBD", "speed": "-40", "congestion_level": 1, "weather": "CLEAR", "event_time": now_str, "road_condition": None},
            {"kafka_topic": "traffic-topic", "kafka_partition": 0, "kafka_offset": 3, "raw_payload": 'err', "parse_status": "CORRUPT", "_corrupt_record": "err", "event_id": None, "vehicle_id": None, "road_id": None, "city_zone": None, "speed": None, "congestion_level": None, "weather": None, "event_time": None, "road_condition": None},
        ]

        df = self._write_json_df(sample_records)
        evaluated = apply_data_quality_rules(df)
        clean_df, quarantine_df = split_clean_and_quarantine(evaluated)

        # Write to Delta
        clean_df.write.format("delta").mode("append").save(self.silver_path)
        quarantine_df.write.format("delta").mode("append").save(self.quarantine_path)

        # Verify Delta table contents from disk
        saved_silver = self.spark.read.format("delta").load(self.silver_path)
        saved_quarantine = self.spark.read.format("delta").load(self.quarantine_path)

        self.assertEqual(saved_silver.count(), 1)
        self.assertEqual(saved_silver.first()["event_id"], "e1")

        self.assertEqual(saved_quarantine.count(), 2)
        rejection_reasons = {r["quarantine_reason"] for r in saved_quarantine.collect()}
        self.assertIn("INVALID_SPEED_RANGE", rejection_reasons)
        self.assertIn("CORRUPT_JSON", rejection_reasons)

    def test_gold_idempotent_merge_operations(self):
        """Verifies that Delta MERGE in Gold ensures zero duplicate facts or dimensions upon re-run."""
        import traffic_gold
        traffic_gold.GOLD_DIM_ZONE_PATH = self.dim_zone_path
        traffic_gold.GOLD_DIM_ROAD_PATH = self.dim_road_path
        traffic_gold.GOLD_FACT_PATH = self.fact_traffic_path

        now_str = datetime.now(pytz.utc).isoformat()
        clean_records = [
            {"event_id": "e1", "vehicle_id": "v1", "road_id": "R100", "city_zone": "CBD", "speed_int": 80, "congestion_level": 2, "weather": "CLEAR", "event_ts": now_str, "event_date": "2026-10-23", "hour": 9, "peak_flag": 1, "speed_band": "HIGH"},
            {"event_id": "e2", "vehicle_id": "v2", "road_id": "R200", "city_zone": "TECHPARK", "speed_int": 55, "congestion_level": 3, "weather": "RAIN", "event_ts": now_str, "event_date": "2026-10-23", "hour": 9, "peak_flag": 1, "speed_band": "MEDIUM"},
        ]

        batch_df = self._write_json_df(clean_records)

        # First run
        traffic_gold.process_gold_microbatch(batch_df, batch_id=1)

        dim_zone_df1 = self.spark.read.format("delta").load(self.dim_zone_path)
        dim_road_df1 = self.spark.read.format("delta").load(self.dim_road_path)
        fact_df1 = self.spark.read.format("delta").load(self.fact_traffic_path)

        self.assertEqual(dim_zone_df1.count(), 2)
        self.assertEqual(dim_road_df1.count(), 2)
        self.assertEqual(fact_df1.count(), 2)

        # Second run with IDENTICAL batch (re-processing / replay)
        traffic_gold.process_gold_microbatch(batch_df, batch_id=2)

        dim_zone_df2 = self.spark.read.format("delta").load(self.dim_zone_path)
        dim_road_df2 = self.spark.read.format("delta").load(self.dim_road_path)
        fact_df2 = self.spark.read.format("delta").load(self.fact_traffic_path)

        # Assert IDEMPOTENCY: counts must remain exactly 2 (zero duplicate insertions!)
        self.assertEqual(dim_zone_df2.count(), 2, "dim_zone must not accumulate duplicates on replay")
        self.assertEqual(dim_road_df2.count(), 2, "dim_road must not accumulate duplicates on replay")
        self.assertEqual(fact_df2.count(), 2, "fact_traffic must not accumulate duplicates on replay")


if __name__ == "__main__":
    unittest.main()
