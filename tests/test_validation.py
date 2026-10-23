"""Unit tests for production Data Quality validation logic in apps.common.validation.

Validates business rules against real PySpark DataFrames without mirror functions:
  1. Corrupt record handling (_corrupt_record / parse_status).
  2. Required field enforcement.
  3. Physical speed bounds (0 - 160 km/h).
  4. Timestamp format parsing and future skew detection.
  5. Clean vs. quarantine routing and audit metric aggregation.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta

import pytz
from pyspark.sql import SparkSession

# Support imports from apps
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "apps")))

from common.validation import (
    apply_data_quality_rules,
    extract_batch_metrics,
    split_clean_and_quarantine,
)


class TestValidationEngine(unittest.TestCase):

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

        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("TestValidationEngine")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="val_test_")
        self.file_counter = 0

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _create_test_df(self, dict_records):
        self.file_counter += 1
        path = os.path.join(self.test_dir, f"records_{self.file_counter}.json")
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(json.dumps(r) + "\n" for r in dict_records)
        return self.spark.read.json(path)

    def test_clean_record_passes_validation(self):
        now_utc = datetime.now(pytz.utc).isoformat()
        records = [{
            "kafka_topic": "traffic-topic",
            "kafka_partition": 0,
            "kafka_offset": 100,
            "raw_payload": '{"event_id": "e1"}',
            "parse_status": "SUCCESS",
            "_corrupt_record": None,
            "event_id": "e1",
            "vehicle_id": "v1",
            "road_id": "R100",
            "city_zone": "CBD",
            "speed": "80",
            "congestion_level": 2,
            "weather": "CLEAR",
            "event_time": now_utc,
            "road_condition": None,
        }]
        df = self._create_test_df(records)
        evaluated = apply_data_quality_rules(df)
        clean, quarantine = split_clean_and_quarantine(evaluated)

        self.assertEqual(clean.count(), 1)
        self.assertEqual(quarantine.count(), 0)

        record = clean.first()
        self.assertEqual(record["event_id"], "e1")
        self.assertEqual(record["speed_int"], 80)
        self.assertEqual(record["speed_band"], "HIGH")

    def test_corrupt_json_routing(self):
        records = [{
            "kafka_topic": "traffic-topic",
            "kafka_partition": 0,
            "kafka_offset": 101,
            "raw_payload": "###CORRUPT###",
            "parse_status": "CORRUPT",
            "_corrupt_record": "###CORRUPT###",
            "event_id": None,
            "vehicle_id": None,
            "road_id": None,
            "city_zone": None,
            "speed": None,
            "congestion_level": None,
            "weather": None,
            "event_time": None,
            "road_condition": None,
        }]
        df = self._create_test_df(records)
        evaluated = apply_data_quality_rules(df)
        clean, quarantine = split_clean_and_quarantine(evaluated)

        self.assertEqual(clean.count(), 0)
        self.assertEqual(quarantine.count(), 1)
        self.assertEqual(quarantine.first()["quarantine_reason"], "CORRUPT_JSON")

    def test_missing_required_fields_routing(self):
        now_utc = datetime.now(pytz.utc).isoformat()
        records = [{
            "kafka_topic": "traffic-topic",
            "kafka_partition": 0,
            "kafka_offset": 102,
            "raw_payload": "{}",
            "parse_status": "SUCCESS",
            "_corrupt_record": None,
            "event_id": "e2",
            "vehicle_id": None,  # Missing mandatory field
            "road_id": "R100",
            "city_zone": "CBD",
            "speed": "50",
            "congestion_level": 1,
            "weather": "CLEAR",
            "event_time": now_utc,
            "road_condition": None,
        }]
        df = self._create_test_df(records)
        evaluated = apply_data_quality_rules(df)
        clean, quarantine = split_clean_and_quarantine(evaluated)

        self.assertEqual(clean.count(), 0)
        self.assertEqual(quarantine.count(), 1)
        self.assertEqual(quarantine.first()["quarantine_reason"], "MISSING_REQUIRED_FIELD")

    def test_speed_boundary_and_type_rules(self):
        now_utc = datetime.now(pytz.utc).isoformat()
        records = [
            {"kafka_topic": "traffic-topic", "kafka_partition": 0, "kafka_offset": 103, "raw_payload": "{}", "parse_status": "SUCCESS", "_corrupt_record": None, "event_id": "e3", "vehicle_id": "v3", "road_id": "R100", "city_zone": "CBD", "speed": "-40", "congestion_level": 1, "weather": "CLEAR", "event_time": now_utc, "road_condition": None},
            {"kafka_topic": "traffic-topic", "kafka_partition": 0, "kafka_offset": 104, "raw_payload": "{}", "parse_status": "SUCCESS", "_corrupt_record": None, "event_id": "e4", "vehicle_id": "v4", "road_id": "R100", "city_zone": "CBD", "speed": "420", "congestion_level": 1, "weather": "CLEAR", "event_time": now_utc, "road_condition": None},
            {"kafka_topic": "traffic-topic", "kafka_partition": 0, "kafka_offset": 105, "raw_payload": "{}", "parse_status": "SUCCESS", "_corrupt_record": None, "event_id": "e5", "vehicle_id": "v5", "road_id": "R100", "city_zone": "CBD", "speed": "FAST", "congestion_level": 1, "weather": "CLEAR", "event_time": now_utc, "road_condition": None},
        ]
        df = self._create_test_df(records)
        evaluated = apply_data_quality_rules(df)
        clean, quarantine = split_clean_and_quarantine(evaluated)

        self.assertEqual(clean.count(), 0)
        self.assertEqual(quarantine.count(), 3)

        reasons = [r["quarantine_reason"] for r in quarantine.collect()]
        self.assertIn("INVALID_SPEED_RANGE", reasons)
        self.assertIn("INVALID_SPEED_TYPE", reasons)

    def test_future_timestamp_skew_routing(self):
        future_time = (datetime.now(pytz.utc) + timedelta(minutes=45)).isoformat()
        records = [{
            "kafka_topic": "traffic-topic",
            "kafka_partition": 0,
            "kafka_offset": 106,
            "raw_payload": "{}",
            "parse_status": "SUCCESS",
            "_corrupt_record": None,
            "event_id": "e6",
            "vehicle_id": "v6",
            "road_id": "R100",
            "city_zone": "CBD",
            "speed": "60",
            "congestion_level": 1,
            "weather": "CLEAR",
            "event_time": future_time,
            "road_condition": None,
        }]
        df = self._create_test_df(records)
        evaluated = apply_data_quality_rules(df)
        clean, quarantine = split_clean_and_quarantine(evaluated)

        self.assertEqual(clean.count(), 0)
        self.assertEqual(quarantine.count(), 1)
        self.assertEqual(quarantine.first()["quarantine_reason"], "FUTURE_TIMESTAMP_SKEW")

    def test_batch_metrics_extraction(self):
        now_utc = datetime.now(pytz.utc).isoformat()
        records = [
            {"kafka_topic": "traffic-topic", "kafka_partition": 0, "kafka_offset": 1, "raw_payload": "{}", "parse_status": "SUCCESS", "_corrupt_record": None, "event_id": "e1", "vehicle_id": "v1", "road_id": "R100", "city_zone": "CBD", "speed": "60", "congestion_level": 1, "weather": "CLEAR", "event_time": now_utc, "road_condition": None},
            {"kafka_topic": "traffic-topic", "kafka_partition": 0, "kafka_offset": 2, "raw_payload": "{}", "parse_status": "CORRUPT", "_corrupt_record": "err", "event_id": None, "vehicle_id": None, "road_id": None, "city_zone": None, "speed": None, "congestion_level": None, "weather": None, "event_time": None, "road_condition": None},
        ]
        df = self._create_test_df(records)
        evaluated = apply_data_quality_rules(df)
        clean, quarantine = split_clean_and_quarantine(evaluated)
        metrics = extract_batch_metrics(clean, quarantine, batch_id=42)

        self.assertEqual(metrics["batch_id"], "42")
        self.assertEqual(metrics["total_records"], 2)
        self.assertEqual(metrics["valid_records"], 1)
        self.assertEqual(metrics["quarantined_records"], 1)
        self.assertEqual(metrics["rejection_reasons"].get("CORRUPT_JSON"), 1)


if __name__ == "__main__":
    unittest.main()
