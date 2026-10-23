"""Unit tests for Traffic Telemetry Producer.

Verifies:
  1. Deterministic seeding reproducibility.
  2. Data contract conformance of clean events.
  3. Correctness of all 9 edge anomaly classes.
  4. Raw byte serialization of malformed payloads.
"""

import json
import unittest
from datetime import datetime

import pytz

from producer.traffic_dirty_producer import (
    ROADS,
    WEATHER,
    ZONES,
    TrafficEventGenerator,
    serialize_payload,
)


class TestTrafficProducer(unittest.TestCase):

    def setUp(self):
        self.generator = TrafficEventGenerator(seed=12345)

    def test_deterministic_seeding(self):
        """Verifies that two generators initialized with the same seed emit identical sequences."""
        gen1 = TrafficEventGenerator(seed=42)
        gen2 = TrafficEventGenerator(seed=42)

        events1 = [gen1.generate_clean_event() for _ in range(5)]
        events2 = [gen2.generate_clean_event() for _ in range(5)]

        self.assertEqual(events1, events2)

    def test_clean_event_contract(self):
        """Verifies clean events meet all mandatory schema constraints."""
        event = self.generator.generate_clean_event()

        self.assertIn("event_id", event)
        self.assertTrue(len(event["event_id"]) > 0)
        self.assertIn("vehicle_id", event)
        self.assertIn(event["road_id"], ROADS)
        self.assertIn(event["city_zone"], ZONES)
        self.assertIn(event["weather"], WEATHER)
        self.assertTrue(20 <= event["speed"] <= 100)
        self.assertTrue(1 <= event["congestion_level"] <= 5)

        # Parseable UTC ISO timestamp
        dt = datetime.fromisoformat(event["event_time"])
        self.assertIsNotNone(dt.tzinfo)

    def test_anomaly_malformed_json_bytes(self):
        """Verifies malformed JSON is returned as raw un-wrapped bytes."""
        payload = self.generator.generate_dirty_event("malformed_json")
        self.assertIsInstance(payload, bytes)
        with self.assertRaises(json.JSONDecodeError):
            json.loads(payload.decode("utf-8", errors="ignore"))

    def test_anomaly_duplicate_event(self):
        """Verifies duplicate events reuse identical event_id and payload."""
        # Prime cache
        _ = [self.generator.generate_clean_event() for _ in range(10)]
        duplicate = self.generator.generate_dirty_event("duplicate")
        self.assertIn("event_id", duplicate)
        self.assertIn(duplicate, self.generator.recent_events_cache)

    def test_anomaly_missing_required_field(self):
        """Verifies missing_field drops a required attribute."""
        event = self.generator.generate_dirty_event("missing_field")
        dropped_fields = [k for k in ["vehicle_id", "event_time", "road_id", "city_zone"] if event.get(k) is None]
        self.assertTrue(len(dropped_fields) >= 1)

    def test_anomaly_negative_speed(self):
        event = self.generator.generate_dirty_event("negative_speed")
        self.assertEqual(event["speed"], -40)

    def test_anomaly_extreme_speed(self):
        event = self.generator.generate_dirty_event("extreme_speed")
        self.assertEqual(event["speed"], 420)

    def test_anomaly_wrong_datatype(self):
        event = self.generator.generate_dirty_event("wrong_datatype")
        self.assertEqual(event["speed"], "FAST")

    def test_anomaly_late_event(self):
        """Late event timestamp is at least 15 minutes in past."""
        event = self.generator.generate_dirty_event("late_event")
        event_dt = datetime.fromisoformat(event["event_time"])
        anchor = self.generator.base_time or datetime.now(pytz.utc)
        self.assertTrue((anchor - event_dt).total_seconds() >= 15 * 60)

    def test_anomaly_future_event(self):
        """Future event timestamp is at least 10 minutes in future."""
        event = self.generator.generate_dirty_event("future_event")
        event_dt = datetime.fromisoformat(event["event_time"])
        anchor = self.generator.base_time or datetime.now(pytz.utc)
        self.assertTrue((event_dt - anchor).total_seconds() >= 10 * 60)

    def test_anomaly_schema_drift(self):
        """Schema drift introduces non-standard attributes."""
        event = self.generator.generate_dirty_event("schema_drift")
        self.assertIn("road_condition", event)
        self.assertIn("sensor_firmware_version", event)

    def test_payload_serialization(self):
        raw_bytes = b"non_json_bytes"
        self.assertEqual(serialize_payload(raw_bytes), raw_bytes)

        clean_dict = {"event_id": "e1", "speed": 50}
        serialized = serialize_payload(clean_dict)
        self.assertIsInstance(serialized, bytes)
        self.assertEqual(json.loads(serialized.decode("utf-8")), clean_dict)


if __name__ == "__main__":
    unittest.main()
