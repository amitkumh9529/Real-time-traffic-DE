"""Production-minded Traffic Telemetry Producer for Mobility Analytics.

Generates realistic high-frequency vehicular telemetry with deterministic seeding,
explicit event identifiers (event_id), true duplicate re-transmissions, and
unwrapped raw malformed byte payloads for defensive streaming pipeline verification.
"""

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Any

import pytz
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("TrafficProducer")

ROADS = ["R100", "R200", "R300", "R400"]
ZONES = ["CBD", "AIRPORT", "TECHPARK", "SUBURB", "TRAINSTATION"]
WEATHER = ["CLEAR", "RAIN", "FOG", "STORM"]
ANOMALY_TYPES = [
    "duplicate",
    "malformed_json",
    "missing_field",
    "negative_speed",
    "extreme_speed",
    "wrong_datatype",
    "late_event",
    "future_event",
    "schema_drift",
]


class TrafficEventGenerator:
    """Deterministic event generator for mobility telemetry events."""

    def __init__(self, seed: int | None = None, base_time: datetime | None = None):
        self.seed = seed
        self.rng = random.Random(seed)
        self.fake = Faker()
        if seed is not None:
            self.fake.seed_instance(seed)
            Faker.seed(seed)

        self.base_time = base_time or (
            datetime(2026, 10, 23, 0, 0, 0, tzinfo=pytz.utc) if seed is not None
            else None
        )
        self.step_counter = 0
        self.vehicle_pool: list[str] = [self.fake.uuid4() for _ in range(50)]
        self.recent_events_cache: list[dict[str, Any]] = []
        self.max_cache_size = 200

    def _get_current_time(self) -> datetime:
        if self.base_time is not None:
            self.step_counter += 1
            return self.base_time + timedelta(seconds=self.step_counter)
        return datetime.now(pytz.utc)

    def generate_clean_event(self) -> dict[str, Any]:
        """Generates a fully valid telemetry event conforming to the data contract."""
        event_id = self.fake.uuid4()
        vehicle_id = self.rng.choice(self.vehicle_pool)
        
        event = {
            "event_id": event_id,
            "vehicle_id": vehicle_id,
            "road_id": self.rng.choice(ROADS),
            "city_zone": self.rng.choice(ZONES),
            "speed": self.rng.randint(20, 100),
            "congestion_level": self.rng.randint(1, 5),
            "weather": self.rng.choice(WEATHER),
            "event_time": self._get_current_time().isoformat()
        }

        self.recent_events_cache.append(dict(event))
        if len(self.recent_events_cache) > self.max_cache_size:
            self.recent_events_cache.pop(0)

        return event

    def generate_dirty_event(self, anomaly_type: str | None = None) -> dict[str, Any] | bytes:
        """Generates an anomalous payload adhering to one of 9 edge cases.
        
        Returns:
            dict[str, Any] for structural anomalies, or raw bytes for malformed payloads.
        """
        chosen_type = anomaly_type or self.rng.choice(ANOMALY_TYPES)

        # 1. True Duplicates: exact same event_id and payload
        if chosen_type == "duplicate":
            if self.recent_events_cache:
                return dict(self.rng.choice(self.recent_events_cache))
            base = self.generate_clean_event()
            return dict(base)

        # 2. Truly Malformed JSON Bytes (NOT wrapped in JSON)
        if chosen_type == "malformed_json":
            return b"###CORRUPTED_RAW_TELEMETRY_PAYLOAD_NON_JSON###"

        base = self.generate_clean_event()

        # 3. Missing Required Fields
        if chosen_type == "missing_field":
            field_to_drop = self.rng.choice(["vehicle_id", "event_time", "road_id", "city_zone"])
            base[field_to_drop] = None
            return base

        # 4. Negative Speeds (sensor calibration dropouts)
        if chosen_type == "negative_speed":
            base["speed"] = -40
            return base

        # 5. Outlier Velocity Spikes
        if chosen_type == "extreme_speed":
            base["speed"] = 420
            return base

        # 6. Type Incoherence (string in numeric field)
        if chosen_type == "wrong_datatype":
            base["speed"] = "FAST"
            return base

        # 7. Late-Arriving Data (exceeding 15-minute watermark threshold)
        if chosen_type == "late_event":
            late_delta = timedelta(minutes=self.rng.randint(20, 120))
            base["event_time"] = (self._get_current_time() - late_delta).isoformat()
            return base

        # 8. Future Timestamp Clock Skew (exceeding 10-minute tolerance)
        if chosen_type == "future_event":
            future_delta = timedelta(minutes=self.rng.randint(15, 60))
            base["event_time"] = (self._get_current_time() + future_delta).isoformat()
            return base

        # 9. Schema Drift (unexpected novel fields)
        if chosen_type == "schema_drift":
            base["road_condition"] = self.rng.choice(["GOOD", "POTHOLES", "UNDER_CONSTRUCTION"])
            base["sensor_firmware_version"] = "v2.4.0-rc1"
            return base

        return base

    def next_event(self, clean_ratio: float = 0.70) -> tuple[dict[str, Any] | bytes, str]:
        """Emits the next payload with classification."""
        if self.rng.random() < clean_ratio:
            return self.generate_clean_event(), "clean"
        dirty_type = self.rng.choice(ANOMALY_TYPES)
        return self.generate_dirty_event(dirty_type), dirty_type


def serialize_payload(event: dict[str, Any] | bytes) -> bytes:
    """Serializes event to bytes. Preserves raw bytes without JSON wrapping."""
    if isinstance(event, bytes):
        return event
    return json.dumps(event).encode("utf-8")


def get_kafka_producer(bootstrap_servers: str, retries: int = 5, delay_seconds: int = 3):
    """Establishes connection to Kafka broker with exponential backoff."""
    try:
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
    except ImportError:
        logger.error("kafka-python-ng is not installed.")
        sys.exit(1)

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Connecting to Kafka at {bootstrap_servers} (Attempt {attempt}/{retries})...")
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=serialize_payload
            )
        except (KafkaError, OSError, ConnectionRefusedError, TimeoutError) as e:
            logger.warning(f"Kafka unavailable ({e}). Retrying in {delay_seconds}s...")
            time.sleep(delay_seconds)

    logger.error("Could not connect to Kafka. Exiting producer.")
    sys.exit(1)


def run_producer(
    bootstrap_servers: str = "localhost:29092",
    topic: str = "traffic-topic",
    seed: int | None = None,
    delay_min: float = 0.5,
    delay_max: float = 1.5,
    max_events: int | None = None
):
    """Continuously generates and streams telemetry events to Kafka."""
    generator = TrafficEventGenerator(seed=seed)
    producer = get_kafka_producer(bootstrap_servers)

    def handle_shutdown(signum, frame):
        logger.info("Shutdown signal received. Flushing Kafka producer...")
        producer.flush()
        producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    logger.info(f"Starting traffic producer on topic '{topic}' (seed={seed})...")
    events_sent = 0

    while True:
        try:
            event, event_type = generator.next_event()
            producer.send(topic, value=event)

            if isinstance(event, bytes):
                logger.warning(f"[SENT {events_sent+1}] Raw Malformed Bytes ({len(event)} bytes)")
            else:
                logger.info(f"[SENT {events_sent+1}] Type: {event_type} | ID: {event.get('event_id', '')[:8]} | Road: {event.get('road_id')} | Speed: {event.get('speed')}")

            events_sent += 1
            if max_events and events_sent >= max_events:
                logger.info(f"Reached max_events ({max_events}). Stopping producer.")
                break

            time.sleep(random.uniform(delay_min, delay_max))
        except (OSError, RuntimeError, ValueError) as e:
            logger.error(f"Error publishing event: {e}")
            time.sleep(1)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mobility Analytics Traffic Telemetry Producer")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "traffic-topic"))
    parser.add_argument("--seed", type=int, default=os.getenv("PRODUCER_SEED"))
    parser.add_argument("--max-events", type=int, default=None)
    args = parser.parse_args()

    run_producer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        seed=args.seed,
        max_events=args.max_events
    )