"""Reference metadata and domain business rules for Mobility Analytics.

Centralizes road infrastructure classifications, urban zone risk categories,
and physical validation boundaries so they remain maintainable outside
pipeline transformation logic.
"""

# Physical and Domain Boundaries
MIN_VALID_SPEED_KMH = 0
MAX_VALID_SPEED_KMH = 160
MAX_FUTURE_TIME_TOLERANCE_MINUTES = 10
WATERMARK_DELAY_MINUTES = 15

# Road Corridor Reference Mapping
ROAD_REFERENCE = {
    "R100": {"road_type": "Highway", "speed_limit": 100},
    "R200": {"road_type": "Highway", "speed_limit": 100},
    "R300": {"road_type": "City Road", "speed_limit": 60},
    "R400": {"road_type": "City Road", "speed_limit": 60},
}

DEFAULT_ROAD_CONFIG = {"road_type": "Arterial Road", "speed_limit": 50}

# City Zone Reference Mapping
ZONE_REFERENCE = {
    "CBD": {"zone_type": "Commercial", "traffic_risk": "HIGH"},
    "AIRPORT": {"zone_type": "Transit Hub", "traffic_risk": "HIGH"},
    "TRAINSTATION": {"zone_type": "Transit Hub", "traffic_risk": "HIGH"},
    "TECHPARK": {"zone_type": "IT HUB", "traffic_risk": "MEDIUM"},
    "SUBURB": {"zone_type": "Residential", "traffic_risk": "LOW"},
}

DEFAULT_ZONE_CONFIG = {"zone_type": "General Zone", "traffic_risk": "LOW"}


def get_road_metadata(road_id: str) -> dict:
    """Returns classification and speed limit for a road corridor."""
    return ROAD_REFERENCE.get(road_id, DEFAULT_ROAD_CONFIG)


def get_zone_metadata(city_zone: str) -> dict:
    """Returns classification and risk tier for an urban zone."""
    return ZONE_REFERENCE.get(city_zone, DEFAULT_ZONE_CONFIG)
