-- ===================================================
-- Curated Analytical Views for Power BI / JDBC / ODBC
-- ===================================================

USE mobility;

-- Fact View: Explicitly Typed & Formatted for BI Dashboards
CREATE OR REPLACE VIEW bi_fact_traffic AS
SELECT
    CAST(event_id AS STRING)        AS event_id,
    CAST(vehicle_id AS STRING)      AS vehicle_id,
    CAST(road_id AS STRING)         AS road_id,
    CAST(city_zone AS STRING)       AS city_zone,
    CAST(speed_int AS DOUBLE)       AS speed,
    CAST(congestion_level AS INT)   AS congestion_level,
    CAST(event_ts AS TIMESTAMP)     AS event_time,
    CAST(peak_flag AS INT)          AS peak_flag,
    CAST(speed_band AS STRING)      AS speed_band,
    CAST(weather AS STRING)         AS weather,
    CAST(event_date AS DATE)        AS event_date,
    CAST(hour AS INT)               AS event_hour
FROM fact_traffic;

-- Dimension View: Urban Zones
CREATE OR REPLACE VIEW bi_dim_zone AS
SELECT
    CAST(city_zone AS STRING)       AS city_zone,
    CAST(zone_type AS STRING)       AS zone_type,
    CAST(traffic_risk AS STRING)    AS traffic_risk
FROM dim_zone;

-- Dimension View: Roads
CREATE OR REPLACE VIEW bi_dim_road AS
SELECT
    CAST(road_id AS STRING)         AS road_id,
    CAST(road_type AS STRING)       AS road_type,
    CAST(speed_limit AS INT)        AS speed_limit
FROM dim_road;

-- Quarantine Audit View: Real-Time Rejection Analytics
CREATE OR REPLACE VIEW bi_traffic_quarantine AS
SELECT
    CAST(event_id AS STRING)             AS event_id,
    CAST(vehicle_id AS STRING)           AS vehicle_id,
    CAST(road_id AS STRING)              AS road_id,
    CAST(city_zone AS STRING)            AS city_zone,
    CAST(speed AS STRING)                AS raw_speed,
    CAST(quarantine_reason AS STRING)    AS quarantine_reason,
    CAST(error_details AS STRING)        AS error_details,
    CAST(kafka_topic AS STRING)          AS kafka_topic,
    CAST(kafka_partition AS INT)         AS kafka_partition,
    CAST(kafka_offset AS BIGINT)         AS kafka_offset,
    CAST(kafka_timestamp AS TIMESTAMP)   AS kafka_timestamp,
    CAST(ingest_timestamp AS TIMESTAMP)  AS ingest_timestamp,
    CAST(rejection_timestamp AS TIMESTAMP) AS rejection_timestamp
FROM traffic_quarantine;

-- Pipeline Metrics View: Data Quality Trends & Microbatch Health
CREATE OR REPLACE VIEW bi_pipeline_metrics AS
SELECT
    CAST(pipeline_name AS STRING)          AS pipeline_name,
    CAST(batch_id AS STRING)               AS batch_id,
    CAST(processing_timestamp AS TIMESTAMP) AS processing_timestamp,
    CAST(total_records AS BIGINT)          AS total_records,
    CAST(valid_records AS BIGINT)          AS valid_records,
    CAST(quarantined_records AS BIGINT)    AS quarantined_records,
    CAST(duplicate_records AS BIGINT)      AS duplicate_records,
    CAST(late_records AS BIGINT)           AS late_records,
    CAST(rejection_reasons AS STRING)      AS rejection_reasons
FROM pipeline_metrics;
