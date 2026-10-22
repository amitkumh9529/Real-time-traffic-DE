-- ===================================================
-- Pipeline Data Quality & Validation Queries
-- ===================================================

USE mobility;

-- Verify Fact Table Ingestion Count
SELECT COUNT(*) AS total_fact_records FROM fact_traffic;

-- Verify Quarantine Count and Top Rejection Reasons
SELECT 
    quarantine_reason, 
    COUNT(*) AS total_rejected 
FROM traffic_quarantine
GROUP BY quarantine_reason
ORDER BY total_rejected DESC;

-- Inspect Road Dimension
SELECT * FROM dim_road;

-- Inspect Zone Dimension
SELECT * FROM dim_zone;

-- Sample Fact View Aggregation
SELECT 
    city_zone, 
    road_id, 
    COUNT(*) AS total_events, 
    ROUND(AVG(speed), 2) AS avg_speed, 
    ROUND(AVG(congestion_level), 2) AS avg_congestion
FROM bi_fact_traffic
GROUP BY city_zone, road_id
ORDER BY total_events DESC;
