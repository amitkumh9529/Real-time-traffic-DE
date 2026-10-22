-- ===================================================
-- Register External Delta Lake Tables in Hive Metastore
-- ===================================================

USE mobility;

-- Fact Table: Streaming Traffic Events
CREATE TABLE IF NOT EXISTS fact_traffic
USING DELTA
LOCATION '/opt/spark/warehouse/fact_traffic';

-- Dimension Table: Urban City Zones
CREATE TABLE IF NOT EXISTS dim_zone
USING DELTA
LOCATION '/opt/spark/warehouse/dim_zone';

-- Dimension Table: Road Infrastructure
CREATE TABLE IF NOT EXISTS dim_road
USING DELTA
LOCATION '/opt/spark/warehouse/dim_road';

-- Quarantine Table: Rejected / Corrupt Telemetry Records
CREATE TABLE IF NOT EXISTS traffic_quarantine
USING DELTA
LOCATION '/opt/spark/warehouse/traffic_quarantine';

-- Intermediate Medallion Layers (Optional Audit Access)
CREATE TABLE IF NOT EXISTS traffic_bronze
USING DELTA
LOCATION '/opt/spark/warehouse/traffic_bronze';

CREATE TABLE IF NOT EXISTS traffic_silver
USING DELTA
LOCATION '/opt/spark/warehouse/traffic_silver';
