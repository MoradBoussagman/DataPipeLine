-- ============================================================
-- 01_create_tables.sql
-- Runs automatically on first Docker start
-- Creates airquality schema used by Airflow pipelines
-- ============================================================

-- Create database if missing
SELECT 'CREATE DATABASE airquality' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airquality')\gexec

\connect airquality

-- ============================================================
-- processed_metrics
-- Clean records produced by phase1.py (one row per reading)
-- ============================================================
CREATE TABLE IF NOT EXISTS processed_metrics (
    id             BIGSERIAL PRIMARY KEY,
    sensor_id      INTEGER,
    location_id    INTEGER,
    station_name   VARCHAR(255),
    parameter      VARCHAR(100),
    units          VARCHAR(50),
    value          FLOAT,
    warning_level  FLOAT,
    danger_level   FLOAT,
    severity       VARCHAR(20),
    fetched_at     TIMESTAMP,
    processed_at   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_processed_fetched_at ON processed_metrics(fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_processed_parameter  ON processed_metrics(parameter, fetched_at DESC);

-- ============================================================
-- anomalies
-- Stores anomaly context snapshots from phase1.py
-- ============================================================
CREATE TABLE IF NOT EXISTS anomalies (
    id                BIGSERIAL PRIMARY KEY,
    snapshot_id       VARCHAR(64),
    anomaly_parameter VARCHAR(100),
    anomaly_value     FLOAT,
    anomaly_warning   FLOAT,
    anomaly_danger    FLOAT,
    anomaly_severity  VARCHAR(20),
    sensor_id         INTEGER,
    location_id       INTEGER,
    station_name      VARCHAR(255),
    parameter         VARCHAR(100),
    units             VARCHAR(50),
    value             FLOAT,
    warning_level     FLOAT,
    danger_level      FLOAT,
    severity          VARCHAR(20),
    fetched_at        TIMESTAMP,
    detected_at       TIMESTAMP DEFAULT NOW(),
    threshold         FLOAT,
    measured_at       TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at ON anomalies(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_snapshot    ON anomalies(snapshot_id);

-- ============================================================
-- daily_metrics
-- Kept for daily batch DAG
-- ============================================================
CREATE TABLE IF NOT EXISTS daily_metrics (
    id            BIGSERIAL PRIMARY KEY,
    date          DATE        NOT NULL UNIQUE,
    avg_pm25      FLOAT,
    avg_pm10      FLOAT,
    avg_pm1       FLOAT,
    avg_pm03      FLOAT,
    avg_temp      FLOAT,
    avg_rh        FLOAT,
    aqi           INTEGER,
    aqi_category  VARCHAR(50),
    computed_at   TIMESTAMP   DEFAULT NOW()
);

-- ============================================================
-- thresholds
-- Static threshold reference used by processing jobs
-- ============================================================
CREATE TABLE IF NOT EXISTS thresholds (
    parameter     VARCHAR(100) PRIMARY KEY,
    warning_level FLOAT        NOT NULL,
    danger_level  FLOAT        NOT NULL,
    units         VARCHAR(50)
);

INSERT INTO thresholds VALUES
    ('pm25',              15.0,   35.0,   'ug/m3'),
    ('pm10',              45.0,   75.0,   'ug/m3'),
    ('pm1',               10.0,   25.0,   'ug/m3'),
    ('pm003',            500.0, 1000.0,   'particles/cm3'),
    ('temperature',       35.0,   40.0,   'C'),
    ('relativehumidity',  80.0,   95.0,   '%')
ON CONFLICT (parameter) DO NOTHING;

-- Grant access to airflow user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
