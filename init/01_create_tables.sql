-- ============================================================
-- 01_create_tables.sql
-- Runs automatically on first Docker start
-- Creates the airquality and metabase databases + all tables
-- ============================================================

-- Create databases
SELECT 'CREATE DATABASE airquality' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airquality')\gexec
-- SELECT 'CREATE DATABASE metabase'   WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

\connect airquality

-- ============================================================
-- raw_readings
-- Every single measurement from OpenAQ
-- Written by consumer.py every minute (real-time)
-- ============================================================
CREATE TABLE IF NOT EXISTS raw_readings (
    id           BIGSERIAL PRIMARY KEY,
    sensor_id    INTEGER,
    parameter    VARCHAR(50)  NOT NULL,   -- 'pm25', 'pm10', 'pm1', 'pm003', 'temperature', 'relativehumidity'
    value        FLOAT        NOT NULL,
    units        VARCHAR(50),
    measured_at  TIMESTAMP    NOT NULL,   -- original time from OpenAQ
    ingested_at  TIMESTAMP    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_measured_at  ON raw_readings(measured_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_parameter    ON raw_readings(parameter, measured_at DESC);

-- ============================================================
-- anomalies
-- Threshold violations detected by consumer.py (real-time)
-- ============================================================
CREATE TABLE IF NOT EXISTS anomalies (
    id           BIGSERIAL PRIMARY KEY,
    sensor_id    INTEGER,
    parameter    VARCHAR(50)  NOT NULL,
    value        FLOAT        NOT NULL,
    threshold    FLOAT        NOT NULL,
    severity     VARCHAR(20)  NOT NULL,   -- 'WARNING' or 'DANGER'
    measured_at  TIMESTAMP    NOT NULL,
    detected_at  TIMESTAMP    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at ON anomalies(detected_at DESC);

-- ============================================================
-- daily_metrics
-- One row per day — written by batch_job.py every midnight
-- Contains daily averages for all 6 parameters + AQI
-- This is the ML training table (Y = aqi, aqi_category)
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
    aqi           INTEGER,                -- computed from avg_pm25 + avg_pm10
    aqi_category  VARCHAR(50),            -- Good / Moderate / Unhealthy / etc.
    computed_at   TIMESTAMP   DEFAULT NOW()
);

-- ============================================================
-- thresholds
-- Static reference — warning and danger levels per parameter
-- Used by consumer.py to decide if a reading is an anomaly
-- ============================================================
CREATE TABLE IF NOT EXISTS thresholds (
    parameter     VARCHAR(50)  PRIMARY KEY,
    warning_level FLOAT        NOT NULL,
    danger_level  FLOAT        NOT NULL,
    units         VARCHAR(50)
);

INSERT INTO thresholds VALUES
    ('pm25',             15.0,   35.0,   'µg/m³'),
    ('pm10',             45.0,   75.0,   'µg/m³'),
    ('pm1',              10.0,   25.0,   'µg/m³'),
    ('pm003',           500.0, 1000.0,   'particles/cm³'),
    ('temperature',      35.0,   40.0,   '°C'),
    ('relativehumidity', 80.0,   95.0,   '%')
ON CONFLICT (parameter) DO NOTHING;

-- Grant access to airflow user
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
