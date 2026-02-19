CREATE USER metabase WITH PASSWORD 'mysecretpassword';
CREATE DATABASE metabaseappdb OWNER metabase;
GRANT ALL PRIVILEGES ON DATABASE metabaseappdb TO metabase;

CREATE DATABASE mlflow;
GRANT ALL PRIVILEGES ON DATABASE mlflow TO airflow;

CREATE DATABASE airquality;
GRANT ALL PRIVILEGES ON DATABASE airquality TO airflow;

\connect airquality

CREATE TABLE IF NOT EXISTS processed_readings (
    id          BIGSERIAL PRIMARY KEY,
    measured_at TIMESTAMP NOT NULL,
    pm1         FLOAT,
    pm10        FLOAT,
    pm25        FLOAT,
    rh          FLOAT,
    temperature FLOAT,
    pm003       FLOAT
);

CREATE TABLE IF NOT EXISTS anomalies (
    id                BIGSERIAL PRIMARY KEY,
    measured_at       TIMESTAMP NOT NULL,
    pm1               FLOAT,
    pm10              FLOAT,
    pm25              FLOAT,
    rh                FLOAT,
    temperature       FLOAT,
    pm003             FLOAT,
    failed_parameter  VARCHAR(50),
    value             FLOAT,
    threshold         FLOAT,
    severity          VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS metrics_batch (
    id          BIGSERIAL PRIMARY KEY,
    computed_at TIMESTAMP DEFAULT NOW(),
    min_pm1     FLOAT, max_pm1     FLOAT, mean_pm1     FLOAT, std_pm1     FLOAT,
    min_pm10    FLOAT, max_pm10    FLOAT, mean_pm10    FLOAT, std_pm10    FLOAT,
    min_pm25    FLOAT, max_pm25    FLOAT, mean_pm25    FLOAT, std_pm25    FLOAT,
    min_rh      FLOAT, max_rh      FLOAT, mean_rh      FLOAT, std_rh      FLOAT,
    min_temp    FLOAT, max_temp    FLOAT, mean_temp    FLOAT, std_temp    FLOAT,
    min_pm003   FLOAT, max_pm003   FLOAT, mean_pm003   FLOAT, std_pm003   FLOAT,
    aqi         INTEGER,
    aqi_category VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS thresholds (
    parameter     VARCHAR(50) PRIMARY KEY,
    warning_level FLOAT NOT NULL,
    danger_level  FLOAT NOT NULL,
    units         VARCHAR(50)
);

INSERT INTO thresholds VALUES
    ('pm1',              10.0,  25.0,  'µg/m³'),
    ('pm10',             45.0,  75.0,  'µg/m³'),
    ('pm25',             15.0,  35.0,  'µg/m³'),
    ('rh',               80.0,  95.0,  '%'),
    ('temperature',      35.0,  40.0,  '°C'),
    ('pm003',           500.0, 1000.0, 'particles/cm³')
ON CONFLICT (parameter) DO NOTHING;

GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;