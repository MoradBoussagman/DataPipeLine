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

CREATE TABLE IF NOT EXISTS predictions (
    id              BIGSERIAL PRIMARY KEY,
    predicted_at    TIMESTAMP DEFAULT NOW(),
    measured_at     TIMESTAMP NOT NULL,
    pm25_predicted  FLOAT,
    pm10_predicted  FLOAT,
    pm25_actual     FLOAT,
    pm10_actual     FLOAT,
    pm25_error      FLOAT,
    pm10_error      FLOAT,
    aqi_predicted   INTEGER,
    aqi_actual      INTEGER,
    aqi_category    VARCHAR(50),
    model_version   VARCHAR(50)
);

GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;

-- ─────────────────────────────────────────────────────────────────────────────
--  DATA CATALOG TABLES  (Phase 4)
-- ─────────────────────────────────────────────────────────────────────────────

-- 1. Assets: every data asset in the platform
CREATE TABLE IF NOT EXISTS catalog_assets (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL UNIQUE,
    type        VARCHAR(50)  NOT NULL,   -- 'postgresql_table', 'kafka_topic', 'ml_model', 'api'
    description TEXT,
    owner       VARCHAR(100),
    source      VARCHAR(100),            -- 'OpenAQ API', 'Kafka', 'MLflow'...
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW()
);

-- 2. Columns: fields inside each table asset
CREATE TABLE IF NOT EXISTS catalog_columns (
    id          SERIAL PRIMARY KEY,
    asset_id    INTEGER REFERENCES catalog_assets(id) ON DELETE CASCADE,
    column_name VARCHAR(100) NOT NULL,
    data_type   VARCHAR(50),
    description TEXT,
    unit        VARCHAR(50),
    nullable    BOOLEAN DEFAULT TRUE,
    UNIQUE (asset_id, column_name)
);

-- 3. Lineage: which asset feeds which
CREATE TABLE IF NOT EXISTS catalog_lineage (
    id           SERIAL PRIMARY KEY,
    source_asset VARCHAR(100) NOT NULL,
    target_asset VARCHAR(100) NOT NULL,
    transform    VARCHAR(200),           -- description of the transformation
    created_at   TIMESTAMP DEFAULT NOW()
);

-- 4. Quality snapshots: freshness + completeness per table (filled by DAG)
CREATE TABLE IF NOT EXISTS catalog_quality (
    id              SERIAL PRIMARY KEY,
    asset_name      VARCHAR(100) NOT NULL,
    checked_at      TIMESTAMP DEFAULT NOW(),
    row_count       BIGINT,
    null_pct        FLOAT,               -- average null % across columns
    freshness_mins  FLOAT,               -- minutes since last record
    status          VARCHAR(20)          -- 'fresh', 'stale', 'empty'
);

-- ─────────────────────────────────────────────
--  SEED: static metadata about your assets
-- ─────────────────────────────────────────────

INSERT INTO catalog_assets (name, type, description, owner, source) VALUES
('processed_readings',       'postgresql_table', 'Cleaned sensor readings ingested from OpenAQ API via Kafka and Spark ETL',                        'data_analyst', 'OpenAQ API → Kafka → Spark'),
('anomalies',                'postgresql_table', 'Records flagged as anomalies by threshold-based business rules during streaming',                  'data_analyst', 'processed_readings'),
('metrics_batch',            'postgresql_table', 'Daily aggregated statistics (min/max/mean/std/AQI) computed by the batch DAG',                    'data_analyst', 'processed_readings'),
('thresholds',               'postgresql_table', 'Static warning and danger thresholds per air quality parameter',                                  'data_analyst', 'manual'),
('predictions',              'postgresql_table', 'ML model predictions vs actuals for pm25 and pm10, stored by FastAPI service',                   'data_analyst', 'MLflow model + FastAPI'),
('training_log',             'postgresql_table', 'Records each model training run: timestamp and number of rows used. Used by dag_phase2 sensor to decide when to retrain.', 'data_analyst', 'dag_phase2'),
('raw_data',                 'kafka_topic',      'Raw JSON messages from OpenAQ API, one message per sensor reading cycle',                        'data_analyst', 'OpenAQ API'),
('processed_data',           'kafka_topic',      'Validated and cleaned messages after consumer routing',                                           'data_analyst', 'consumer_.py / Spark consumer.py'),
('anomalies_topic',          'kafka_topic',      'Anomaly events published when a reading exceeds warning/danger thresholds',                      'data_analyst', 'consumer_.py / Spark consumer.py'),
('AirQuality_AQI_predictor', 'ml_model',         'Multi-output regression model predicting pm25 and pm10 at H+1, trained with MLflow',             'data_analyst', 'MLflow Registry'),
('FastAPI Prediction Service','api',             'REST API exposing /predictions and /latest endpoints, runs background prediction loop',          'data_analyst', 'FastAPI + MLflow')
ON CONFLICT (name) DO NOTHING;

-- Columns for processed_readings
INSERT INTO catalog_columns (asset_id, column_name, data_type, description, unit, nullable)
SELECT a.id, c.col, c.dtype, c.description, c.unit, c.nullable
FROM catalog_assets a,
(VALUES
    ('id',          'BIGSERIAL', 'Auto-increment primary key',           NULL,      false),
    ('measured_at', 'TIMESTAMP', 'Timestamp of the sensor measurement',  NULL,      false),
    ('pm1',         'FLOAT',     'Particulate matter < 1µm',             'µg/m³',   true),
    ('pm10',        'FLOAT',     'Particulate matter < 10µm',            'µg/m³',   true),
    ('pm25',        'FLOAT',     'Particulate matter < 2.5µm',           'µg/m³',   true),
    ('rh',          'FLOAT',     'Relative humidity',                    '%',       true),
    ('temperature', 'FLOAT',     'Ambient temperature',                  '°C',      true),
    ('pm003',       'FLOAT',     'Particle count > 0.3µm',               'p/cm³',   true)
) AS c(col, dtype, description, unit, nullable)
WHERE a.name = 'processed_readings'
ON CONFLICT DO NOTHING;

-- Columns for anomalies
INSERT INTO catalog_columns (asset_id, column_name, data_type, description, unit, nullable)
SELECT a.id, c.col, c.dtype, c.description, c.unit, c.nullable
FROM catalog_assets a,
(VALUES
    ('id',               'BIGSERIAL',   'Auto-increment primary key',         NULL,    false),
    ('measured_at',      'TIMESTAMP',   'Timestamp of the anomalous reading', NULL,    false),
    ('failed_parameter', 'VARCHAR(50)', 'Which parameter triggered the alert',NULL,    true),
    ('value',            'FLOAT',       'Actual measured value',              NULL,    true),
    ('threshold',        'FLOAT',       'Threshold that was exceeded',        NULL,    true),
    ('severity',         'VARCHAR(20)', 'warning or danger',                  NULL,    true)
) AS c(col, dtype, description, unit, nullable)
WHERE a.name = 'anomalies'
ON CONFLICT DO NOTHING;

-- Lineage
INSERT INTO catalog_lineage (source_asset, target_asset, transform) VALUES
('raw_data',                   'processed_data',              'consumer_.py / Spark consumer.py: valid readings sent to processed_data Kafka topic'),
('raw_data',                   'anomalies_topic',             'consumer_.py / Spark consumer.py: readings exceeding thresholds routed to anomalies Kafka topic'),
('processed_data',             'processed_readings',          'db_writer.py: reads processed_data Kafka topic and inserts cleaned rows into PostgreSQL'),
('anomalies_topic',            'anomalies',                   'db_writer.py: reads anomalies Kafka topic and inserts anomalous rows into PostgreSQL'),
('processed_readings',         'metrics_batch',               'db_writer.py: every 60 new rows triggers compute_batch() which aggregates min/max/mean/std + AQI'),
('processed_readings',         'AirQuality_AQI_predictor',    'dag_phase2: NewDataSensor waits for 500+ new rows then trains MLflow model from processed_readings'),
('processed_readings',         'training_log',                'dag_phase2 log_training(): SELECT COUNT(*) from processed_readings and inserts rows_used into training_log'),
('processed_readings',         'FastAPI Prediction Service',  'FastAPI get_last_rows(6): reads last 6 rows as feature vector; wait_for_new_row() reads actuals to compute error'),
('AirQuality_AQI_predictor',   'FastAPI Prediction Service',  'FastAPI loads @production model alias from MLflow registry on startup'),
('AirQuality_AQI_predictor',   'training_log',                'dag_phase2 log_training(): runs after train_model task completes, records rows_used for this training run'),
('FastAPI Prediction Service', 'predictions',                 'FastAPI background loop: predicts pm25/pm10 at H+1, waits for actual reading, stores both in predictions'),
('thresholds',                 'anomalies_topic',             'Consumer scripts use threshold values defined in this table (hardcoded in Python) to route anomalous readings'),
('thresholds',                 'anomalies',                   'Threshold values defined in this table are stored alongside each anomaly record for traceability')
ON CONFLICT DO NOTHING;

GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
