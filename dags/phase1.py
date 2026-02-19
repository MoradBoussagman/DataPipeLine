"""
Air Quality Pipeline - Phase 1
Flow: init_db -> create_topics -> fetch_openaq -> process_clean_data -> consume_results
Schedule: Every 1 minute
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
import requests
import json
import os
import psycopg2

# CONFIG
API_KEY = "917fed820e507d7aed1eb0bc763ac2cc1f8d1f4c9451f294a54e8fb2608f9cac"
LOCATION_ID = 2851582
KAFKA_BROKER = "broker:9092"
RAW_TOPIC = "raw_data"
PROCESSED_TOPIC = "processed_data"
ANOMALY_TOPIC = "anomalies"

PG_HOST = "postgres"
PG_DB = os.environ.get("POSTGRES_DB", "airflow")
PG_USER = os.environ.get("POSTGRES_USER", "airflow")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "airflow")

THRESHOLDS = {
    "pm25": {"warning": 15.0, "danger": 35.0},
    "pm10": {"warning": 45.0, "danger": 75.0},
    "pm1": {"warning": 10.0, "danger": 25.0},
    "pm003": {"warning": 500.0, "danger": 1000.0},
    "temperature": {"warning": 35.0, "danger": 40.0},
    "relativehumidity": {"warning": 80.0, "danger": 95.0},
}

default_args = {
    "owner": "moi",
    "start_date": datetime(2026, 2, 16),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def init_postgres_schema():
    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASSWORD)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_metrics (
            id             SERIAL PRIMARY KEY,
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

        CREATE TABLE IF NOT EXISTS anomalies (
            id                SERIAL PRIMARY KEY,
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
            threshold         FLOAT,
            measured_at       TIMESTAMP,
            detected_at       TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS thresholds (
            parameter     VARCHAR(100) PRIMARY KEY,
            warning_level FLOAT,
            danger_level  FLOAT,
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
    """)

    # Compatibility migration for already existing tables.
    cur.execute("""
        ALTER TABLE IF EXISTS processed_metrics ADD COLUMN IF NOT EXISTS station_name VARCHAR(255);
        ALTER TABLE IF EXISTS processed_metrics ADD COLUMN IF NOT EXISTS units VARCHAR(50);
        ALTER TABLE IF EXISTS processed_metrics ADD COLUMN IF NOT EXISTS value FLOAT;
        ALTER TABLE IF EXISTS processed_metrics ADD COLUMN IF NOT EXISTS warning_level FLOAT;
        ALTER TABLE IF EXISTS processed_metrics ADD COLUMN IF NOT EXISTS danger_level FLOAT;
        ALTER TABLE IF EXISTS processed_metrics ADD COLUMN IF NOT EXISTS severity VARCHAR(20);
        ALTER TABLE IF EXISTS processed_metrics ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMP;

        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS snapshot_id VARCHAR(64);
        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS anomaly_parameter VARCHAR(100);
        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS anomaly_value FLOAT;
        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS anomaly_warning FLOAT;
        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS anomaly_danger FLOAT;
        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS anomaly_severity VARCHAR(20);
        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS location_id INTEGER;
        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS station_name VARCHAR(255);
        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS units VARCHAR(50);
        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS warning_level FLOAT;
        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS danger_level FLOAT;
        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS severity VARCHAR(20);
        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMP;
        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS threshold FLOAT;
        ALTER TABLE IF EXISTS anomalies ADD COLUMN IF NOT EXISTS measured_at TIMESTAMP;

        ALTER TABLE IF EXISTS anomalies ALTER COLUMN threshold DROP NOT NULL;
        ALTER TABLE IF EXISTS anomalies ALTER COLUMN measured_at DROP NOT NULL;
        ALTER TABLE IF EXISTS anomalies ALTER COLUMN severity DROP NOT NULL;
    """)
    cur.close()
    conn.close()
    print("Schema initialized")


def create_kafka_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER,
        client_id="airflow_admin"
    )
    topics = [
        NewTopic(name=RAW_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(name=PROCESSED_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(name=ANOMALY_TOPIC, num_partitions=3, replication_factor=1),
    ]
    try:
        admin_client.create_topics(new_topics=topics, validate_only=False)
        print("Kafka topics created")
    except Exception as e:
        print(f"Topics may already exist: {e}")
    admin_client.close()


def fetch_openaq_to_kafka():
    headers = {"X-API-Key": API_KEY}

    meta = requests.get(f"https://api.openaq.org/v3/locations/{LOCATION_ID}", headers=headers, timeout=10).json()
    station = meta["results"][0]

    sensor_map = {
        s["id"]: {
            "parameter": s["parameter"]["name"],
            "display": s["parameter"]["displayName"],
            "units": s["parameter"]["units"],
        }
        for s in station["sensors"]
    }

    data = requests.get(f"https://api.openaq.org/v3/locations/{LOCATION_ID}/latest", headers=headers, timeout=10).json()
    results = data.get("results", [])

    if not results:
        raise ValueError("No results from OpenAQ API")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    published = 0
    snapshot_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    fetched_at = datetime.utcnow().isoformat()
    for r in results:
        sensor_id = r.get("sensorsId") or r.get("sensorId")
        sensor_info = sensor_map.get(sensor_id, {})
        value = r.get("value")

        if value is None or value < 0:
            continue

        message = {
            "snapshot_id": snapshot_id,
            "sensor_id": sensor_id,
            "location_id": LOCATION_ID,
            "station_name": station["name"],
            "parameter": sensor_info.get("parameter", "unknown"),
            "display_name": sensor_info.get("display", "unknown"),
            "units": sensor_info.get("units", "unknown"),
            "value": value,
            "fetched_at": fetched_at,
        }

        producer.send(RAW_TOPIC, value=message)
        published += 1

    producer.flush()
    producer.close()
    print(f"Published {published} readings to '{RAW_TOPIC}'")

def process_clean_data():
    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASSWORD)
    cur = conn.cursor()

    inserted = 0
    by_snapshot = {}
    for message in consumer:
        msg = message.value
        snapshot_id = msg.get("snapshot_id") or f"legacy-{msg.get('fetched_at') or datetime.utcnow().isoformat()}"
        parameter = (msg.get("parameter") or "").lower()
        value = msg.get("value")

        if value is None or value < 0:
            continue
        if parameter not in THRESHOLDS:
            continue

        warning = THRESHOLDS[parameter]["warning"]
        danger = THRESHOLDS[parameter]["danger"]

        severity = None
        if value >= danger:
            severity = "DANGER"
        elif value >= warning:
            severity = "WARNING"

        cleaned = {
            "snapshot_id": snapshot_id,
            "sensor_id": msg.get("sensor_id"),
            "location_id": msg.get("location_id"),
            "station_name": msg.get("station_name"),
            "parameter": parameter,
            "units": msg.get("units"),
            "value": value,
            "warning_level": warning,
            "danger_level": danger,
            "severity": severity,
            "fetched_at": msg.get("fetched_at"),
        }
        by_snapshot.setdefault(snapshot_id, []).append(cleaned)

    anomalies_inserted = 0
    for snapshot_records in by_snapshot.values():
        snapshot_anomalies = [r for r in snapshot_records if r["severity"] in ("WARNING", "DANGER")]
        if snapshot_anomalies:
            primary_anomaly = next((r for r in snapshot_anomalies if r["severity"] == "DANGER"), snapshot_anomalies[0])
            anomaly_event = {
                "snapshot_id": primary_anomaly["snapshot_id"],
                "anomaly_parameter": primary_anomaly["parameter"],
                "anomaly_value": primary_anomaly["value"],
                "anomaly_warning": primary_anomaly["warning_level"],
                "anomaly_danger": primary_anomaly["danger_level"],
                "anomaly_severity": primary_anomaly["severity"],
                "records": snapshot_records,
            }
            producer.send(ANOMALY_TOPIC, value=anomaly_event)

            for rec in snapshot_records:
                cur.execute(
                    """
                    INSERT INTO anomalies
                        (snapshot_id, anomaly_parameter, anomaly_value, anomaly_warning, anomaly_danger, anomaly_severity,
                         sensor_id, location_id, station_name, parameter, units, value, warning_level, danger_level,
                         severity, fetched_at, threshold, measured_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        primary_anomaly["snapshot_id"], primary_anomaly["parameter"], primary_anomaly["value"], primary_anomaly["warning_level"],
                        primary_anomaly["danger_level"], primary_anomaly["severity"],
                        rec["sensor_id"], rec["location_id"], rec["station_name"], rec["parameter"], rec["units"],
                        rec["value"], rec["warning_level"], rec["danger_level"], (rec["severity"] or "NORMAL"), rec["fetched_at"],
                        (primary_anomaly["danger_level"] if primary_anomaly["severity"] == "DANGER" else primary_anomaly["warning_level"]),
                        rec["fetched_at"],
                    ),
                )
                anomalies_inserted += 1
        else:
            for rec in snapshot_records:
                cur.execute(
                    """
                    INSERT INTO processed_metrics
                        (sensor_id, location_id, station_name, parameter, units, value,
                         warning_level, danger_level, severity, fetched_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        rec["sensor_id"], rec["location_id"], rec["station_name"],
                        rec["parameter"], rec["units"], rec["value"],
                        rec["warning_level"], rec["danger_level"], "NORMAL",
                        rec["fetched_at"],
                    ),
                )
                producer.send(PROCESSED_TOPIC, value={**rec, "severity": "NORMAL"})
                inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    producer.flush()
    producer.close()
    consumer.close()
    print(f"Stored {inserted} clean records in processed_metrics")
    print(f"Stored {anomalies_inserted} records in anomalies")


def consume_results():
    consumer = KafkaConsumer(
        PROCESSED_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: m.decode("utf-8")
    )

    print("=" * 60)
    print("PROCESSED CLEAN DATA:")
    print("=" * 60)

    count = 0
    for message in consumer:
        data = json.loads(message.value)
        print(f"{data.get('parameter')} | value={data.get('value')} | severity={data.get('severity')}")
        count += 1

    print("=" * 60)
    print(f"Total records consumed: {count}")
    print("=" * 60)
    consumer.close()

def consume_anomalies():
    consumer = KafkaConsumer(
        ANOMALY_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: m.decode("utf-8")
    )

    print("=" * 60)
    print("ANOMALIES WITH FULL SNAPSHOT CONTEXT:")
    print("=" * 60)

    count = 0
    for message in consumer:
        data = json.loads(message.value)
        print(
            f"anomaly={data.get('anomaly_parameter')} "
            f"severity={data.get('anomaly_severity')} "
            f"snapshot_size={len(data.get('records', []))}"
        )
        count += 1

    print("=" * 60)
    print(f"Total anomaly events consumed: {count}")
    print("=" * 60)
    consumer.close()


with DAG(
    dag_id="air_quality_pipeline",
    default_args=default_args,
    description="Phase 1: OpenAQ -> Kafka -> clean data -> PostgreSQL",
    schedule_interval="* * * * *",
    catchup=False,
    tags=["phase1", "openaq", "kafka"]
) as dag:

    task_init_db = PythonOperator(
        task_id="init_postgres_schema",
        python_callable=init_postgres_schema
    )

    task_create_topics = PythonOperator(
        task_id="create_kafka_topics",
        python_callable=create_kafka_topics
    )

    task_fetch = PythonOperator(
        task_id="fetch_openaq_to_kafka",
        python_callable=fetch_openaq_to_kafka
    )

    task_process_clean = PythonOperator(
        task_id="process_clean_data",
        python_callable=process_clean_data
    )

    task_consume_results = PythonOperator(
        task_id="consume_processed_results",
        python_callable=consume_results
    )

    task_consume_anomalies = PythonOperator(
        task_id="consume_anomalies",
        python_callable=consume_anomalies
    )

    [task_init_db, task_create_topics] >> task_fetch >> task_process_clean
    task_process_clean >> task_consume_results
    task_process_clean >> task_consume_anomalies
