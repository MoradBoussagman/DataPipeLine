"""
Air Quality Pipeline - Phase 1
Based on working kafka_spark_pipeline template
Flow: init_db → create_topics → fetch_openaq → spark_process → consume_results
Schedule: Every 1 minute
"""

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
import requests
import json
import os
import psycopg2

# ─── CONFIG ────────────────────────────────────────────────────────────────────
API_KEY      = "917fed820e507d7aed1eb0bc763ac2cc1f8d1f4c9451f294a54e8fb2608f9cac"
LOCATION_ID  = 2851582
KAFKA_BROKER = "broker:9092"
RAW_TOPIC        = "raw_data"
PROCESSED_TOPIC  = "processed_data"
ANOMALY_TOPIC    = "anomalies"

PG_HOST     = "postgres"
PG_DB       = os.environ.get("POSTGRES_DB", "airflow")
PG_USER     = os.environ.get("POSTGRES_USER", "airflow")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "airflow")

default_args = {
    "owner": "moi",
    "start_date": datetime(2026, 2, 16),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# ─── TASK 1: INIT POSTGRESQL ───────────────────────────────────────────────────
def init_postgres_schema():
    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASSWORD)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_readings (
            id           SERIAL PRIMARY KEY,
            sensor_id    INTEGER,
            location_id  INTEGER,
            station_name VARCHAR(255),
            parameter    VARCHAR(100),
            units        VARCHAR(50),
            value        FLOAT,
            ingested_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS processed_metrics (
            id             SERIAL PRIMARY KEY,
            sensor_id      INTEGER,
            location_id    INTEGER,
            parameter      VARCHAR(100),
            avg_value      FLOAT,
            min_value      FLOAT,
            max_value      FLOAT,
            reading_count  INTEGER,
            window_start   TIMESTAMP,
            window_end     TIMESTAMP,
            processed_at   TIMESTAMP DEFAULT NOW()
        );
        -- Drop units column if it exists from old schema
        ALTER TABLE processed_metrics DROP COLUMN IF EXISTS units;
        CREATE TABLE IF NOT EXISTS anomalies (
            id            SERIAL PRIMARY KEY,
            sensor_id     INTEGER,
            location_id   INTEGER,
            parameter     VARCHAR(100),
            value         FLOAT,
            threshold     FLOAT,
            anomaly_type  VARCHAR(100),
            detected_at   TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS thresholds (
            parameter     VARCHAR(100) PRIMARY KEY,
            warning_level FLOAT,
            danger_level  FLOAT,
            units         VARCHAR(50)
        );
        INSERT INTO thresholds VALUES
            ('pm25', 15.0,   35.0,    'µg/m3'),
            ('pm10', 45.0,   75.0,    'µg/m3'),
            ('no2',  25.0,   50.0,    'µg/m3'),
            ('o3',   60.0,   100.0,   'µg/m3'),
            ('co',   4000.0, 10000.0, 'µg/m3'),
            ('so2',  40.0,   100.0,   'µg/m3')
        ON CONFLICT (parameter) DO NOTHING;
    """)
    cur.close()
    conn.close()
    print("✓ PostgreSQL schema initialized!")


# ─── TASK 2: CREATE KAFKA TOPICS ──────────────────────────────────────────────
def create_kafka_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER,
        client_id="airflow_admin"
    )
    topics = [
        NewTopic(name=RAW_TOPIC,       num_partitions=3, replication_factor=1),
        NewTopic(name=PROCESSED_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(name=ANOMALY_TOPIC,   num_partitions=3, replication_factor=1),
    ]
    try:
        admin_client.create_topics(new_topics=topics, validate_only=False)
        print("✓ Kafka topics created!")
    except Exception as e:
        print(f"Topics may already exist: {e}")
    admin_client.close()


# ─── TASK 3: FETCH OPENAQ → KAFKA RAW_DATA ────────────────────────────────────
def fetch_openaq_to_kafka():
    headers = {"X-API-Key": API_KEY}

    meta    = requests.get(f"https://api.openaq.org/v3/locations/{LOCATION_ID}", headers=headers, timeout=10).json()
    station = meta["results"][0]

    sensor_map = {
        s["id"]: {
            "parameter": s["parameter"]["name"],
            "display":   s["parameter"]["displayName"],
            "units":     s["parameter"]["units"],
        }
        for s in station["sensors"]
    }

    data    = requests.get(f"https://api.openaq.org/v3/locations/{LOCATION_ID}/latest", headers=headers, timeout=10).json()
    results = data.get("results", [])

    if not results:
        raise ValueError("No results from OpenAQ API")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    published = 0
    for r in results:
        sensor_id   = r.get("sensorsId") or r.get("sensorId")
        sensor_info = sensor_map.get(sensor_id, {})
        value       = r.get("value")

        if value is None or value < 0:
            print(f"Skipping invalid value for sensor {sensor_id}: {value}")
            continue

        message = {
            "sensor_id":    sensor_id,
            "location_id":  LOCATION_ID,
            "station_name": station["name"],
            "parameter":    sensor_info.get("parameter", "unknown"),
            "display_name": sensor_info.get("display", "unknown"),
            "units":        sensor_info.get("units", "unknown"),
            "value":        value,
            "fetched_at":   datetime.utcnow().isoformat(),
        }

        producer.send(RAW_TOPIC, value=message)
        print(f"✓ Sent: {message['parameter']} = {value} {message['units']}")
        published += 1

    producer.flush()
    producer.close()
    print(f"✓ Published {published} readings to Kafka topic '{RAW_TOPIC}'")


# ─── TASK 5: CONSUME RESULTS FROM PROCESSED_DATA ──────────────────────────────
def consume_results():
    consumer = KafkaConsumer(
        PROCESSED_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: m.decode("utf-8")
    )

    print("=" * 60)
    print("PROCESSED METRICS FROM SPARK:")
    print("=" * 60)

    count = 0
    for message in consumer:
        data = json.loads(message.value)
        print(f"✓ {data.get('parameter')} | avg={data.get('avg_value')} | max={data.get('max_value')} | min={data.get('min_value')}")
        count += 1

    print("=" * 60)
    print(f"Total metrics consumed: {count}")
    print("=" * 60)
    consumer.close()


# ─── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="air_quality_pipeline",
    default_args=default_args,
    description="Phase 1: OpenAQ → Kafka → Spark → PostgreSQL",
    schedule_interval="* * * * *",
    catchup=False,
    tags=["phase1", "openaq", "kafka", "spark"]
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

    task_spark = SparkSubmitOperator(
        task_id="spark_process_kafka",
        application="/opt/airflow/scripts/consumer.py",
        conn_id="spark_cluster",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.5.0",
        name="air_quality_spark_job",
        verbose=True
    )

    task_consume_results = PythonOperator(
        task_id="consume_processed_results",
        python_callable=consume_results
    )

    [task_init_db, task_create_topics] >> task_fetch >> task_spark >> task_consume_results