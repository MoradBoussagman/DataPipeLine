"""
db_writer.py
Reads from Kafka processed_data and anomalies topics
Writes to PostgreSQL:
  processed_data → raw_readings table
  anomalies      → anomalies table
"""

import json
import psycopg2
from kafka import KafkaConsumer

KAFKA_BROKER    = "broker:9092"
PROCESSED_TOPIC = "processed_data"
ANOMALY_TOPIC   = "anomalies"

PG_HOST = "postgres"
PG_DB   = "airquality"
PG_USER = "airflow"
PG_PASS = "airflow"

def get_connection():
    return psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)

def write_processed(conn, msg):
    """Insert one clean reading into raw_readings"""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO raw_readings (sensor_id, parameter, value, units, measured_at)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        msg.get("sensor_id"),
        msg.get("parameter"),
        msg.get("value"),
        msg.get("units"),
        msg.get("measured_at"),
    ))
    conn.commit()
    cur.close()

def write_anomaly(conn, msg):
    """Insert one anomaly into anomalies table"""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO anomalies (sensor_id, parameter, value, threshold, severity, measured_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        msg.get("sensor_id"),
        msg.get("parameter"),
        msg.get("value"),
        msg.get("threshold"),
        msg.get("severity"),
        msg.get("measured_at"),
    ))
    conn.commit()
    cur.close()

def run_db_writer():
    conn = get_connection()

    # ── read processed_data → raw_readings ────────────────
    consumer_processed = KafkaConsumer(
        PROCESSED_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    processed_count = 0
    for message in consumer_processed:
        write_processed(conn, message.value)
        processed_count += 1

    consumer_processed.close()
    print(f"✓ Written {processed_count} readings to raw_readings")

    # ── read anomalies → anomalies table ──────────────────
    consumer_anomalies = KafkaConsumer(
        ANOMALY_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    anomaly_count = 0
    for message in consumer_anomalies:
        write_anomaly(conn, message.value)
        anomaly_count += 1

    consumer_anomalies.close()
    print(f"✓ Written {anomaly_count} anomalies to anomalies table")

    conn.close()

if __name__ == "__main__":
    run_db_writer()
