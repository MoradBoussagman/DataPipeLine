"""
fetch.py
Fetches latest air quality readings from OpenAQ API
Sends each reading as a message to Kafka raw_data topic
Runs every minute via Airflow dag_realtime.py
"""

import json
import requests
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "broker:9092"
RAW_TOPIC    = "raw_data"
API_KEY      = "917fed820e507d7aed1eb0bc763ac2cc1f8d1f4c9451f294a54e8fb2608f9cac"
LOCATION_ID  = 2851582

def fetch_and_send():
    # ── 1. get sensor metadata (parameter names + units) ──
    headers  = {"X-API-Key": API_KEY}
    meta     = requests.get(f"https://api.openaq.org/v3/locations/{LOCATION_ID}", headers=headers, timeout=10).json()
    sensors  = {
        s["id"]: {
            "parameter": s["parameter"]["name"],
            "units":     s["parameter"]["units"],
        }
        for s in meta["results"][0]["sensors"]
    }

    # ── 2. get latest readings ─────────────────────────────
    data    = requests.get(f"https://api.openaq.org/v3/locations/{LOCATION_ID}/latest", headers=headers, timeout=10).json()
    results = data.get("results", [])

    if not results:
        print("No data returned from OpenAQ")
        return

    # ── 3. send each reading to Kafka ──────────────────────
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    sent = 0
    for r in results:
        sensor_id   = r.get("sensorsId") or r.get("sensorId")
        sensor_info = sensors.get(sensor_id, {})
        value       = r.get("value")

        if value is None or value < 0:
            continue

        message = {
            "sensor_id":   sensor_id,
            "parameter":   sensor_info.get("parameter", "unknown"),
            "units":       sensor_info.get("units", "unknown"),
            "value":       value,
            "measured_at": datetime.utcnow().isoformat(),
        }

        producer.send(RAW_TOPIC, value=message)
        print(f"✓ {message['parameter']} = {value} {message['units']}")
        sent += 1

    producer.flush()
    producer.close()
    print(f"✓ Sent {sent} readings to Kafka topic '{RAW_TOPIC}'")

if __name__ == "__main__":
    fetch_and_send()
