"""
fetch.py
Fetches latest readings from OpenAQ API
Combines all 6 parameters into ONE message
Sends to Kafka raw_data topic
"""

import json
import requests
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "broker:9092"
RAW_TOPIC    = "raw_data"
API_KEY      = "917fed820e507d7aed1eb0bc763ac2cc1f8d1f4c9451f294a54e8fb2608f9cac"
LOCATION_ID  = 2851582

# maps OpenAQ parameter names to our column names
PARAM_MAP = {
    "pm1":               "pm1",
    "pm10":              "pm10",
    "pm25":              "pm25",
    "relativehumidity":  "rh",
    "temperature":       "temperature",
    "um003":             "pm003", # api i think sends um not pm
}

def fetch_and_send():
    headers = {"X-API-Key": API_KEY}

    # get sensor metadata
    meta    = requests.get(f"https://api.openaq.org/v3/locations/{LOCATION_ID}", headers=headers, timeout=10).json()
    sensors = {
        s["id"]: s["parameter"]["name"]
        for s in meta["results"][0]["sensors"]
    }

    # get latest readings
    data    = requests.get(f"https://api.openaq.org/v3/locations/{LOCATION_ID}/latest", headers=headers, timeout=10).json()
    results = data.get("results", [])

    if not results:
        raise ValueError("No results from OpenAQ API")

    # combine all 6 into one row
    row = {"measured_at": datetime.utcnow().isoformat()}
    for r in results:
        sensor_id  = r.get("sensorsId") or r.get("sensorId")
        param_name = sensors.get(sensor_id, "")
        col_name   = PARAM_MAP.get(param_name)
        value      = r.get("value")
        if col_name and value is not None:
            row[col_name] = value

    print(f"✓ Combined row: {row}")

    # send ONE message to Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    producer.send(RAW_TOPIC, value=row)
    producer.flush()
    producer.close()
    print(f"✓ Sent 1 combined row to Kafka '{RAW_TOPIC}'")

if __name__ == "__main__":
    fetch_and_send()
