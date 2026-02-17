"""
consumer.py
Reads from Kafka raw_data topic
Cleans and validates each message
Good reading  → sends to Kafka processed_data topic
Bad reading   → sends to Kafka anomalies topic
"""

import json
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER     = "broker:9092"
RAW_TOPIC        = "raw_data"
PROCESSED_TOPIC  = "processed_data"
ANOMALY_TOPIC    = "anomalies"

# warning and danger thresholds per parameter
THRESHOLDS = {
    "pm25":              {"warning": 15.0,   "danger": 35.0},
    "pm10":              {"warning": 45.0,   "danger": 75.0},
    "pm1":               {"warning": 10.0,   "danger": 25.0},
    "pm003":             {"warning": 500.0,  "danger": 1000.0},
    "temperature":       {"warning": 35.0,   "danger": 40.0},
    "relativehumidity":  {"warning": 80.0,   "danger": 95.0},
}

def is_valid(msg):
    """Check if a message is valid (not null, not negative)"""
    if msg.get("value") is None:
        return False, "null value"
    if msg["value"] < 0:
        return False, "negative value"
    if not msg.get("parameter"):
        return False, "missing parameter"
    return True, None

def check_anomaly(msg):
    """Check if value exceeds warning or danger threshold"""
    param      = msg["parameter"].lower()
    value      = msg["value"]
    thresholds = THRESHOLDS.get(param)

    if thresholds is None:
        return None, None

    if value >= thresholds["danger"]:
        return "DANGER", thresholds["danger"]
    if value >= thresholds["warning"]:
        return "WARNING", thresholds["warning"]
    return None, None

def run_consumer():
    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        consumer_timeout_ms=15000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    processed = 0
    anomalies = 0

    for message in consumer:
        msg = message.value

        # ── 1. validate ────────────────────────────────────
        valid, reason = is_valid(msg)
        if not valid:
            print(f"✗ Invalid message ({reason}): {msg}")
            continue

        # ── 2. check anomaly ───────────────────────────────
        severity, threshold = check_anomaly(msg)

        if severity:
            # bad → send to anomalies topic
            anomaly_msg = {
                **msg,
                "severity":  severity,
                "threshold": threshold,
            }
            producer.send(ANOMALY_TOPIC, value=anomaly_msg)
            print(f"⚠ ANOMALY {severity}: {msg['parameter']} = {msg['value']} (threshold: {threshold})")
            anomalies += 1
        else:
            # good → send to processed_data topic
            producer.send(PROCESSED_TOPIC, value=msg)
            print(f"✓ OK: {msg['parameter']} = {msg['value']} {msg['units']}")
            processed += 1

    producer.flush()
    producer.close()
    consumer.close()

    print(f"✓ Done: {processed} processed, {anomalies} anomalies")

if __name__ == "__main__":
    run_consumer()
