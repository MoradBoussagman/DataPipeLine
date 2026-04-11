"""
db_writer.py
Reads processed_data from Kafka → inserts into processed_readings
Reads anomalies from Kafka      → inserts into anomalies
Every 60 rows in processed_readings → computes aggregates + AQI → inserts into metrics_batch
"""

import json
import math
import psycopg2
from kafka import KafkaConsumer

KAFKA_BROKER    = "broker:9092"
PROCESSED_TOPIC = "processed_data"
ANOMALY_TOPIC   = "anomalies"

PG_HOST = "postgres"
PG_DB   = "airquality"
PG_USER = "airflow"
PG_PASS = "airflow"

BATCH_SIZE = 60  # compute aggregates every 60 rows

# ── AQI formula (US EPA) ───────────────────────────────────
PM25_BP = [(0.0,12.0,0,50),(12.1,35.4,51,100),(35.5,55.4,101,150),(55.5,150.4,151,200),(150.5,250.4,201,300),(250.5,500.4,301,500)]
PM10_BP = [(0,54,0,50),(55,154,51,100),(155,254,101,150),(255,354,151,200),(355,424,201,300),(425,604,301,500)]
AQI_CAT = [(0,50,"Good"),(51,100,"Moderate"),(101,150,"Unhealthy for Sensitive Groups"),(151,200,"Unhealthy"),(201,300,"Very Unhealthy"),(301,500,"Hazardous")]

def aqi_for(C, bp):
    for C_low, C_high, I_low, I_high in bp:
        if C_low <= C <= C_high:
            return round((I_high - I_low) / (C_high - C_low) * (C - C_low) + I_low)
    return None

def compute_aqi(pm25, pm10):
    values = [v for v in [aqi_for(pm25, PM25_BP) if pm25 else None, aqi_for(pm10, PM10_BP) if pm10 else None] if v]
    return max(values) if values else None

def get_category(aqi):
    if aqi is None: return None
    for low, high, cat in AQI_CAT:
        if low <= aqi <= high: return cat
    return "Hazardous"

def std(values):
    if len(values) < 2: return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((x - mean) ** 2 for x in values) / len(values))

def compute_batch(conn):
    cur = conn.cursor()
    cur.execute(f"""
        SELECT pm1, pm10, pm25, rh, temperature, pm003
        FROM processed_readings
        ORDER BY measured_at DESC
        LIMIT {BATCH_SIZE}
    """)
    rows = cur.fetchall()
    if len(rows) < BATCH_SIZE:
        cur.close()
        return

    cols = ["pm1", "pm10", "pm25", "rh", "temperature", "pm003"]
    data = {c: [r[i] for r in rows if r[i] is not None] for i, c in enumerate(cols)}

    def stats(vals):
        if not vals: return None, None, None, None
        return min(vals), max(vals), round(sum(vals)/len(vals), 4), round(std(vals), 4)

    pm1_stats  = stats(data["pm1"])
    pm10_stats = stats(data["pm10"])
    pm25_stats = stats(data["pm25"])
    rh_stats   = stats(data["rh"])
    temp_stats = stats(data["temperature"])
    pm003_stats= stats(data["pm003"])

    avg_pm25 = pm25_stats[2]
    avg_pm10 = pm10_stats[2]
    aqi      = compute_aqi(avg_pm25, avg_pm10)
    aqi_cat  = get_category(aqi)

    cur.execute("""
        INSERT INTO metrics_batch (
            min_pm1, max_pm1, mean_pm1, std_pm1,
            min_pm10, max_pm10, mean_pm10, std_pm10,
            min_pm25, max_pm25, mean_pm25, std_pm25,
            min_rh, max_rh, mean_rh, std_rh,
            min_temp, max_temp, mean_temp, std_temp,
            min_pm003, max_pm003, mean_pm003, std_pm003,
            aqi, aqi_category
        ) VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,
                  %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s)
    """, (
        *pm1_stats, *pm10_stats, *pm25_stats,
        *rh_stats, *temp_stats, *pm003_stats,
        aqi, aqi_cat
    ))
    conn.commit()
    cur.close()
    print(f"✓ Batch computed: AQI={aqi} ({aqi_cat})")

def run_db_writer():
    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)

    # ── processed_data → processed_readings ───────────────
    consumer_p = KafkaConsumer(
        PROCESSED_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="db_writer_processed",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    cur = conn.cursor()
    count = 0
    for msg in consumer_p:
        d = msg.value
        cur.execute("""
            INSERT INTO processed_readings (measured_at, pm1, pm10, pm25, rh, temperature, pm003)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (d.get("measured_at"), d.get("pm1"), d.get("pm10"), d.get("pm25"),
              d.get("rh"), d.get("temperature"), d.get("pm003")))
        count += 1
    conn.commit()
    cur.close()
    consumer_p.close()
    print(f"✓ Inserted {count} rows into processed_readings")

    # ── anomalies → anomalies table ───────────────────────
    consumer_a = KafkaConsumer(
        ANOMALY_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="db_writer_anomalies",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    cur = conn.cursor()
    acount = 0
    for msg in consumer_a:
        d = msg.value
        cur.execute("""
            INSERT INTO anomalies (measured_at, pm1, pm10, pm25, rh, temperature, pm003,
                                   failed_parameter, value, threshold, severity)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (d.get("measured_at"), d.get("pm1"), d.get("pm10"), d.get("pm25"),
              d.get("rh"), d.get("temperature"), d.get("pm003"),
              d.get("failed_parameter"), d.get("value"),
              d.get("threshold"), d.get("severity")))
        acount += 1
    conn.commit()
    cur.close()
    consumer_a.close()
    print(f"✓ Inserted {acount} anomalies")

    # ── check if batch needed ──────────────────────────────
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM processed_readings")
    total = cur.fetchone()[0]
    cur.close()

    if total % BATCH_SIZE == 0 and total > 0:
        print(f"✓ {total} rows — computing batch metrics...")
        compute_batch(conn)

    conn.close()

if __name__ == "__main__":
    run_db_writer()
