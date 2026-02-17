"""
batch_job.py
Runs once per day at midnight via Airflow dag_daily.py
Reads yesterday's raw_readings
Computes daily averages for all 6 parameters
Computes AQI from avg_pm25 + avg_pm10
Stores one row into daily_metrics
"""

import psycopg2
from datetime import date, timedelta

PG_HOST = "postgres"
PG_DB   = "airquality"
PG_USER = "airflow"
PG_PASS = "airflow"

# ── AQI formula (US EPA) ───────────────────────────────────
PM25_BREAKPOINTS = [
    (0.0,  12.0,  0,   50),
    (12.1, 35.4,  51,  100),
    (35.5, 55.4,  101, 150),
    (55.5, 150.4, 151, 200),
    (150.5,250.4, 201, 300),
    (250.5,500.4, 301, 500),
]

PM10_BREAKPOINTS = [
    (0,   54,  0,   50),
    (55,  154, 51,  100),
    (155, 254, 101, 150),
    (255, 354, 151, 200),
    (355, 424, 201, 300),
    (425, 604, 301, 500),
]

AQI_CATEGORIES = [
    (0,   50,  "Good"),
    (51,  100, "Moderate"),
    (101, 150, "Unhealthy for Sensitive Groups"),
    (151, 200, "Unhealthy"),
    (201, 300, "Very Unhealthy"),
    (301, 500, "Hazardous"),
]

def aqi_for_pollutant(C, breakpoints):
    for (C_low, C_high, I_low, I_high) in breakpoints:
        if C_low <= C <= C_high:
            return round((I_high - I_low) / (C_high - C_low) * (C - C_low) + I_low)
    return None

def compute_aqi(pm25, pm10):
    aqi_pm25 = aqi_for_pollutant(pm25, PM25_BREAKPOINTS) if pm25 is not None else None
    aqi_pm10 = aqi_for_pollutant(pm10, PM10_BREAKPOINTS) if pm10 is not None else None

    values = [v for v in [aqi_pm25, aqi_pm10] if v is not None]
    return max(values) if values else None

def get_aqi_category(aqi):
    if aqi is None:
        return None
    for (low, high, category) in AQI_CATEGORIES:
        if low <= aqi <= high:
            return category
    return "Hazardous"

def run_batch():
    yesterday = date.today() - timedelta(days=1)
    print(f"Processing daily metrics for {yesterday}")

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur  = conn.cursor()

    # ── compute daily averages per parameter ───────────────
    cur.execute("""
        SELECT
            parameter,
            ROUND(AVG(value)::numeric, 4) as avg_value
        FROM raw_readings
        WHERE DATE(measured_at) = %s
        GROUP BY parameter
    """, (yesterday,))

    rows = cur.fetchall()

    if not rows:
        print(f"No data found for {yesterday}")
        cur.close()
        conn.close()
        return

    # ── map results to variables ───────────────────────────
    avgs = {row[0]: row[1] for row in rows}
    print(f"✓ Daily averages: {avgs}")

    avg_pm25 = avgs.get("pm25")
    avg_pm10 = avgs.get("pm10")
    avg_pm1  = avgs.get("pm1")
    avg_pm03 = avgs.get("pm003")
    avg_temp = avgs.get("temperature")
    avg_rh   = avgs.get("relativehumidity")

    # ── compute AQI ────────────────────────────────────────
    aqi          = compute_aqi(avg_pm25, avg_pm10)
    aqi_category = get_aqi_category(aqi)
    print(f"✓ AQI = {aqi} → {aqi_category}")

    # ── insert into daily_metrics ──────────────────────────
    cur.execute("""
        INSERT INTO daily_metrics
            (date, avg_pm25, avg_pm10, avg_pm1, avg_pm03, avg_temp, avg_rh, aqi, aqi_category)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
            avg_pm25     = EXCLUDED.avg_pm25,
            avg_pm10     = EXCLUDED.avg_pm10,
            avg_pm1      = EXCLUDED.avg_pm1,
            avg_pm03     = EXCLUDED.avg_pm03,
            avg_temp     = EXCLUDED.avg_temp,
            avg_rh       = EXCLUDED.avg_rh,
            aqi          = EXCLUDED.aqi,
            aqi_category = EXCLUDED.aqi_category,
            computed_at  = NOW()
    """, (yesterday, avg_pm25, avg_pm10, avg_pm1, avg_pm03, avg_temp, avg_rh, aqi, aqi_category))

    conn.commit()
    print(f"✓ Stored daily metrics for {yesterday}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_batch()
