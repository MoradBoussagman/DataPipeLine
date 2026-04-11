import time
import threading
import numpy as np
import pandas as pd
import psycopg2
import mlflow
import mlflow.sklearn
from fastapi import FastAPI
from datetime import datetime
from sqlalchemy import create_engine

# ── Config ────────────────────────────────────────────────
PG_HOST    = "postgres"
PG_DB      = "airquality"
PG_USER    = "airflow"
PG_PASS    = "airflow"
MLFLOW_URI = "http://mlflow:5000"
MODEL_NAME = "AirQuality_AQI_predictor"
LOOKBACK   = 6
PARAMETERS = ["pm1", "pm10", "pm25", "rh", "temperature", "pm003"]

# feature column names matching training
FEATURE_COLS = [f"{p}_t-{LOOKBACK-j}"
                for j in range(LOOKBACK) for p in PARAMETERS]

app = FastAPI(title="Air Quality Prediction API")

# ── Global state ──────────────────────────────────────────
current_model         = None
current_model_version = None

# ── DB connection ─────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host=PG_HOST, database=PG_DB,
        user=PG_USER, password=PG_PASS
    )

def get_engine():
    return create_engine(f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}/{PG_DB}")

# ── AQI Calculator ────────────────────────────────────────
def calculate_aqi(pm25, pm10):
    aqi_pm25 = min(int((pm25 / 35.0) * 100), 500)
    aqi_pm10 = min(int((pm10 / 75.0) * 100), 500)
    aqi = max(aqi_pm25, aqi_pm10)

    if aqi <= 50:
        category = "Good"
    elif aqi <= 100:
        category = "Moderate"
    elif aqi <= 150:
        category = "Unhealthy for Sensitive Groups"
    elif aqi <= 200:
        category = "Unhealthy"
    elif aqi <= 300:
        category = "Very Unhealthy"
    else:
        category = "Hazardous"

    return aqi, category

# ── Load model from MLflow ────────────────────────────────
def load_production_model():
    global current_model, current_model_version
    try:
        mlflow.set_tracking_uri(MLFLOW_URI)
        client = mlflow.MlflowClient()
        prod   = client.get_model_version_by_alias(MODEL_NAME, "production")

        if prod.version != current_model_version:
            print(f"✓ Loading model version {prod.version}...")
            current_model         = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@production")
            current_model_version = prod.version
            print(f"✓ Model version {prod.version} loaded")

    except Exception as e:
        print(f"✗ Failed to load model: {e}")

# ── Get last N rows ───────────────────────────────────────
def get_last_rows(n=6):
    engine = get_engine()
    df     = pd.read_sql(f"""
        SELECT measured_at, pm1, pm10, pm25, rh, temperature, pm003
        FROM processed_readings
        WHERE pm1  IS NOT NULL
          AND pm10 IS NOT NULL
          AND pm25 IS NOT NULL
        ORDER BY measured_at DESC
        LIMIT {n}
    """, engine)
    return df.sort_values("measured_at").reset_index(drop=True)

# ── Wait for new row ──────────────────────────────────────
def wait_for_new_row(after_timestamp, timeout=600):
    start = time.time()
    while time.time() - start < timeout:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT measured_at, pm25, pm10
            FROM processed_readings
            WHERE measured_at > %s
              AND pm25 IS NOT NULL
              AND pm10 IS NOT NULL
            ORDER BY measured_at ASC
            LIMIT 1
        """, (after_timestamp,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row:
            return row  # (measured_at, pm25, pm10)

        time.sleep(10)  # check every 10 seconds

    return None

# ── Save prediction ───────────────────────────────────────
def save_prediction(measured_at, pm25_pred, pm10_pred,
                    pm25_actual, pm10_actual, model_version):

    pm25_error    = abs(pm25_pred - pm25_actual)
    pm10_error    = abs(pm10_pred - pm10_actual)
    aqi_pred, cat = calculate_aqi(pm25_pred, pm10_pred)
    aqi_actual, _ = calculate_aqi(pm25_actual, pm10_actual)

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO predictions (
            measured_at,
            pm25_predicted, pm10_predicted,
            pm25_actual,    pm10_actual,
            pm25_error,     pm10_error,
            aqi_predicted,  aqi_actual,
            aqi_category,   model_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        measured_at,
        pm25_pred,   pm10_pred,
        pm25_actual, pm10_actual,
        pm25_error,  pm10_error,
        aqi_pred,    aqi_actual,
        cat,         model_version
    ))
    conn.commit()
    cur.close()
    conn.close()

    print(f"✓ Saved prediction: pm25={pm25_pred:.2f} (actual={pm25_actual:.2f}, error={pm25_error:.2f})")

# ── Background prediction loop ────────────────────────────
def prediction_loop():
    print("✓ Starting prediction loop...")
    time.sleep(15)  # wait for services to be ready

    while True:
        try:
            # reload model if new version promoted
            load_production_model()

            if current_model is None:
                print("✗ No model loaded, retrying in 30s...")
                time.sleep(30)
                continue

            # get last 6 rows
            df = get_last_rows(LOOKBACK)
            if len(df) < LOOKBACK:
                print(f"✗ Not enough rows ({len(df)}/{LOOKBACK}), waiting...")
                time.sleep(30)
                continue

            # build input features with named columns (matches training)
            flat     = df[PARAMETERS].values.flatten()
            features = pd.DataFrame([flat], columns=FEATURE_COLS)

            # predict
            pred           = current_model.predict(features)[0]
            pm25_predicted = pred[0]
            pm10_predicted = pred[1]
            last_timestamp = df["measured_at"].iloc[-1]

            print(f"✓ Predicted pm25={pm25_predicted:.2f}, pm10={pm10_predicted:.2f}")
            print(f"✓ Waiting for new row after {last_timestamp}...")

            # wait for actual new row
            new_row = wait_for_new_row(last_timestamp)

            if new_row is None:
                print("✗ Timeout waiting for new row, retrying...")
                continue

            actual_timestamp, pm25_actual, pm10_actual = new_row

            # save prediction vs actual
            save_prediction(
                measured_at   = actual_timestamp,
                pm25_pred     = float(pm25_predicted),
                pm10_pred     = float(pm10_predicted),
                pm25_actual   = float(pm25_actual),
                pm10_actual   = float(pm10_actual),
                model_version = current_model_version
            )

        except Exception as e:
            print(f"✗ Prediction loop error: {e}")
            time.sleep(30)

# ── Start background thread on startup ───────────────────
@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=prediction_loop, daemon=True)
    thread.start()
    print("✓ Prediction loop thread started")

# ── Endpoints ─────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status":        "ok",
        "model_name":    MODEL_NAME,
        "model_version": current_model_version,
        "model_loaded":  current_model is not None,
        "timestamp":     datetime.now().isoformat()
    }

@app.get("/predictions")
def get_predictions(limit: int = 50):
    engine = get_engine()
    df     = pd.read_sql(f"""
        SELECT *
        FROM predictions
        ORDER BY predicted_at DESC
        LIMIT {limit}
    """, engine)
    return df.to_dict(orient="records")

@app.get("/latest")
def get_latest():
    engine = get_engine()
    df     = pd.read_sql("""
        SELECT *
        FROM predictions
        ORDER BY predicted_at DESC
        LIMIT 1
    """, engine)
    if df.empty:
        return {"message": "No predictions yet"}
    return df.to_dict(orient="records")[0]