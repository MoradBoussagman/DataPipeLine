import os
import warnings
import numpy as np
import pandas as pd
import psycopg2
import mlflow
import mlflow.sklearn
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.multioutput import MultiOutputRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error

os.environ["GIT_PYTHON_REFRESH"] = "quiet"
warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────
LOOKBACK        = 6
PARAMETERS      = ["pm1", "pm10", "pm25", "rh", "temperature", "pm003"]
MIN_ROWS        = 100
DATA_SOURCE     = os.environ.get("DATA_SOURCE", "postgres")
BACKFILL_START  = os.environ.get("BACKFILL_START")
BACKFILL_END    = os.environ.get("BACKFILL_END")

PG_HOST    = os.environ.get("PG_HOST",    "postgres")
PG_DB      = os.environ.get("PG_DB",      "airquality")
PG_USER    = os.environ.get("PG_USER",    "airflow")
PG_PASS    = os.environ.get("PG_PASS",    "airflow")
MLFLOW_URI = os.environ.get("MLFLOW_URI", "http://localhost:5000")
MODEL_NAME = "AirQuality_AQI_predictor_backfill"
EXPERIMENT = "air_quality_backfill"

# ── 1. Load data ──────────────────────────────────────────
def load_data():
    if not BACKFILL_START or not BACKFILL_END:
        raise ValueError("BACKFILL_START and BACKFILL_END must be set")

    print(f"✓ Backfill mode: {BACKFILL_START} → {BACKFILL_END}")

    conn = psycopg2.connect(
        host=PG_HOST, database=PG_DB,
        user=PG_USER, password=PG_PASS
    )

    df = pd.read_sql("""
        SELECT measured_at, pm1, pm10, pm25, rh, temperature, pm003
        FROM processed_readings
        WHERE pm1  IS NOT NULL
          AND pm10 IS NOT NULL
          AND pm25 IS NOT NULL
          AND measured_at >= %(start)s
          AND measured_at < %(end)s::date + INTERVAL '1 day'
        ORDER BY measured_at ASC
    """, conn, params={"start": BACKFILL_START, "end": BACKFILL_END})

    conn.close()

    if len(df) < MIN_ROWS:
        print(f"✗ Not enough data in range: {len(df)} rows (need {MIN_ROWS})")
        return None

    print(f"✓ Loaded {len(df)} rows from PostgreSQL for backfill range")
    return df

# ── 2. Build sequences ────────────────────────────────────
def build_sequences(df):
    rows_X, rows_y = [], []

    for i in range(LOOKBACK, len(df) - 1):
        past      = df[PARAMETERS].iloc[i - LOOKBACK:i].values.flatten()
        pm25_next = df["pm25"].iloc[i]
        pm10_next = df["pm10"].iloc[i]
        rows_X.append(past)
        rows_y.append([pm25_next, pm10_next])

    feature_cols = [f"{p}_t-{LOOKBACK-j}"
                    for j in range(LOOKBACK) for p in PARAMETERS]
    target_cols  = ["pm25_t+1", "pm10_t+1"]

    X = pd.DataFrame(rows_X, columns=feature_cols)
    y = pd.DataFrame(rows_y, columns=target_cols)

    return X, y

# ── 3. Train and log ──────────────────────────────────────
def run_experiment():
    df = load_data()
    if df is None:
        return

    X, y = build_sequences(df)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=False
    )
    print(f"✓ Train: {X_train.shape} | Test: {X_test.shape}")

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)

    models = {
        "LinearRegression": MultiOutputRegressor(LinearRegression()),
        "RandomForest":     RandomForestRegressor(n_estimators=100, random_state=42),
        "GradientBoosting": MultiOutputRegressor(
                                GradientBoostingRegressor(n_estimators=100, random_state=42)
                            ),
    }

    best_rmse   = float("inf")
    best_run_id = None
    best_name   = None

    for name, model in models.items():
        with mlflow.start_run(run_name=name) as run:
            model.fit(X_train, y_train)
            preds = model.predict(X_test)

            rmse = np.sqrt(mean_squared_error(y_test, preds))
            mae  = mean_absolute_error(y_test, preds)

            mlflow.log_param("model",          name)
            mlflow.log_param("lookback",       LOOKBACK)
            mlflow.log_param("targets",        "pm25_t+1, pm10_t+1")
            mlflow.log_param("data_source",    DATA_SOURCE)
            mlflow.log_param("backfill_start", BACKFILL_START)
            mlflow.log_param("backfill_end",   BACKFILL_END)
            mlflow.log_param("train_rows",     len(X_train))
            mlflow.log_metric("rmse",          rmse)
            mlflow.log_metric("mae",           mae)
            mlflow.sklearn.log_model(model, name="model")

            print(f"{name}: RMSE={rmse:.4f}  MAE={mae:.4f}")

            if rmse < best_rmse:
                best_rmse   = rmse
                best_run_id = run.info.run_id
                best_name   = name

    # ── 4. Register best model ────────────────────────────
    print(f"\n✓ Best model: {best_name} (RMSE={best_rmse:.4f})")
    model_uri = f"runs:/{best_run_id}/model"
    result    = mlflow.register_model(model_uri=model_uri, name=MODEL_NAME)
    print(f"✓ Registered version {result.version}")

    # ── 5. Compare with Production, promote if better ─────
    client = mlflow.MlflowClient()

    try:
        prod_alias  = client.get_model_version_by_alias(MODEL_NAME, "production")
        prod_run_id = prod_alias.run_id
        prod_rmse   = client.get_metric_history(prod_run_id, "rmse")[-1].value

        if best_rmse < prod_rmse:
            client.delete_registered_model_alias(MODEL_NAME, "production")
            client.set_registered_model_alias(MODEL_NAME, "production", str(result.version))
            print(f"✓ Version {result.version} promoted (RMSE {best_rmse:.4f} < {prod_rmse:.4f})")
        else:
            print(f"✗ Not better (RMSE {best_rmse:.4f} >= {prod_rmse:.4f})")
            print(f"✗ Keeping current production version {prod_alias.version}")

    except Exception:
        client.set_registered_model_alias(MODEL_NAME, "production", str(result.version))
        print(f"✓ Version {result.version} set as production (first time)")

if __name__ == "__main__":
    run_experiment()