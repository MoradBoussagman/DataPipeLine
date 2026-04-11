import os
import warnings
import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.multioutput import MultiOutputRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error

os.environ["GIT_PYTHON_REFRESH"] = "quiet"
warnings.filterwarnings("ignore")

# ── 1. Load and prepare data ──────────────────────────────
df = pd.read_csv("/mlscripts/historical.csv")
df = df[["datetimeUtc", "parameter", "value"]]

df_wide = df.pivot_table(
    index="datetimeUtc",
    columns="parameter",
    values="value",
    aggfunc="mean"
).reset_index()

df_wide.columns.name = None
df_wide = df_wide.rename(columns={
    "datetimeUtc":      "measured_at",
    "um003":            "pm003",
    "relativehumidity": "rh",
})

df_wide = df_wide[["measured_at", "pm1", "pm10", "pm25", "rh", "temperature", "pm003"]]
df_wide["measured_at"] = pd.to_datetime(df_wide["measured_at"])
df_wide = df_wide.sort_values("measured_at").reset_index(drop=True)

# ── 2. Build sequences ────────────────────────────────────
LOOKBACK   = 6
HORIZON    = 6
PARAMETERS = ["pm1", "pm10", "pm25", "rh", "temperature", "pm003"]

rows_X, rows_y = [], []
for i in range(LOOKBACK, len(df_wide) - HORIZON):
    past   = df_wide[PARAMETERS].iloc[i - LOOKBACK:i].values.flatten()
    future = df_wide["pm25"].iloc[i:i + HORIZON].values
    rows_X.append(past)
    rows_y.append(future)

feature_cols = [f"{p}_t-{LOOKBACK-j}" for j in range(LOOKBACK) for p in PARAMETERS]
target_cols  = [f"pm25_t+{i+1}" for i in range(HORIZON)]

X = pd.DataFrame(rows_X, columns=feature_cols)
y = pd.DataFrame(rows_y, columns=target_cols)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, shuffle=False
)

print(f"Train: {X_train.shape} | Test: {X_test.shape}")

# ── 3. Train and log to MLflow ────────────────────────────
mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("air_quality_pm25_prediction")

models = {
    "LinearRegression": MultiOutputRegressor(LinearRegression()),
    "RandomForest":     RandomForestRegressor(n_estimators=100, random_state=42),
    "GradientBoosting": MultiOutputRegressor(GradientBoostingRegressor(n_estimators=100, random_state=42)),
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

        mlflow.log_param("model",        name)
        mlflow.log_param("lookback",     LOOKBACK)
        mlflow.log_param("horizon",      HORIZON)
        mlflow.log_param("n_estimators", 100)
        mlflow.log_metric("rmse",        rmse)
        mlflow.log_metric("mae",         mae)
        mlflow.sklearn.log_model(model,  name="model")

        print(f"{name}: RMSE={rmse:.4f}  MAE={mae:.4f}")

        if rmse < best_rmse:
            best_rmse   = rmse
            best_run_id = run.info.run_id
            best_name   = name

# ── 4. Register best model ────────────────────────────────
print(f"\n✓ Best model: {best_name} (RMSE={best_rmse:.4f})")
model_uri = f"runs:/{best_run_id}/model"
result = mlflow.register_model(model_uri=model_uri, name="AirQuality_pm25_predictor")
print(f"✓ Registered version {result.version}")

# ── 5. Compare with current Production, promote if better ─
client = mlflow.MlflowClient()

# get current Production version if exists
production_versions = client.get_latest_versions(
    name="AirQuality_pm25_predictor",
    stages=["Production"]
)

if not production_versions:
    # no Production yet → promote directly
    client.transition_model_version_stage(
        name="AirQuality_pm25_predictor",
        version=result.version,
        stage="Production"
    )
    print(f"✓ Version {result.version} promoted to Production (first time)")

else:
    # compare new model RMSE with current Production RMSE
    prod_version    = production_versions[0]
    prod_run_id     = prod_version.run_id
    prod_rmse       = client.get_metric_history(prod_run_id, "rmse")[-1].value

    if best_rmse < prod_rmse:
        # new model is better → archive old, promote new
        client.transition_model_version_stage(
            name="AirQuality_pm25_predictor",
            version=prod_version.version,
            stage="Archived"
        )
        client.transition_model_version_stage(
            name="AirQuality_pm25_predictor",
            version=result.version,
            stage="Production"
        )
        print(f"✓ Version {result.version} is better (RMSE {best_rmse:.4f} < {prod_rmse:.4f})")
        print(f"✓ Promoted to Production, version {prod_version.version} archived")
    else:
        # current Production is still better → keep it, archive new
        client.transition_model_version_stage(
            name="AirQuality_pm25_predictor",
            version=result.version,
            stage="Archived"
        )
        print(f"✗ New model not better (RMSE {best_rmse:.4f} >= {prod_rmse:.4f})")
        print(f"✗ Keeping current Production version {prod_version.version}")