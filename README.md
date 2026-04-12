# Air Quality Data Pipeline

This repository is a containerized air-quality platform built around Airflow, Kafka, Spark, PostgreSQL, MLflow, FastAPI, and Metabase.

It currently supports:

- Real-time ingestion of air-quality readings from OpenAQ
- Kafka-based routing of valid readings and anomalies
- Spark-based processing inside Airflow DAGs
- PostgreSQL storage for processed readings, anomalies, metrics, and model metadata
- MLflow training, registration, and production-alias promotion
- FastAPI model-serving and prediction tracking
- Metabase dashboards on top of the same PostgreSQL instance

## Stack

Core services started by `docker-compose.yaml`:

- `postgres` for Airflow metadata, pipeline data, and MLflow backend storage
- `airflow-webserver`, `airflow-scheduler`, `airflow-init`
- `spark-master`, `spark-worker`
- `broker` (Kafka in KRaft mode)
- `kafka-ui`
- `mlflow`
- `fastapi`
- `metabase`

## Repository Layout

```text
DataPipeline/
|-- airflow/                  # Airflow image with Java, PySpark, Kafka libs, Docker CLI
|-- dags/                     # Airflow DAGs
|-- fastapi/                  # Prediction API container
|-- init/                     # PostgreSQL bootstrap SQL
|-- logs/                     # Airflow logs
|-- mlscripts/                # Training and backfill scripts
|-- scripts/                  # Kafka, Spark, ETL, and DB helpers
|-- spark/                    # Spark image
|-- .env
|-- docker-compose.yaml
`-- README.md
```

## Main Workflows

### 1. Real-time ingestion

The real-time path is implemented mainly through `dags/dag_phase1.py`:

1. Create Kafka topics
2. Fetch the latest OpenAQ readings for a configured location
3. Submit a Spark job that reads `raw_data` and routes rows to:
   - `processed_data`
   - `anomalies`
4. Persist both streams into PostgreSQL

Key files:

- `scripts/topics.py`
- `scripts/fetch.py`
- `scripts/consumer.py`
- `scripts/db_writer.py`

### 2. Model training

`dags/dag_phase2.py` watches `processed_readings` and retrains when enough new rows are available.

The training job:

- loads data from PostgreSQL
- builds lookback-window features
- trains multiple regressors
- compares RMSE/MAE
- registers the best model in MLflow
- updates the `production` alias when the new model is better
- stores model metadata in PostgreSQL table `model_registry`

Key files:

- `dags/dag_phase2.py`
- `mlscripts/experiment2.py`

### 3. Historical backfill training

`dags/dag_backfill.py` retrains on a user-specified date range passed as DAG params:

```json
{
  "start_date": "2026-01-01",
  "end_date": "2026-02-01"
}
```

Key files:

- `dags/dag_backfill.py`
- `mlscripts/experiment_backfill.py`

### 4. Prediction serving

The FastAPI service loads the MLflow production model alias and continuously:

- reads the latest processed rows
- predicts next `pm25` and `pm10`
- waits for the next actual row
- stores prediction vs actual values in PostgreSQL

Key file:

- `fastapi/main.py`

## Database Objects

The main bootstrap lives in `init/01_create_tables.sql`.

Created databases:

- `metabaseappdb`
- `mlflow`
- `airquality`

Main tables in `airquality`:

- `processed_readings`
- `anomalies`
- `metrics_batch`
- `thresholds`
- `predictions`
- `model_registry`

Tables created later by DAGs at runtime:

- `training_log`
- `backfill_log`

## DAG Inventory

### Active project DAGs

- `dag_phase1`: OpenAQ -> Kafka -> Spark -> PostgreSQL
- `dag_phase2`: sensor-driven model retraining with MLflow
- `dag_backfill`: manual historical retraining
- `dag_realtime`: earlier Python-only real-time variant of phase 1

### Demo or older DAGs kept in the repo

- `hello_spark_dag.py`
- `kafka_spark_dag.py`
- `ETL_AQ.py`
- `phase1.py`
- `dag_daily.py`

`dag_daily.py` currently references `raw_readings` and `daily_metrics`, which are not created by `init/01_create_tables.sql`. Treat it as legacy/incomplete unless you add the missing schema.

## Exposed Interfaces

After startup, the default local endpoints are:

- Airflow: `http://localhost:8085`
- Spark master UI: `http://localhost:8081`
- Kafka UI: `http://localhost:8090`
- MLflow: `http://localhost:5000`
- FastAPI: `http://localhost:8000`
- Metabase: `http://localhost:3000`

Default credentials present in the repo:

- Airflow: `admin` / `admin`
- Metabase metadata DB user: `metabase`

Environment values from `.env`:

- `POSTGRES_USER=airflow`
- `POSTGRES_PASSWORD=airflow`
- `POSTGRES_DB=airflow`
- `POSTGRES_PORT=5432`
- `AIRFLOW_PORT=8085`

## Quick Start

### 1. Build the containers

```bash
docker compose build
```

### 2. Start the stack

```bash
docker compose up -d
```

### 3. Check service state

```bash
docker compose ps
```

### 4. Open the main UIs

- Airflow: `http://localhost:8085`
- MLflow: `http://localhost:5000`
- FastAPI: `http://localhost:8000`
- FastAPI docs: `http://localhost:8000/docs`
- Kafka UI: `http://localhost:8090`
- Metabase: `http://localhost:3000`

## Airflow Notes

`airflow-init` currently does three bootstrap actions:

- runs Airflow DB migrations
- creates the default admin user
- creates the Spark connection `spark_cluster`

The compose file also mounts the Docker socket into Airflow so DAGs can run `docker exec` against the `mlflow` container for training commands.

## FastAPI Endpoints

The API exposes:

- `GET /health`
- `GET /predictions?limit=50`
- `GET /latest`

Example:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/latest
```

## MLflow Notes

Training scripts use:

- experiment `air_quality_aqi_prediction` for the main retraining flow
- experiment `air_quality_backfill` for backfill runs

Registered model names:

- `AirQuality_AQI_predictor`
- `AirQuality_AQI_predictor_backfill`

The FastAPI service reads `AirQuality_AQI_predictor@production`.

## Important Caveats

- Several secrets are hardcoded in the repo today, including the OpenAQ API key and SMTP credentials. Replace them with environment variables before any real deployment.
- `scripts/fetch.py` and some DAGs are tied to a single OpenAQ `LOCATION_ID`.
- `dag_phase2.py` is configured with `MIN_ROWS = 2` for testing, while the comments still describe a much larger production threshold.
- The repository contains generated MLflow artifacts under `mlscripts/artifacts/`; they are useful for local continuity but make the repo heavy.
- Some files are historical or experimental. Prefer `dag_phase1.py`, `dag_phase2.py`, `dag_backfill.py`, `fastapi/main.py`, and `init/01_create_tables.sql` when navigating the current implementation.

## Useful Commands

```bash
docker compose logs -f airflow-scheduler
docker compose logs -f mlflow
docker compose logs -f fastapi
docker compose restart airflow-scheduler
docker compose down
docker compose down -v
```

Useful Airflow checks:

```bash
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow connections list
docker compose exec airflow-webserver airflow dags list-import-errors
```

---

## 📄 License

This project is open-source and available under the MIT License.

---
