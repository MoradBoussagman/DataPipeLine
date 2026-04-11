"""
dag_backfill.py
Manual backfill DAG - retrain model on a specific historical date range
Trigger with: { "start_date": "2026-01-01", "end_date": "2026-02-01" }
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.param import Param
from datetime import datetime, timedelta
import psycopg2

PG_HOST  = "postgres"
PG_DB    = "airquality"
PG_USER  = "airflow"
PG_PASS  = "airflow"

# ── Task 1: Check data availability ──────────────────────
def check_data_availability(**context):
    start_date = context["params"].get("start_date")
    end_date   = context["params"].get("end_date")

    if not start_date or not end_date:
        raise ValueError("Missing parameters: provide start_date and end_date")

    conn = psycopg2.connect(
        host=PG_HOST, database=PG_DB,
        user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*) FROM processed_readings
        WHERE measured_at >= %s AND measured_at < %s::date + INTERVAL '1 day'
    """, (start_date, end_date))
    row_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    print(f"✓ Date range: {start_date} → {end_date}")
    print(f"✓ Rows found in range: {row_count}")

    if row_count == 0:
        raise ValueError(f"No data found between {start_date} and {end_date} — nothing to backfill")

    context["ti"].xcom_push(key="row_count",  value=row_count)
    context["ti"].xcom_push(key="start_date", value=start_date)
    context["ti"].xcom_push(key="end_date",   value=end_date)

    print("✓ Data availability check passed")

# ── Task 2: Validate data quality ────────────────────────
def validate_backfill_data(**context):
    start_date = context["ti"].xcom_pull(key="start_date", task_ids="check_data_availability")
    end_date   = context["ti"].xcom_pull(key="end_date",   task_ids="check_data_availability")

    conn = psycopg2.connect(
        host=PG_HOST, database=PG_DB,
        user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT
            ROUND(100.0 * SUM(CASE WHEN pm25 IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2),
            ROUND(100.0 * SUM(CASE WHEN pm10 IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2),
            ROUND(100.0 * SUM(CASE WHEN pm1  IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
        FROM processed_readings
        WHERE measured_at >= %s AND measured_at < %s::date + INTERVAL '1 day'
    """, (start_date, end_date))
    nulls = cur.fetchone()

    cur.close()
    conn.close()

    print(f"✓ pm25 nulls: {nulls[0]}%")
    print(f"✓ pm10 nulls: {nulls[1]}%")
    print(f"✓ pm1  nulls: {nulls[2]}%")

    if nulls[0] > 50:
        raise ValueError(f"Too many pm25 nulls in backfill range: {nulls[0]}%")
    if nulls[1] > 50:
        raise ValueError(f"Too many pm10 nulls in backfill range: {nulls[1]}%")

    print("✓ Backfill validation passed")

# ── Task 3: Log backfill ──────────────────────────────────
def log_backfill(**context):
    start_date = context["ti"].xcom_pull(key="start_date", task_ids="check_data_availability")
    end_date   = context["ti"].xcom_pull(key="end_date",   task_ids="check_data_availability")
    row_count  = context["ti"].xcom_pull(key="row_count",  task_ids="check_data_availability")

    conn = psycopg2.connect(
        host=PG_HOST, database=PG_DB,
        user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS backfill_log (
            id         SERIAL PRIMARY KEY,
            start_date TIMESTAMP,
            end_date   TIMESTAMP,
            rows_used  INTEGER,
            ran_at     TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        INSERT INTO backfill_log (start_date, end_date, rows_used)
        VALUES (%s, %s, %s)
    """, (start_date, end_date, row_count))

    conn.commit()
    cur.close()
    conn.close()

    print(f"✓ Backfill logged: {start_date} → {end_date} ({row_count} rows)")

# ── Alerts ────────────────────────────────────────────────
def on_failure_callback(context):
    from alerts import email_alert
    email_alert(context)

# ── Default args ──────────────────────────────────────────
default_args = {
    "owner":               "team",
    "start_date":          datetime(2026, 2, 18),
    "retries":             1,
    "retry_delay":         timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

# ── DAG ───────────────────────────────────────────────────
with DAG(
    dag_id="dag_backfill",
    default_args=default_args,
    description="Manual backfill DAG - retrain on historical date range",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["backfill", "ml"],
    params={
        "start_date": Param("2026-01-01", type="string", description="Start date YYYY-MM-DD"),
        "end_date":   Param("2026-02-26", type="string", description="End date YYYY-MM-DD"),
    }
) as dag:

    task_check = PythonOperator(
        task_id="check_data_availability",
        python_callable=check_data_availability,
        provide_context=True,
    )

    task_validate = PythonOperator(
        task_id="validate_backfill_data",
        python_callable=validate_backfill_data,
        provide_context=True,
    )

    task_retrain = BashOperator(
        task_id="retrain_model",
        bash_command=(
            "docker exec "
            "-e DATA_SOURCE=postgres "
            "-e MLFLOW_URI=http://localhost:5000 "
            "-e PG_HOST=postgres "
            "-e PG_DB=airquality "
            "-e PG_USER=airflow "
            "-e PG_PASS=airflow "
            "-e BACKFILL_START={{ params.start_date }} "
            "-e BACKFILL_END={{ params.end_date }} "
            "mlflow python /mlscripts/experiment_backfill.py"
        ),
    )

    task_log = PythonOperator(
        task_id="log_backfill",
        python_callable=log_backfill,
        provide_context=True,
    )

    task_check >> task_validate >> task_retrain >> task_log