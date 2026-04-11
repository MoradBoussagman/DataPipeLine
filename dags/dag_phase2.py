"""
dag_phase2.py
Training pipeline triggered when 500+ new rows added to processed_readings
sensor (500 new rows?) → validate_data → train_model → log_training
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
import sys
import psycopg2

sys.path.insert(0, "/opt/airflow/scripts")

PG_HOST  = "postgres"
PG_DB    = "airquality"
PG_USER  = "airflow"
PG_PASS  = "airflow"
MIN_ROWS = 2 # 2 for testing later would be 500 or more !!

# ── Custom Sensor ─────────────────────────────────────────
class NewDataSensor(BaseSensorOperator):
    """
    Waits until 500 new rows added since last training run.
    """
    @apply_defaults
    def __init__(self, min_rows=MIN_ROWS, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.min_rows = min_rows

    def poke(self, context):
        conn = psycopg2.connect(
            host=PG_HOST, database=PG_DB,
            user=PG_USER, password=PG_PASS
        )
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS training_log (
                id         SERIAL PRIMARY KEY,
                trained_at TIMESTAMP DEFAULT NOW(),
                rows_used  INTEGER
            )
        """)
        conn.commit()

        cur.execute("SELECT trained_at FROM training_log ORDER BY trained_at DESC LIMIT 1")
        last_training = cur.fetchone()

        if last_training is None:
            cur.execute("SELECT COUNT(*) FROM processed_readings")
        else:
            cur.execute("""
                SELECT COUNT(*) FROM processed_readings
                WHERE measured_at > %s
            """, (last_training[0],))

        new_rows = cur.fetchone()[0]
        cur.close()
        conn.close()

        self.log.info(f"New rows since last training: {new_rows} (need {self.min_rows})")
        return new_rows >= self.min_rows

# ── Task 1: Validate data ─────────────────────────────────
def validate_data():
    conn = psycopg2.connect(
        host=PG_HOST, database=PG_DB,
        user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM processed_readings")
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT
            ROUND(100.0 * SUM(CASE WHEN pm25 IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2),
            ROUND(100.0 * SUM(CASE WHEN pm10 IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2),
            ROUND(100.0 * SUM(CASE WHEN pm1  IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
        FROM processed_readings
    """)
    nulls = cur.fetchone()

    cur.execute("SELECT MAX(measured_at) FROM processed_readings")
    last_reading = cur.fetchone()[0]

    cur.close()
    conn.close()

    print(f"✓ Total rows:   {total}")
    print(f"✓ pm25 nulls:   {nulls[0]}%")
    print(f"✓ pm10 nulls:   {nulls[1]}%")
    print(f"✓ pm1  nulls:   {nulls[2]}%")
    print(f"✓ Last reading: {last_reading}")

    if nulls[0] > 50:
        raise ValueError(f"Too many pm25 nulls: {nulls[0]}%")
    if nulls[1] > 50:
        raise ValueError(f"Too many pm10 nulls: {nulls[1]}%")

    print("✓ Validation passed")

# ── Task 3: Log training ──────────────────────────────────
def log_training():
    conn = psycopg2.connect(
        host=PG_HOST, database=PG_DB,
        user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM processed_readings")
    total = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO training_log (rows_used) VALUES (%s)",
        (total,)
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"✓ Training logged at {total} total rows")

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
    dag_id="dag_phase2",
    default_args=default_args,
    description="Train when 500 new rows added to processed_readings",
    schedule_interval="@continuous",
    #schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["phase2", "ml"]
) as dag:

    task_sensor = NewDataSensor(
        task_id="wait_for_500_new_rows",
        min_rows=MIN_ROWS,
        poke_interval=10,   # check every 10 seconds instead of 300
        timeout=86400,
        mode="poke"
    )

    task_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    task_train = BashOperator(
        task_id="train_model",
        bash_command=(
            "docker exec "
            "-e DATA_SOURCE=postgres "
            "-e MLFLOW_URI=http://localhost:5000 "
            "-e PG_HOST=postgres "
            "-e PG_DB=airquality "
            "-e PG_USER=airflow "
            "-e PG_PASS=airflow "
            "mlflow python /mlscripts/experiment2.py"
        ),
    )

    task_log = PythonOperator(
        task_id="log_training",
        python_callable=log_training
    )

    task_sensor >> task_validate >> task_train >> task_log