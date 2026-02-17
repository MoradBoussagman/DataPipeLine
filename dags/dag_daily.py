"""
dag_daily.py
Runs every day at midnight:
  1. reads yesterday's raw_readings
  2. computes daily averages for all 6 parameters
  3. computes AQI
  4. stores into daily_metrics
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, "/opt/airflow/scripts")

from batch_job import run_batch

default_args = {
    "owner":       "data_analyst",
    "start_date":  datetime(2026, 2, 17),
    "retries":     1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_daily",
    default_args=default_args,
    description="Every midnight: compute daily averages + AQI → daily_metrics",
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["phase1", "batch", "daily"]
) as dag:

    task_batch = PythonOperator(
        task_id="compute_daily_metrics",
        python_callable=run_batch
    )
