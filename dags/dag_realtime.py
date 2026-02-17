"""
dag_realtime.py
Runs every minute:
  1. create Kafka topics (if not exist)
  2. fetch from OpenAQ → Kafka raw_data
  3. consumer reads raw_data → routes to processed_data or anomalies
  4. db_writer reads processed_data + anomalies → writes to PostgreSQL
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, "/opt/airflow/scripts")

from topics    import create_topics
from fetch     import fetch_and_send
from consumer_ import run_consumer
from db_writer import run_db_writer

default_args = {
    "owner":       "data_engineer",
    "start_date":  datetime(2026, 2, 17),
    "retries":     1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dag_realtime",
    default_args=default_args,
    description="Every minute: topics → fetch → consumer → db_writer",
    schedule_interval="* * * * *",
    catchup=False,
    tags=["phase1", "realtime"]
) as dag:

    task_topics = PythonOperator(
        task_id="create_kafka_topics",
        python_callable=create_topics
    )

    task_fetch = PythonOperator(
        task_id="fetch_openaq_to_kafka",
        python_callable=fetch_and_send
    )

    task_consumer = PythonOperator(
        task_id="consumer_clean_and_route",
        python_callable=run_consumer
    )

    task_db_writer = PythonOperator(
        task_id="db_writer_kafka_to_postgres",
        python_callable=run_db_writer
    )

    task_topics >> task_fetch >> task_consumer >> task_db_writer
