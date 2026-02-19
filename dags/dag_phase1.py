"""
dag_phase1.py
Every minute: topics → fetch → consumer (Spark) → db_writer
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, "/opt/airflow/scripts")

from topics    import create_topics
from fetch     import fetch_and_send
from db_writer import run_db_writer

default_args = {
    "owner":       "team",
    "start_date":  datetime(2026, 2, 18),
    "retries":     1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dag_phase1",
    default_args=default_args,
    description="Every minute: topics → fetch → spark consumer → db_writer",
    schedule_interval="* * * * *",
    catchup=False,
    tags=["phase1"]
) as dag:

    task_topics = PythonOperator(
        task_id="create_kafka_topics",
        python_callable=create_topics
    )

    task_fetch = PythonOperator(
        task_id="fetch_openaq_to_kafka",
        python_callable=fetch_and_send
    )

    task_spark = SparkSubmitOperator(
        task_id="spark_consumer",
        application="/opt/airflow/scripts/consumer.py",
        conn_id="spark_cluster",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.5.0",
        name="air_quality_consumer",
        verbose=True
    )

    task_db_writer = PythonOperator(
        task_id="db_writer",
        python_callable=run_db_writer
    )

    task_topics >> task_fetch >> task_spark >> task_db_writer