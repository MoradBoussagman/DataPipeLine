from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="hello_spark_cluster",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["spark", "demo"]
) as dag:

    spark_job = SparkSubmitOperator(
        task_id="run_hello_spark",
        application="/opt/airflow/scripts/hello_spark.py",
        conn_id="spark_cluster",  # Airflow connection pointing to spark://spark-master:7077
        name="hello_spark_job",
        conf={"spark.master": "spark://spark-master:7077"}
    )