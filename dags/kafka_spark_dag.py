from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
import json

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
}

def create_kafka_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers="broker:9092",
        client_id="airflow_admin"
    )
    
    topics = [
        NewTopic(name="input-topic", num_partitions=3, replication_factor=1),
        NewTopic(name="output-topic", num_partitions=3, replication_factor=1)
    ]
    
    try:
        admin_client.create_topics(new_topics=topics, validate_only=False)
        print("✓ Topics created successfully!")
    except Exception as e:
        print(f"Topics may already exist: {e}")
    
    admin_client.close()

def produce_sample_data():
    producer = KafkaProducer(
        bootstrap_servers="broker:9092",
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    sample_data = [
        {"name": "Alice", "age": 25, "city": "NYC"},
        {"name": "Bob", "age": 17, "city": "LA"},
        {"name": "Charlie", "age": 30, "city": "Chicago"},
        {"name": "Diana", "age": 16, "city": "Boston"},
        {"name": "Eve", "age": 22, "city": "Seattle"}
    ]
    
    for record in sample_data:
        producer.send("input-topic", value=record)
        print(f"✓ Sent: {record}")
    
    producer.flush()
    producer.close()
    print("✓ All sample data sent!")

def consume_output():
    consumer = KafkaConsumer(
        "output-topic",
        bootstrap_servers="broker:9092",
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: m.decode('utf-8')
    )
    
    print("=" * 60)
    print("OUTPUT FROM KAFKA (Processed by Spark):")
    print("=" * 60)
    
    count = 0
    for message in consumer:
        print(f"✓ {message.value}")
        count += 1
    
    print("=" * 60)
    print(f"Total messages consumed: {count}")
    print("=" * 60)
    
    consumer.close()

with DAG(
    dag_id="kafka_spark_pipeline",
    default_args=default_args,
    description="Read from Kafka, process with Spark, write back to Kafka",
    schedule=None,
    catchup=False,
    tags=["kafka", "spark", "streaming"]
) as dag:

    create_topics = PythonOperator(
        task_id="create_kafka_topics",
        python_callable=create_kafka_topics
    )

    produce_data = PythonOperator(
        task_id="produce_sample_data",
        python_callable=produce_sample_data
    )

    process_kafka_with_spark = SparkSubmitOperator(
        task_id="process_kafka_with_spark",
        application="/opt/airflow/scripts/kafka_spark_job.py",
        conn_id="spark_cluster",  # Defines master, deploy mode, etc.
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        name="kafka_spark_job",
        verbose=True
    )

    consume_output_task = PythonOperator(
        task_id="consume_output_topic",
        python_callable=consume_output
    )

    create_topics >> produce_data >> process_kafka_with_spark >> consume_output_task