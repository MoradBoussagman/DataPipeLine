# DataPipeline: Airflow + Spark + Kafka

A production-ready **Data Pipeline** combining **Apache Airflow 2.8.1**, **Apache Spark 3.5.0**, and **Apache Kafka (KRaft)** for orchestrating real-time streaming data workflows. Clone this repository and start processing streaming data in minutes!

---

## 📋 Table of Contents

* [Features](#features)
* [What's Included](#whats-included)
* [Technologies](#technologies)
* [Architecture Overview](#architecture-overview)
* [Prerequisites](#prerequisites)
* [Quick Start](#quick-start)
* [Kafka Integration Deep Dive](#kafka-integration-deep-dive)
* [Spark Streaming with Kafka](#spark-streaming-with-kafka)
* [Usage Examples](#usage-examples)
* [Creating Your Own Pipelines](#creating-your-own-pipelines)
* [Advanced Configuration](#advanced-configuration)
* [Monitoring & Debugging](#monitoring--debugging)
* [Troubleshooting](#troubleshooting)
* [Performance Tuning](#performance-tuning)
* [License](#license)

---

## ✨ Features

* **Full Airflow Integration**: Complete Airflow setup with webserver, scheduler, and PostgreSQL backend
* **Spark Cluster**: Master-worker Spark cluster configuration for distributed processing
* **Kafka Streaming**: KRaft-based Kafka broker for real-time data ingestion
* **Kafka UI**: Web interface for monitoring topics, messages, and consumer groups
* **Spark Structured Streaming**: Process Kafka streams with Spark's streaming API
* **Working Examples**: Pre-configured DAGs demonstrating batch and streaming workflows
* **Containerized Environment**: Docker Compose orchestration for all services
* **Production Ready**: Includes proper initialization, user management, and logging

---

## 📦 What's Included

When you clone this repository, you get everything you need:

### ✅ **Pre-Configured Files**

* **Docker Compose** setup for all services (Airflow, Spark, Kafka)
* **Airflow Dockerfile** with Java and PySpark
* **Spark Dockerfile** for cluster nodes
* **Kafka Broker** with KRaft (no Zookeeper needed)
* **Kafka UI** for topic monitoring
* **Example DAGs**:
  - `hello_spark_dag.py` - Batch processing example
  - `kafka_spark_pipeline.py` - Streaming pipeline example
* **Sample Jobs**:
  - `hello_spark.py` - Basic PySpark job
  - `kafka_spark_job.py` - Kafka streaming consumer

### 📁 **Project Structure**

```
DataPipeLine/
├── airflow/
│   └── Dockerfile               # Airflow with Java & PySpark
├── dags/
│   ├── hello_spark_dag.py       # Batch processing DAG
│   └── kafka_spark_dag.py  # Kafka streaming DAG
├── logs/                        # Auto-generated during runtime
├── plugins/                     # For custom Airflow plugins
├── scripts/
│   ├── hello_spark.py           # Sample PySpark job
│   └── kafka_spark_job.py       # Kafka consumer with Spark
├── spark/
│   └── Dockerfile               # Spark base official image
├── connection_string            # Airflow connection setup
├── docker-compose.yaml          # Full orchestration 
└── README.md                    # This file
```

---

## 🛠 Technologies

| Technology | Version | Purpose |
| --- | --- | --- |
| **Apache Airflow** | 2.8.1 | Workflow orchestration and scheduling |
| **Apache Spark** | 3.5.0 | Distributed data processing (batch & streaming) |
| **Apache Kafka** | Latest (KRaft) | Real-time message streaming |
| **Kafka UI** | Latest | Web-based Kafka monitoring |
| **PySpark** | 3.5.0 | Python API for Spark |
| **PostgreSQL** | 13 | Airflow metadata database |
| **Docker Compose** | 2.x+ | Multi-container orchestration |
| **OpenJDK** | 17 | Java runtime for Spark |

---

## 🏗 Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Airflow Webserver                            │
│                        (localhost:8085)                              │
│           - DAG Management                                           │
│           - Job Monitoring                                           │
│           - Connection Configuration                                 │
└────────────────────────┬─────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────────┐
│                       Airflow Scheduler                              │
│           - Monitors DAGs                                            │
│           - Triggers SparkSubmitOperators                            │
│           - Manages Task Dependencies                                │
└────────┬───────────────────────────────┬───────────────────────┬────┘
         │                               │                       │
         ▼                               ▼                       ▼
┌────────────────┐         ┌─────────────────────┐   ┌──────────────────┐
│   PostgreSQL   │         │   Spark Master      │   │  Kafka Broker    │
│ (Metadata DB)  │         │  (localhost:8080)   │   │ (localhost:9092) │
│                │         │  - Job Scheduling   │   │  - KRaft Mode    │
│ - DAG State    │         │  - Resource Mgmt    │   │  - Topics        │
│ - Task Logs    │         │  - Worker Coord.    │   │  - Partitions    │
└────────────────┘         └──────────┬──────────┘   └────────┬─────────┘
                                      │                       │
                                      ▼                       │
                          ┌───────────────────────┐           │
                          │    Spark Worker(s)    │           │
                          │  - Execute Tasks      │◄──────────┘
                          │  - Process Streams    │   Consume
                          │  - Write Results      │   Messages
                          └───────────┬───────────┘
                                      │
                                      ▼
                          ┌───────────────────────┐
                          │   Kafka UI            │
                          │ (localhost:8090)      │
                          │  - Topic Monitoring   │
                          │  - Message Browser    │
                          │  - Consumer Groups    │
                          └───────────────────────┘

Data Flow (Streaming Pipeline):
1. Airflow Scheduler triggers kafka_spark_pipeline DAG
2. Python tasks create Kafka topics and produce messages
3. SparkSubmitOperator submits streaming job to Spark Master
4. Spark Workers consume from input-topic, process data
5. Processed results written to output-topic
6. Consumer task reads and displays results
```

---

## 📦 Prerequisites

* **Docker Desktop** or **Docker Engine** (20.x+)
* Minimum **8GB RAM**
* Minimum **20GB disk space**
* **Git** for cloning

---

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/MoradBoussagman/DataPipeLine.git
cd DataPipeLine
```

### 2. Build and Start

```bash
# Build Docker images
docker compose build

# Start all services
docker compose up -d
```

**What happens during startup:**
- PostgreSQL initializes Airflow metadata database
- Airflow init creates admin user and sets up Spark connection
- Spark Master starts (ports 7077, 8080)
- Spark Worker(s) register with Master
- Kafka broker starts in KRaft mode (port 9092)
- Kafka UI connects to broker (port 8090)
- Airflow webserver and scheduler start

### 3. Verify Everything is Running

```bash
docker compose ps
```

Expected output:

```
NAME                    STATUS
postgres                Up
spark-master            Up
spark-worker            Up
broker                  Up
kafka-ui                Up
airflow-webserver       Up (healthy)
airflow-scheduler       Up
airflow-init            Exited (0)
```

### 4. Access the Interfaces

* **Airflow UI**: http://localhost:8085
  + Username: `admin`
  + Password: `admin`
* **Spark Master UI**: http://localhost:8080
  + View cluster status and running applications
* **Kafka UI**: http://localhost:8090
  + Monitor topics, messages, and consumer groups

---

## 🔥 Kafka Integration Deep Dive

### What is Kafka?

Apache Kafka is a distributed streaming platform that:
- **Publishes and subscribes** to streams of records (like a message queue)
- **Stores** streams of records in a fault-tolerant way
- **Processes** streams of records as they occur

### Kafka Architecture

#### **KRaft Mode (No Zookeeper!)**

this setup uses Kafka in **KRaft mode** (Kafka Raft), which eliminates the need for Zookeeper:

```yaml
broker:
  image: apache/kafka:latest
  environment:
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:9092
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker:9093
```

**Key Points:**
- **Simpler**: No separate Zookeeper ensemble
- **Faster**: Reduced operational complexity
- **Modern**: Latest Kafka architecture

#### **Kafka Components**

1. **Broker** (`broker:9092`)
   - Central message hub
   - Stores and serves messages
   - Handles client connections

2. **Topics**
   - Named streams of messages
   - Example: `input-topic`, `output-topic`
   - Partitioned for parallelism

3. **Partitions**
   - Default: 3 partitions per topic
   - Enable parallel processing
   - Increase throughput

4. **Kafka UI** (`localhost:8090`)
   - Web interface for monitoring
   - Browse messages visually
   - Track consumer lag

### How Airflow Interacts with Kafka

`kafka_spark_pipeline.py` DAG demonstrates the complete workflow:

```python
# Task 1: Create Kafka topics
create_topics = PythonOperator(
    task_id="create_kafka_topics",
    python_callable=create_kafka_topics  # Uses KafkaAdminClient
)

# Task 2: Produce sample data
produce_data = PythonOperator(
    task_id="produce_sample_data",
    python_callable=produce_sample_data  # Uses KafkaProducer
)

# Task 3: Process with Spark Streaming
process_kafka_with_spark = SparkSubmitOperator(
    task_id="process_kafka_with_spark",
    application="/opt/airflow/scripts/kafka_spark_job.py",
    packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"  # Kafka connector
)

# Task 4: Consume processed results
consume_output_task = PythonOperator(
    task_id="consume_output_topic",
    python_callable=consume_output  # Uses KafkaConsumer
)
```

**Workflow:**
1. **Admin**: Creates `input-topic` and `output-topic`
2. **Producer**: Sends JSON records to `input-topic`
3. **Spark**: Reads from `input-topic`, filters adults (age >= 18), writes to `output-topic`
4. **Consumer**: Reads processed results from `output-topic`

---

## ⚡ Spark Streaming with Kafka

### Spark Structured Streaming

Spark Structured Streaming treats streaming data as a continuously growing table:

```python
# Read from Kafka (Streaming DataFrame)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "input-topic") \
    .option("startingOffsets", "earliest") \
    .load()
```

### Kafka Spark Job

Let's break down `kafka_spark_job.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Create Spark Session with Kafka package
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# 2. Define schema for incoming JSON
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

# 3. Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "input-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Parse JSON values
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 5. Filter adults (age >= 18)
filtered_df = parsed_df.filter(col("age") >= 18)

# 6. Write back to Kafka
query = filtered_df.selectExpr(
    "CAST(name AS STRING) AS key",
    "to_json(struct(*)) AS value"
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", "output-topic") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

# 7. Wait for termination
query.awaitTermination(timeout=60)
```

### Key Concepts

#### **1. Streaming DataFrame**
- Treats stream as infinite table
- Apply SQL-like transformations
- Automatic micro-batching

#### **2. Kafka Source**
```python
.format("kafka")
.option("kafka.bootstrap.servers", "broker:9092")
.option("subscribe", "input-topic")
```

#### **3. Kafka Sink**
```python
.writeStream
.format("kafka")
.option("topic", "output-topic")
```

#### **4. Checkpointing**
```python
.option("checkpointLocation", "/tmp/checkpoint")
```
- Tracks processed offsets
- Enables fault tolerance
- Allows recovery after failures

---

## 💡 Usage Examples

### Example 1: Running the Kafka Streaming Pipeline

**Step 1: Ensure all services are running**

```bash
docker compose ps
# All services should show "Up"
```

**Step 2: Access Airflow UI**

Open http://localhost:8085 and login with `admin`/`admin`

**Step 3: Run the Kafka pipeline**

1. Find DAG: **`kafka_spark_pipeline`**
2. Toggle to **ON**
3. Click **▶️ Trigger DAG**

**Step 4: Monitor execution**

- **Airflow**: Watch task progress in the Graph/Tree view
- **Spark UI** (http://localhost:8080): View streaming job
- **Kafka UI** (http://localhost:8090): See messages in topics

**Step 5: Check results**

In the Airflow task logs for `consume_output_topic`, you'll see:

```
============================================================
OUTPUT FROM KAFKA (Processed by Spark):
============================================================
✓ {"name": "Alice", "age": 25, "city": "NYC"}
✓ {"name": "Charlie", "age": 30, "city": "Chicago"}
✓ {"name": "Eve", "age": 22, "city": "Seattle"}
============================================================
Total messages consumed: 3
============================================================
```

Note: Bob (17) and Diana (16) were filtered out (age < 18)!

### Example 2: Exploring Kafka UI

**Navigate to http://localhost:8090**

**View Topics:**
1. Click **Topics** in sidebar
2. You'll see: `input-topic`, `output-topic`
3. Click on a topic to view:
   - Partition distribution
   - Message count
   - Consumer groups

**Browse Messages:**
1. Click on `input-topic`
2. Click **Messages** tab
3. Click **Fetch messages**
4. See all raw messages with timestamps

**Monitor Consumer Groups:**
1. Click **Consumers** in sidebar
2. View active consumer groups
3. Check lag (unconsumed messages)

### Example 3: Manual Kafka Operations

**Produce messages manually:**

```bash
# Enter broker container
docker compose exec broker bash

# Create a test topic
kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic test-topic \
    --partitions 3 \
    --replication-factor 1

# Produce messages
kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic test-topic

# Type messages (press Enter after each):
> Hello Kafka
> This is a test
> Ctrl+C to exit

# Consume messages
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic test-topic \
    --from-beginning
```

---

## 🔨 Creating Your Own Pipelines

### Streaming Pipeline Template

**1. Create your Spark streaming script:**

```python
# scripts/my_streaming_job.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count

spark = SparkSession.builder \
    .appName("MyStreamingJob") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "sensor-data") \
    .load()

# Parse and process
processed = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*") \
    .groupBy(window("timestamp", "1 minute"), "sensor_id") \
    .agg(count("*").alias("count"))

# Write to Kafka
query = processed.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", "sensor-aggregates") \
    .option("checkpointLocation", "/tmp/checkpoint-sensors") \
    .outputMode("update") \
    .start()

query.awaitTermination()
```

**2. Create your Airflow DAG:**

```python
# dags/streaming_pipeline.py
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="streaming_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["streaming", "kafka"]
) as dag:

    stream_processing = SparkSubmitOperator(
        task_id="process_stream",
        application="/opt/airflow/scripts/my_streaming_job.py",
        conn_id="spark_cluster",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        conf={
            "spark.streaming.stopGracefullyOnShutdown": "true",
            "spark.sql.streaming.checkpointLocation": "/tmp/checkpoint"
        }
    )
```

### Batch + Streaming Hybrid

**Process historical data, then stream new data:**

```python
with DAG(
    dag_id="batch_then_stream",
    start_date=datetime(2026, 1, 1),
    schedule="@daily"
) as dag:

    # Batch: Process historical data
    batch_job = SparkSubmitOperator(
        task_id="batch_historical",
        application="/opt/airflow/scripts/batch_process.py",
        conn_id="spark_cluster"
    )
    
    # Stream: Process real-time data
    stream_job = SparkSubmitOperator(
        task_id="stream_realtime",
        application="/opt/airflow/scripts/stream_process.py",
        conn_id="spark_cluster",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    
    batch_job >> stream_job
```

### Real-World Use Case: Air Quality Monitoring

```python
# scripts/air_quality_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window, from_json
from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType, StringType

spark = SparkSession.builder \
    .appName("AirQualityMonitoring") \
    .getOrCreate()

# Schema for air quality sensor data
schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("pm25", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("location", StringType())
])

# Read from Kafka
sensor_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "air-quality-raw") \
    .load()

# Parse JSON
parsed = sensor_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Calculate 5-minute rolling averages
aggregated = parsed \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window("timestamp", "5 minutes", "1 minute"),
        "location"
    ) \
    .agg(
        avg("pm25").alias("avg_pm25"),
        avg("pm10").alias("avg_pm10"),
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity")
    )

# Flag unhealthy air quality (PM2.5 > 35.4)
alerts = aggregated.filter(col("avg_pm25") > 35.4)

# Write alerts to Kafka
query = alerts.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", "air-quality-alerts") \
    .option("checkpointLocation", "/tmp/checkpoint-airquality") \
    .start()

query.awaitTermination()
```

---

## ⚙️ Advanced Configuration

### Kafka Configuration

#### **Topic Creation with Custom Settings**

```python
from kafka.admin import NewTopic

topics = [
    NewTopic(
        name="high-throughput-topic",
        num_partitions=10,           # More partitions = more parallelism
        replication_factor=1,
        topic_configs={
            "retention.ms": "86400000",      # 1 day retention
            "compression.type": "snappy",    # Compress messages
            "max.message.bytes": "10485760"  # 10 MB max message size
        }
    )
]
```

#### **Producer Configuration**

```python
producer = KafkaProducer(
    bootstrap_servers="broker:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    compression_type='gzip',          # Compress before sending
    batch_size=16384,                 # Batch size in bytes
    linger_ms=10,                     # Wait 10ms to batch messages
    acks='all'                        # Wait for all replicas (durability)
)
```

#### **Consumer Configuration**

```python
consumer = KafkaConsumer(
    "my-topic",
    bootstrap_servers="broker:9092",
    group_id="my-consumer-group",
    auto_offset_reset='earliest',     # Start from beginning
    enable_auto_commit=True,          # Auto commit offsets
    auto_commit_interval_ms=5000,     # Commit every 5 seconds
    max_poll_records=500              # Fetch 500 records per poll
)
```

### Spark Streaming Configuration

#### **Checkpoint Configuration**

```python
SparkSubmitOperator(
    task_id="stream_job",
    application="/opt/airflow/scripts/stream.py",
    conf={
        # Checkpoint settings
        "spark.sql.streaming.checkpointLocation": "/data/checkpoints",
        
        # Graceful shutdown
        "spark.streaming.stopGracefullyOnShutdown": "true",
        
        # Backpressure (auto-adjust batch size)
        "spark.streaming.backpressure.enabled": "true",
        
        # Kafka consumer settings
        "spark.streaming.kafka.maxRatePerPartition": "1000",  # Max 1000 msgs/partition/sec
        
        # State management
        "spark.sql.streaming.stateStore.maintenanceInterval": "60s"
    }
)
```

#### **Windowing and Watermarking**

```python
# 10-minute tumbling window with 5-minute watermark
windowed = df \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "10 minutes")) \
    .count()
```

#### **Output Modes**

```python
# Append: Only new rows (default)
.outputMode("append")

# Complete: All rows in result table (for aggregations)
.outputMode("complete")

# Update: Only updated rows (for aggregations)
.outputMode("update")
```

### Scaling Kafka

**Add more partitions to existing topic:**

```bash
docker compose exec broker kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --alter \
    --topic my-topic \
    --partitions 10
```

**Add more Kafka brokers** (edit `docker-compose.yaml`):

```yaml
broker-1:
  image: apache/kafka:latest
  environment:
    KAFKA_NODE_ID: 1
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker-1:9093,2@broker-2:9093

broker-2:
  image: apache/kafka:latest
  environment:
    KAFKA_NODE_ID: 2
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker-1:9093,2@broker-2:9093
```

---

## 📊 Monitoring & Debugging

### Kafka Monitoring

#### **Using Kafka UI (http://localhost:8090)**

1. **Topics Overview**:
   - Total messages
   - Partition distribution
   - Replication status

2. **Consumer Groups**:
   - Active consumers
   - Lag per partition
   - Offset positions

3. **Brokers**:
   - Broker health
   - Disk usage
   - Network throughput

#### **Command-Line Monitoring**

```bash
# List topics
docker compose exec broker kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --list

# Describe topic
docker compose exec broker kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --topic input-topic

# Check consumer group lag
docker compose exec broker kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --group my-consumer-group
```

### Spark Streaming Monitoring

#### **Spark UI (http://localhost:8080)**

For streaming jobs, check:

1. **Streaming Tab**:
   - Input rate (records/sec)
   - Processing time per batch
   - Scheduling delay

2. **SQL Tab**:
   - Query execution plans
   - Shuffle read/write

3. **Executors Tab**:
   - Memory usage
   - GC time
   - Task failures

#### **Key Metrics to Watch**

- **Batch Processing Time**: Should be < Batch Interval
- **Scheduling Delay**: Should stay near 0
- **Total Delay**: Processing + Scheduling delay

### Logging

**Kafka logs:**
```bash
docker compose logs -f broker
```

**Spark streaming logs:**
```bash
docker compose logs -f spark-worker
```

**Airflow task logs:**
- Check in Airflow UI under task → Logs

---

## 🔧 Troubleshooting

### Kafka Issues

#### **1. "Cannot connect to broker"**

**Symptoms**: Producer/Consumer can't connect to `broker:9092`

**Fix**:
```bash
# Check if broker is running
docker compose ps broker

# Check broker logs
docker compose logs broker | grep "started"

# Restart broker
docker compose restart broker
```

#### **2. "Topic does not exist"**

**Fix**:
```bash
# List topics
docker compose exec broker kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --list

# Create topic manually
docker compose exec broker kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --topic my-topic \
    --partitions 3 \
    --replication-factor 1
```

#### **3. "Kafka UI not loading topics"**

**Fix**:
```bash
# Check Kafka UI logs
docker compose logs kafka-ui

# Restart Kafka UI
docker compose restart kafka-ui

# Verify connection in docker-compose.yaml:
# KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: broker:9092
```

#### **4. "Messages not appearing in topic"**

**Check producer:**
```python
# Add error handling
producer = KafkaProducer(
    bootstrap_servers="broker:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=3,
    max_block_ms=5000  # Timeout if broker unavailable
)

future = producer.send("topic", value)
try:
    record_metadata = future.get(timeout=10)
    print(f"✓ Message sent to {record_metadata.topic}")
except Exception as e:
    print(f"✗ Error: {e}")
```

### Spark Streaming Issues

#### **1. "Package not found: spark-sql-kafka"**

**Fix**: Ensure `packages` parameter in SparkSubmitOperator:

```python
SparkSubmitOperator(
    task_id="stream_job",
    application="/opt/airflow/scripts/stream.py",
    packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"  # REQUIRED
)
```

#### **2. "Checkpoint directory already exists"**

**Cause**: Previous run didn't complete gracefully

**Fix**:
```bash
# Remove checkpoint directory
docker compose exec spark-master rm -rf /tmp/checkpoint

# Or use a unique checkpoint per run
.option("checkpointLocation", f"/tmp/checkpoint-{datetime.now()}")
```

#### **3. "Streaming query not processing data"**

**Check**:
```python
# Add query listener
from pyspark.sql.streaming import StreamingQueryListener

class MyListener(StreamingQueryListener):
    def onQueryProgress(self, event):
        print(f"Batch {event.progress.batchId}: {event.progress.numInputRows} rows")

spark.streams.addListener(MyListener())
```

#### **4. "OutOfMemory in streaming job"**

**Fix**: Increase executor memory and adjust batch size

```python
SparkSubmitOperator(
    conf={
        "spark.executor.memory": "2g",
        "spark.driver.memory": "2g",
        "spark.streaming.kafka.maxRatePerPartition": "500"  # Reduce rate
    }
)
```

### Integration Issues

#### **1. "Airflow can't connect to Spark"**

```bash
# Verify Spark connection exists
docker compose exec airflow-webserver airflow connections get spark_cluster

# Test connection
docker compose exec airflow-webserver python -c "
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
hook = SparkSubmitHook(conn_id='spark_cluster')
print(hook._resolve_connection())
"
```

#### **2. "Spark can't reach Kafka broker"**

**Fix**: Use service name `broker`, not `localhost`:

```python
# ✗ Wrong
.option("kafka.bootstrap.servers", "localhost:9092")

# ✓ Correct
.option("kafka.bootstrap.servers", "broker:9092")
```

---

## 🚀 Performance Tuning

### Kafka Optimization

**1. Increase partitions for parallelism:**
```bash
# More partitions = more Spark tasks = higher throughput
kafka-topics.sh --create --topic high-volume --partitions 20
```

**2. Tune producer for throughput:**
```python
producer = KafkaProducer(
    batch_size=32768,           # Larger batches
    linger_ms=100,              # Wait longer to batch
    compression_type='lz4'      # Faster compression
)
```

**3. Consumer fetch settings:**
```python
consumer = KafkaConsumer(
    max_poll_records=1000,      # Fetch more per poll
    fetch_min_bytes=1024,       # Wait for at least 1KB
    fetch_max_wait_ms=500       # Max wait 500ms
)
```

### Spark Streaming Optimization

**1. Tune batch intervals:**
```python
# Balance latency vs throughput
# Smaller interval = lower latency, higher overhead
# Larger interval = higher throughput, higher latency

conf={
    "spark.streaming.blockInterval": "200ms",
    "spark.sql.streaming.trigger.processingTime": "5 seconds"
}
```

**2. Enable backpressure:**
```python
conf={
    "spark.streaming.backpressure.enabled": "true",
    "spark.streaming.backpressure.initialRate": "1000",
    "spark.streaming.kafka.maxRatePerPartition": "2000"
}
```

**3. Optimize state management:**
```python
conf={
    "spark.sql.streaming.stateStore.providerClass": 
        "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
    "spark.sql.streaming.stateStore.maintenance.interval": "60s"
}
```

**4. Partition tuning:**
```python
# Match Kafka partitions with Spark parallelism
df.repartition(20)  # If topic has 20 partitions
```

### Resource Allocation

**For high-throughput streaming:**

```yaml
spark-worker:
  environment:
    - SPARK_WORKER_CORES=4
    - SPARK_WORKER_MEMORY=6G
  deploy:
    resources:
      limits:
        cpus: '4'
        memory: 6G
```

```python
SparkSubmitOperator(
    conf={
        "spark.executor.memory": "4g",
        "spark.executor.cores": "2",
        "spark.driver.memory": "2g",
        "spark.default.parallelism": "40"
    }
)
```

---

## 🛠 Useful Commands

### Kafka Commands

```bash
# List all topics
docker compose exec broker kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --list

# Create topic
docker compose exec broker kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --topic my-topic \
    --partitions 3 \
    --replication-factor 1

# Delete topic
docker compose exec broker kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --delete \
    --topic my-topic

# Describe topic
docker compose exec broker kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --topic input-topic

# Produce messages
docker compose exec broker kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic test-topic

# Consume messages
docker compose exec broker kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic test-topic \
    --from-beginning

# Check consumer group
docker compose exec broker kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --group my-group

# Reset consumer group offset
docker compose exec broker kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group my-group \
    --reset-offsets \
    --to-earliest \
    --topic my-topic \
    --execute
```

### Spark Commands

```bash
# Submit streaming job manually
docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --conf spark.executor.memory=2g \
    /opt/airflow/scripts/kafka_spark_job.py

# Check running streaming queries
docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --class org.apache.spark.streaming.examples.NetworkWordCount \
    /opt/spark/examples/jars/spark-examples*.jar

# Monitor Spark logs
docker compose logs -f spark-master spark-worker
```

### Managing Services

```bash
# View all logs
docker compose logs -f

# View specific service logs
docker compose logs -f broker kafka-ui

# Restart services
docker compose restart broker
docker compose restart spark-master spark-worker

# Stop all
docker compose down

# Stop and remove volumes (clean slate)
docker compose down -v

# Scale Spark workers
docker compose up -d --scale spark-worker=3
```

---

## 📚 Additional Resources

### Documentation
- [Apache Kafka Docs](https://kafka.apache.org/documentation/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Spark-Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Airflow Spark Provider](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/)

### Learning Resources
- [Kafka Streams in Action](https://www.manning.com/books/kafka-streams-in-action)
- [Spark Streaming Examples](https://github.com/apache/spark/tree/master/examples/src/main/python/streaming)

---

## 📄 License

This project is open-source and available under the MIT License.

---

## 🤝 Contributing

Contributions are welcome! Feel free to:
- Report bugs
- Suggest features
- Submit pull requests
- Improve documentation

---

## 📧 Support

If you encounter issues:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review logs: `docker compose logs -f [service]`
3. Check Kafka UI: http://localhost:8090
4. Check Spark UI: http://localhost:8080
5. Open an issue on GitHub with:
   - Error message
   - Relevant logs
   - Steps to reproduce

---

**Happy Real-Time Data Processing! ⚡🚀🔥**
