# DataPipeline: Airflow + Spark + kafka

A initial  **Data Pipeline** combining **Apache Airflow 2.8.1** , Apache kafka  and **Apache Spark 3.5.0** for orchestrating and processing distributed data workflows. Clone this repository and start running Spark jobs from Airflow in minutes!

---

## 📋 Table of Contents

- [Features](#features)
- [What's Included](#whats-included)
- [Technologies](#technologies)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [Creating Your Own Pipelines](#creating-your-own-pipelines)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## ✨ Features

- **Full Airflow Integration**: Complete Airflow setup with webserver, scheduler, and PostgreSQL backend
- **Kafka-broker** : Modern KRaft-based message broker (no Zookeeper required)
- **Spark Cluster**: Master-worker Spark cluster configuration for distributed processing
- **Working Example**: Pre-configured DAG demonstrating Spark job submission
- **Containerized Environment**: Docker Compose orchestration for all services
- **Ready to Run**: Clone and start - no additional configuration needed
- **Production Ready**: Includes proper initialization, user management, and logging

---

## 📦 What's Included

When you clone this repository, you get everything you need:

### ✅ **Pre-Configured Files**
- **Docker Compose** setup for all services (kafka , postges ,etc)
- **Airflow Dockerfile** with Java and PySpark
- **Spark Dockerfile** for cluster nodes
- **Example DAG** (`hello_spark_dag.py`) - ready to run
- **Sample Spark Job** (`hello_spark.py`) - working PySpark script

### 📁 **Project Structure**

```
DataPipeLine/
├── airflow/
│   └── Dockerfile               # Airflow with Java & PySpark
├── dags/
│   └── hello_spark_dag.py       # Example DAG
|   └── kafka_spark_dag.py       # Example DAG
├── logs/                        # Auto-generated during runtime
├── plugins/                     # For custom Airflow plugins
├── scripts/
│   └── hello_spark.py           # Sample PySpark job
|   └── kafka_spark_job.py           # Sample PySpark job
├── spark/
│   └── Dockerfile               # Spark base official image
├── connection_string            # Airflow connection setup
├── docker-compose.yaml          # Full orchestration 
└── README.md                    # documentation
```

## 🛠 Technologies

| Technology | Version | Purpose |
|------------|---------|---------|
| **Apache Airflow** | 2.8.1 | Workflow orchestration and scheduling |
| **Apache Spark** | 3.5.0 | Distributed data processing |
| **PostgreSQL** | 13 | Airflow metadata database |
| **Docker Compose** | 2.x+ | Multi-container orchestration |
| **PySpark** | 3.5.0 | Python API for Spark |
| **OpenJDK** | 17 | Java runtime for Spark |
| **Kafka** | 3.5.0 | streaming messages |
---

## 📦 Prerequisites

- **Docker Desktop** or **Docker Engine** (20.x+)
- **Docker Compose** (v2.x recommended)
- Minimum **8GB RAM** allocated to Docker
- Minimum **20GB disk space**
- **Git** for cloning

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
airflow-webserver       Up (healthy)
airflow-scheduler       Up
airflow-init            Exited (0)
broker                  Up
```

### 4. Access the Interfaces

- **Airflow UI**: http://localhost:8085
  - Username: `admin`
  - Password: `admin`
- **Spark Master UI**: http://localhost:8080

---

## 💡 Usage (hello_spark_dag.py)

### Running the Included Example

The repository includes a working example DAG that you can run immediately!

#### Step 1: Set Up Spark Connection

Choose **ONE** of these methods:

**Option A: Via Airflow UI (Recommended)**

1. Open http://localhost:8085 and login
2. Go to **Admin → Connections**
3. Click **+** to add a new connection
4. Fill in:
   - **Connection Id**: `spark_cluster`
   - **Connection Type**: `Spark`
   - **Host**: `spark-master`
   - **Port**: `7077`
5. Click **Save**

**Option B: Via CLI**

```bash
docker compose exec airflow-webserver airflow connections add \
    'spark_cluster' \
    --conn-type 'spark' \
    --conn-host 'spark-master' \
    --conn-port '7077'
```


#### Step 2: Run the Example DAG

1. In Airflow UI, find the DAG **`hello_spark_cluster`**
2. Toggle the DAG to **ON** (unpause)
3. Click **▶️ Trigger DAG** button
4. Click on the running task → **Log** to see output

#### Step 3: Check Results

You should see in the logs:

```
==================================================
HELLO WORLD FROM SPARK CLUSTER
==================================================
+-----+-----+
| word|count|
+-----+-----+
|Hello|    1|
|World|    2|
| from|    3|
|Spark|    4|
+-----+-----+

Spark job completed successfully!
```

---

## 🔨 Creating Your Own Pipelines

Now that the example works, create your own data pipelines!

### Adding a New Spark Job

**1. Create your PySpark script in `scripts/` folder:**

```bash
# Create new file: scripts/my_processing.py
```

```python
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("MyProcessing") \
        .getOrCreate()
    
    # Your data processing logic
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    # Process data
    result = df.filter(df.age > 25)
    result.show()
    
    spark.stop()

if __name__ == "__main__":
    main()
```

**2. Create your DAG in `dags/` folder:**

```bash
# Create new file: dags/my_pipeline.py
```

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="my_data_pipeline",
    default_args=default_args,
    schedule="@daily",  # Run every day
    catchup=False,
    tags=["production"]
) as dag:

    run_processing = SparkSubmitOperator(
        task_id="run_processing",
        application="/opt/airflow/scripts/my_processing.py",
        conn_id="spark_cluster",
        name="my_processing_job",
        conf={"spark.master": "spark://spark-master:7077"}
    )
```

**3. Your new DAG will automatically appear in Airflow UI!**

No restart needed - Airflow watches the `dags/` folder automatically.

### DAG Schedule Options

```python
schedule="@daily"        # Every day at midnight
schedule="@hourly"       # Every hour
schedule="0 9 * * *"     # Every day at 9 AM
schedule="*/15 * * * *"  # Every 15 minutes
schedule=None            # Manual trigger only
```

---

## 🛠 Useful Commands

### Managing Services

```bash
# View logs
docker compose logs -f [service-name]

# Stop all services
docker compose down

# Restart specific service
docker compose restart airflow-scheduler

# Remove everything (including data)
docker compose down -v
```

### Airflow Commands

```bash
# List all DAGs
docker compose exec airflow-webserver airflow dags list

# Test a DAG
docker compose exec airflow-webserver airflow dags test my_data_pipeline

# Check DAG for errors
docker compose exec airflow-webserver airflow dags list-import-errors

# Create a new admin user
docker compose exec airflow-webserver airflow users create \
    --username myuser \
    --password mypass \
    --firstname John \
    --lastname Doe \
    --role Admin \
    --email john@example.com
```

### Spark Commands

```bash
# Check Spark master status
docker compose logs spark-master

# Check Spark worker status
docker compose logs spark-worker

# Submit Spark job manually
docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/airflow/scripts/hello_spark.py
```

---

## 🔧 Troubleshooting

### DAG Not Appearing?

```bash
# Check for import errors
docker compose exec airflow-webserver airflow dags list-import-errors

# Restart scheduler
docker compose restart airflow-scheduler

# Check DAG file is mounted
docker compose exec airflow-webserver ls /opt/airflow/dags
```

### Connection Not Working?

```bash
# List all connections
docker compose exec airflow-webserver airflow connections list

# Test connection
docker compose exec airflow-webserver airflow connections get spark_cluster

# Delete and recreate
docker compose exec airflow-webserver airflow connections delete spark_cluster
docker compose exec airflow-webserver airflow connections add \
    'spark_cluster' --conn-type 'spark' --conn-host 'spark-master' --conn-port '7077'
```

### Spark Job Failing?

```bash
# Check Spark master logs
docker compose logs spark-master

# Check if workers are connected
# Open http://localhost:8080 and look for connected workers

# Verify script exists
docker compose exec airflow-webserver cat /opt/airflow/scripts/hello_spark.py
```

### Port Already in Use?

Change ports in `.env` file:

```bash
AIRFLOW_PORT=8090  # Change from 8085
POSTGRES_PORT=5433  # Change from 5432
```

Then restart:

```bash
docker compose down
docker compose up -d
```

---

## 📊 Architecture Overview


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
## 📄 License

This project is open-source and available under the MIT License.

---

**Happy Data Processing! 🚀**
