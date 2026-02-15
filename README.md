# DataPipeline: Airflow + Spark

A production-ready **Data Pipeline** combining **Apache Airflow 2.8.1** and **Apache Spark 3.5.0** for orchestrating and processing distributed data workflows. Clone this repository and start running Spark jobs from Airflow in minutes!

---

## 📋 Table of Contents

* [Features](#features)
* [What's Included](#whats-included)
* [Technologies](#technologies)
* [Architecture Overview](#architecture-overview)
* [Prerequisites](#prerequisites)
* [Quick Start](#quick-start)
* [Spark Integration Deep Dive](#spark-integration-deep-dive)
* [Usage](#usage)
* [Creating Your Own Pipelines](#creating-your-own-pipelines)
* [Advanced Spark Configuration](#advanced-spark-configuration)
* [Monitoring & Debugging](#monitoring--debugging)
* [Troubleshooting](#troubleshooting)
* [Performance Tuning](#performance-tuning)
* [License](#license)

---

## ✨ Features

* **Full Airflow Integration**: Complete Airflow setup with webserver, scheduler, and PostgreSQL backend
* **Spark Cluster**: Master-worker Spark cluster configuration for distributed processing
* **Working Example**: Pre-configured DAG demonstrating Spark job submission
* **Containerized Environment**: Docker Compose orchestration for all services
* **Production Ready**: Includes proper initialization, user management, and logging
* **Scalable Architecture**: Add more Spark workers as needed
* **PySpark Support**: Python API for Spark with full integration
* **Spark UI**: Built-in monitoring interface for jobs and executors

---

## 📦 What's Included

When you clone this repository, you get everything you need:

### ✅ **Pre-Configured Files**

* **Docker Compose** setup for all services
* **Airflow Dockerfile** with Java and PySpark
* **Spark Dockerfile** for cluster nodes
* **Example DAG** (`hello_spark_dag.py`) - ready to run
* **Sample Spark Job** (`hello_spark.py`) - working PySpark script
* **Connection Configuration** for Airflow-Spark integration

### 📁 **Project Structure**

```
DataPipeLine/
├── airflow/
│   └── Dockerfile               # Airflow with Java & PySpark
├── dags/
│   └── hello_spark_dag.py       # Example DAG with SparkSubmitOperator
├── logs/                        # Auto-generated during runtime
├── plugins/                     # For custom Airflow plugins
├── scripts/
│   └── hello_spark.py           # Sample PySpark job
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
| **Apache Spark** | 3.5.0 | Distributed data processing |
| **PySpark** | 3.5.0 | Python API for Spark |
| **PostgreSQL** | 13 | Airflow metadata database |
| **Docker Compose** | 2.x+ | Multi-container orchestration |
| **OpenJDK** | 17 | Java runtime for Spark |

---

## 🏗 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Airflow Webserver                           │
│                    (localhost:8085)                             │
│         - DAG Management                                        │
│         - Job Monitoring                                        │
│         - Connection Configuration                              │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Airflow Scheduler                             │
│         - Monitors DAGs                                         │
│         - Triggers SparkSubmitOperators                         │
│         - Manages Task Dependencies                             │
└────────────┬────────────────────────────┬──────────────────────┘
             │                            │
             ▼                            ▼
┌────────────────────┐       ┌───────────────────────────────────┐
│    PostgreSQL      │       │        Spark Master               │
│  (Metadata Store)  │       │      (localhost:8080)             │
│                    │       │  - Job Scheduling                 │
│  - DAG Metadata    │       │  - Resource Management            │
│  - Task State      │       │  - Worker Coordination            │
│  - Connections     │       │  - Application Submission (7077)  │
└────────────────────┘       └───────────────┬───────────────────┘
                                             │
                                             ▼
                             ┌───────────────────────────────────┐
                             │        Spark Worker(s)            │
                             │  - Execute Spark Tasks            │
                             │  - Process Data Partitions        │
                             │  - Report to Master               │
                             └───────────────────────────────────┘

Data Flow:
1. Airflow Scheduler triggers SparkSubmitOperator
2. Operator submits PySpark script to Spark Master (port 7077)
3. Spark Master assigns tasks to Workers
4. Workers execute transformations/actions
5. Results returned to Airflow task
6. Task status updated in PostgreSQL
```

---

## 📦 Prerequisites

* **Docker Desktop** or **Docker Engine** (20.x+)
* **Docker Compose** (v2.x recommended)
* Minimum **8GB RAM** allocated to Docker
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
- Airflow init creates admin user and sets up environment
- Spark Master starts and opens ports 7077 (submission) and 8080 (UI)
- Spark Worker(s) register with Master
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
airflow-webserver       Up (healthy)
airflow-scheduler       Up
airflow-init            Exited (0)
```

### 4. Access the Interfaces

* **Airflow UI**: http://localhost:8085
  + Username: `admin`
  + Password: `admin`
* **Spark Master UI**: http://localhost:8080
  + View cluster status, running applications, and worker nodes

---

## ⚡ Spark Integration Deep Dive

### How Airflow Communicates with Spark

The integration uses the **SparkSubmitOperator** which:

1. **Establishes Connection**: Uses the `spark_cluster` connection to communicate with Spark Master
2. **Submits Application**: Sends your PySpark script to `spark://spark-master:7077`
3. **Monitors Execution**: Tracks job progress and logs output
4. **Reports Status**: Updates Airflow task state based on Spark job completion

### Spark Cluster Components

#### **Spark Master**
- **Purpose**: Cluster manager and entry point for job submission
- **Ports**:
  - `7077`: Application submission (used by Airflow)
  - `8080`: Web UI for monitoring
- **Location**: `spark-master` container
- **Resource Allocation**: Manages CPU and memory across workers

#### **Spark Worker(s)**
- **Purpose**: Execute Spark tasks assigned by Master
- **Ports**:
  - `8081+`: Worker Web UI (increments for each worker)
- **Default Resources**:
  - Cores: 2
  - Memory: 2GB
- **Scalability**: Add more workers by modifying `docker-compose.yaml`

### PySpark Script Execution Flow

```
┌─────────────────────────────────────────────────────────┐
│  1. Airflow triggers SparkSubmitOperator                │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  2. Operator reads script from /opt/airflow/scripts/    │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  3. Submits to spark://spark-master:7077                │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  4. Master creates SparkContext                         │
│     - Allocates executors on workers                    │
│     - Distributes application JAR/files                 │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  5. Workers execute transformations                     │
│     - Read data sources                                 │
│     - Apply transformations (map, filter, etc.)         │
│     - Execute actions (collect, save, etc.)             │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  6. Results returned to driver                          │
│     - Aggregated results                                │
│     - Logs and metrics                                  │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  7. Airflow task marked as SUCCESS/FAILED               │
└─────────────────────────────────────────────────────────┘
```

### Spark Configuration in Docker Compose

Key configurations from your setup:

```yaml
spark-master:
  environment:
    - SPARK_MODE=master
    - SPARK_MASTER_HOST=spark-master
    - SPARK_MASTER_PORT=7077
  ports:
    - "8080:8080"  # Spark UI
    - "7077:7077"  # Job submission

spark-worker:
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    - SPARK_WORKER_CORES=2
    - SPARK_WORKER_MEMORY=2G
```

---

## 💡 Usage

### Running the Included Example

The repository includes a working example DAG that demonstrates Spark integration!

#### Step 1: Set Up Spark Connection

Choose **ONE** of these methods:

**Option A: Via Airflow UI (Recommended)**

1. Open http://localhost:8085 and login
2. Go to **Admin → Connections**
3. Click **+** to add a new connection
4. Fill in:
   * **Connection Id**: `spark_cluster`
   * **Connection Type**: `Spark`
   * **Host**: `spark-master`
   * **Port**: `7077`
   * **Extra**: `{"queue": "default", "deploy-mode": "client"}`
5. Click **Save**

**Option B: Via CLI**

```bash
docker compose exec airflow-webserver airflow connections add \
    'spark_cluster' \
    --conn-type 'spark' \
    --conn-host 'spark-master' \
    --conn-port '7077' \
    --conn-extra '{"queue": "default", "deploy-mode": "client"}'
```

**Option C: Via Connection File**

The repository includes a `connection_string` file for automated setup.

#### Step 2: Verify Spark Cluster Health

Before running jobs, ensure Spark cluster is healthy:

```bash
# Check Spark Master logs
docker compose logs spark-master | tail -20

# Check Worker registration
docker compose logs spark-worker | grep "Successfully registered"
```

Visit http://localhost:8080 - you should see:
- **Status**: ALIVE
- **Workers**: 1 (or more if you added workers)
- **Cores**: Available cores listed
- **Memory**: Available memory listed

#### Step 3: Run the Example DAG

1. In Airflow UI, find the DAG **`hello_spark_cluster`**
2. Toggle the DAG to **ON** (unpause)
3. Click **▶️ Trigger DAG** button
4. Click on the running task → **Log** to see output

#### Step 4: Monitor Spark Execution

**In Airflow:**
- View real-time logs in the task interface
- Check task duration and status

**In Spark UI (http://localhost:8080):**
- Click on the running application
- View stages, tasks, and executors
- Check resource utilization
- Review event timeline

#### Step 5: Check Results

You should see in the Airflow logs:

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

### Example 1: Simple Data Processing

**1. Create your PySpark script in `scripts/` folder:**

```python
# scripts/process_data.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("DataProcessing") \
        .getOrCreate()
    
    # Sample data
    data = [
        ("Alice", 25, "Engineering"),
        ("Bob", 30, "Sales"),
        ("Charlie", 35, "Engineering"),
        ("David", 28, "Sales"),
        ("Eve", 32, "Engineering")
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(data, ["name", "age", "department"])
    
    # Process data
    result = df.groupBy("department") \
               .agg(
                   count("name").alias("employee_count"),
                   avg("age").alias("avg_age")
               ) \
               .orderBy("department")
    
    # Show results
    print("\n" + "="*50)
    print("DEPARTMENT STATISTICS")
    print("="*50)
    result.show()
    
    # Save results (optional)
    # result.write.mode("overwrite").parquet("/opt/airflow/data/output")
    
    spark.stop()

if __name__ == "__main__":
    main()
```

**2. Create your DAG in `dags/` folder:**

```python
# dags/data_processing_pipeline.py
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_processing_pipeline",
    default_args=default_args,
    description="Process data using Spark",
    schedule="@daily",
    catchup=False,
    tags=["spark", "data-processing"]
) as dag:

    # Task 1: Run Spark job
    process_data = SparkSubmitOperator(
        task_id="process_data",
        application="/opt/airflow/scripts/process_data.py",
        conn_id="spark_cluster",
        name="data_processing_job",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.executor.memory": "1g",
            "spark.driver.memory": "1g",
            "spark.executor.cores": "1"
        },
        verbose=True
    )
    
    # Task 2: Post-processing (optional)
    def notify_completion(**context):
        print(f"Data processing completed at {datetime.now()}")
    
    notify = PythonOperator(
        task_id="notify_completion",
        python_callable=notify_completion,
        provide_context=True
    )
    
    # Define task dependencies
    process_data >> notify
```

### Example 2: Reading External Data

```python
# scripts/read_csv_data.py
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("CSVReader") \
        .getOrCreate()
    
    # Read CSV (mount your data directory in docker-compose.yaml)
    df = spark.read.csv(
        "/opt/airflow/data/input/dataset.csv",
        header=True,
        inferSchema=True
    )
    
    # Process
    df.printSchema()
    df.show(10)
    
    # Transformations
    cleaned_df = df.dropna()
    
    # Save results
    cleaned_df.write.mode("overwrite") \
        .parquet("/opt/airflow/data/output/cleaned_data")
    
    spark.stop()

if __name__ == "__main__":
    main()
```

### Example 3: Complex ETL Pipeline

```python
# dags/etl_pipeline.py
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",  # Run at 2 AM daily
    catchup=False
) as dag:

    # Extract
    extract = SparkSubmitOperator(
        task_id="extract_data",
        application="/opt/airflow/scripts/extract.py",
        conn_id="spark_cluster",
        name="extract_job"
    )
    
    # Transform
    transform = SparkSubmitOperator(
        task_id="transform_data",
        application="/opt/airflow/scripts/transform.py",
        conn_id="spark_cluster",
        name="transform_job",
        application_args=["--input", "/data/extracted", "--output", "/data/transformed"]
    )
    
    # Load
    load = SparkSubmitOperator(
        task_id="load_data",
        application="/opt/airflow/scripts/load.py",
        conn_id="spark_cluster",
        name="load_job"
    )
    
    # Cleanup
    cleanup = BashOperator(
        task_id="cleanup_temp_files",
        bash_command="rm -rf /tmp/spark-temp-*"
    )
    
    extract >> transform >> load >> cleanup
```

### DAG Schedule Options

```python
schedule="@daily"        # Every day at midnight
schedule="@hourly"       # Every hour
schedule="0 9 * * *"     # Every day at 9 AM
schedule="*/15 * * * *"  # Every 15 minutes
schedule="0 2 * * 1"     # Every Monday at 2 AM
schedule=None            # Manual trigger only
```

---

## ⚙️ Advanced Spark Configuration

### Passing Spark Configuration

You can customize Spark behavior through the `conf` parameter:

```python
SparkSubmitOperator(
    task_id="advanced_spark_job",
    application="/opt/airflow/scripts/my_job.py",
    conn_id="spark_cluster",
    conf={
        # Resource allocation
        "spark.executor.memory": "4g",
        "spark.driver.memory": "2g",
        "spark.executor.cores": "2",
        "spark.executor.instances": "3",
        
        # Performance tuning
        "spark.default.parallelism": "100",
        "spark.sql.shuffle.partitions": "200",
        
        # Serialization
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        
        # Dynamic allocation
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "5",
        
        # Networking
        "spark.network.timeout": "800s",
        
        # Logging
        "spark.eventLog.enabled": "true"
    }
)
```

### Using Spark with Different Data Sources

#### Reading from PostgreSQL

```python
# scripts/read_postgres.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PostgresReader") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.x.x.jar") \
    .getOrCreate()

df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/airflow") \
    .option("dbtable", "my_table") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()
```

#### Writing to Parquet

```python
# Optimized columnar storage
df.write.mode("overwrite") \
    .partitionBy("date", "department") \
    .parquet("/opt/airflow/data/output/data.parquet")
```

#### Reading JSON

```python
df = spark.read.json("/opt/airflow/data/input/*.json")
```

### Passing Application Arguments

```python
SparkSubmitOperator(
    task_id="parameterized_job",
    application="/opt/airflow/scripts/process.py",
    conn_id="spark_cluster",
    application_args=[
        "--input-path", "/data/input",
        "--output-path", "/data/output",
        "--date", "{{ ds }}",  # Airflow template
        "--mode", "production"
    ]
)
```

Then in your PySpark script:

```python
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input-path", required=True)
parser.add_argument("--output-path", required=True)
parser.add_argument("--date", required=True)
parser.add_argument("--mode", default="dev")
args = parser.parse_args()

# Use arguments
df = spark.read.parquet(args.input_path)
```

### Scaling Your Spark Cluster

To add more workers, modify `docker-compose.yaml`:

```yaml
spark-worker-1:
  image: bitnami/spark:3.5.0
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    - SPARK_WORKER_CORES=2
    - SPARK_WORKER_MEMORY=2G
  depends_on:
    - spark-master

spark-worker-2:
  image: bitnami/spark:3.5.0
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    - SPARK_WORKER_CORES=2
    - SPARK_WORKER_MEMORY=2G
  depends_on:
    - spark-master
```

Then restart:

```bash
docker compose down
docker compose up -d
```

---

## 📊 Monitoring & Debugging

### Spark UI (http://localhost:8080)

**What you can monitor:**

1. **Overview**:
   - Cluster resource utilization
   - Number of workers
   - Running applications

2. **Application Details** (click on app):
   - **Jobs**: High-level operations (actions)
   - **Stages**: Groups of tasks
   - **Tasks**: Individual units of work
   - **Executors**: Worker processes
   - **Environment**: Configuration
   - **SQL**: Query execution plans (if using DataFrame API)

3. **Key Metrics**:
   - Input/Output size
   - Shuffle read/write
   - Task duration
   - Memory usage
   - GC time

### Reading Spark Logs

**From Airflow UI:**
```
Go to DAG → Task → Logs
```

**From command line:**
```bash
# Airflow task logs
docker compose logs -f airflow-scheduler

# Spark Master logs
docker compose logs -f spark-master

# Spark Worker logs
docker compose logs -f spark-worker

# All Spark logs
docker compose logs -f spark-master spark-worker
```

### Common Log Patterns to Watch For

**Successful execution:**
```
INFO SparkContext: Successfully stopped SparkContext
INFO ShutdownHookManager: Shutdown hook called
```

**Worker registration:**
```
INFO Master: Registering worker spark-worker:8881 with 2 cores, 2.0 GiB RAM
```

**Job completion:**
```
INFO DAGScheduler: Job 0 finished
INFO TaskSetManager: Finished task in stage 0.0
```

### Debugging Failed Jobs

**Step 1: Check Airflow Logs**
```bash
# Look for Python exceptions or Spark errors
docker compose logs airflow-scheduler | grep -A 10 "ERROR"
```

**Step 2: Check Spark Master**
```bash
# Look for resource issues or worker disconnections
docker compose logs spark-master | grep -A 10 "ERROR\|WARN"
```

**Step 3: Check Spark Worker**
```bash
# Look for executor failures or OOM errors
docker compose logs spark-worker | grep -A 10 "ERROR\|OutOfMemory"
```

**Step 4: Verify Script Exists**
```bash
docker compose exec airflow-webserver ls -l /opt/airflow/scripts/
```

---

## 🔧 Troubleshooting

### Spark-Specific Issues

#### 1. "Connection refused to spark-master:7077"

**Cause**: Spark Master not ready or connection misconfigured

**Fix**:
```bash
# Check if Spark Master is running
docker compose ps spark-master

# Check Master logs
docker compose logs spark-master | grep "Started SocketEndpoint"

# Verify connection in Airflow
docker compose exec airflow-webserver airflow connections get spark_cluster
```

#### 2. "Not enough resources available"

**Cause**: Workers don't have enough CPU/memory

**Fix**:
```bash
# Check available resources in Spark UI (http://localhost:8080)
# Reduce resource requirements in your conf:

conf={
    "spark.executor.memory": "1g",  # Reduce from 2g
    "spark.executor.cores": "1"     # Reduce from 2
}
```

#### 3. "Worker not registered with Master"

**Fix**:
```bash
# Restart worker
docker compose restart spark-worker

# Check registration
docker compose logs spark-worker | grep "Successfully registered"
```

#### 4. "PySpark script not found"

**Fix**:
```bash
# Verify mount point
docker compose exec airflow-webserver cat /opt/airflow/scripts/your_script.py

# Check docker-compose.yaml volumes:
volumes:
  - ./scripts:/opt/airflow/scripts
```

#### 5. "Java heap space" or "OutOfMemoryError"

**Fix**:
```python
# Increase driver/executor memory
conf={
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
    "spark.memory.fraction": "0.8"
}
```

Also increase Docker container memory:
```yaml
spark-worker:
  deploy:
    resources:
      limits:
        memory: 4G
```

#### 6. Job runs but produces no output

**Check**:
1. Are you calling `.show()` or `.collect()` in your script?
2. Are transformations being evaluated? (Spark is lazy)
3. Check for silent exceptions in Spark logs

### General Troubleshooting

#### DAG Not Appearing?

```bash
# Check for import errors
docker compose exec airflow-webserver airflow dags list-import-errors

# Restart scheduler
docker compose restart airflow-scheduler

# Check DAG file is mounted
docker compose exec airflow-webserver ls /opt/airflow/dags
```

#### Connection Not Working?

```bash
# List all connections
docker compose exec airflow-webserver airflow connections list

# Test connection
docker compose exec airflow-webserver airflow connections get spark_cluster

# Delete and recreate
docker compose exec airflow-webserver airflow connections delete spark_cluster
docker compose exec airflow-webserver airflow connections add \
    'spark_cluster' \
    --conn-type 'spark' \
    --conn-host 'spark-master' \
    --conn-port '7077'
```

#### Port Already in Use?

Change ports in `.env` file or `docker-compose.yaml`:

```yaml
airflow-webserver:
  ports:
    - "8090:8085"  # Change external port

spark-master:
  ports:
    - "8081:8080"  # Change external port
```

Then restart:

```bash
docker compose down
docker compose up -d
```

---

## 🚀 Performance Tuning

### Optimizing Spark Jobs

#### 1. Partitioning

```python
# Repartition for better parallelism
df = df.repartition(200)

# Coalesce to reduce partitions (no shuffle)
df = df.coalesce(50)

# Partition by column for efficient filtering
df.write.partitionBy("date", "region").parquet("/output")
```

#### 2. Caching

```python
# Cache frequently accessed DataFrames
df.cache()
# or
df.persist(StorageLevel.MEMORY_AND_DISK)

# Use the DataFrame multiple times
result1 = df.filter(col("age") > 25).count()
result2 = df.filter(col("age") < 30).count()

# Unpersist when done
df.unpersist()
```

#### 3. Broadcast Small DataFrames

```python
from pyspark.sql.functions import broadcast

# Broadcast small lookup table
result = large_df.join(broadcast(small_df), "id")
```

#### 4. Avoid Shuffles

```python
# Bad: Multiple shuffles
df.groupBy("col1").count().groupBy("col2").sum()

# Good: Single groupBy
df.groupBy("col1", "col2").agg(count("*"), sum("value"))
```

#### 5. Use Appropriate File Formats

- **Parquet**: Best for analytics (columnar, compressed)
- **ORC**: Similar to Parquet, good for Hive
- **Avro**: Good for row-based processing
- **CSV**: Avoid for large datasets (slow parsing)

### Resource Allocation Guidelines

| Dataset Size | Executor Memory | Executor Cores | Parallelism |
|-------------|----------------|----------------|-------------|
| < 1 GB | 1-2 GB | 1 | 10-20 |
| 1-10 GB | 2-4 GB | 2 | 50-100 |
| 10-100 GB | 4-8 GB | 2-4 | 100-200 |
| > 100 GB | 8+ GB | 4+ | 200+ |

---

## 🛠 Useful Commands

### Managing Services

```bash
# View logs
docker compose logs -f [service-name]

# View Spark-specific logs
docker compose logs -f spark-master spark-worker

# Stop all services
docker compose down

# Restart specific service
docker compose restart airflow-scheduler

# Remove everything (including data)
docker compose down -v

# Scale workers
docker compose up -d --scale spark-worker=3
```

### Airflow Commands

```bash
# List all DAGs
docker compose exec airflow-webserver airflow dags list

# Test a DAG
docker compose exec airflow-webserver airflow dags test my_data_pipeline

# Test a specific task
docker compose exec airflow-webserver airflow tasks test my_data_pipeline process_data 2026-01-01

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

# Trigger DAG manually
docker compose exec airflow-webserver airflow dags trigger my_data_pipeline
```

### Spark Commands

```bash
# Check Spark Master status
docker compose logs spark-master

# Check Spark Worker status
docker compose logs spark-worker

# Check worker registration
docker compose logs spark-worker | grep "Successfully registered"

# Submit Spark job manually (bypass Airflow)
docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --executor-memory 1g \
    --total-executor-cores 2 \
    /opt/airflow/scripts/hello_spark.py

# Access Spark shell (interactive)
docker compose exec spark-master spark-shell --master spark://spark-master:7077

# Access PySpark shell
docker compose exec spark-master pyspark --master spark://spark-master:7077

# Check Spark version
docker compose exec spark-master spark-submit --version
```

### Container Commands

```bash
# Execute command in container
docker compose exec airflow-webserver bash

# Copy file from container
docker compose cp airflow-webserver:/opt/airflow/logs/my_dag/ ./local_logs/

# Copy file to container
docker compose cp my_script.py airflow-webserver:/opt/airflow/scripts/

# Check resource usage
docker stats
```

---

## 📚 Additional Resources

### Spark Documentation
- [Apache Spark Official Docs](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

### Airflow Documentation
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [SparkSubmitOperator Reference](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/)

### Learning Resources
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
- [Best Practices for Spark Jobs](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

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
2. Review logs: `docker compose logs -f`
3. Open an issue on GitHub with:
   - Error message
   - Relevant logs
   - Steps to reproduce

---

**Happy Data Processing with Spark! ⚡🚀**
