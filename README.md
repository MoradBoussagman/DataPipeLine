# DataPipeline: Airflow + Spark

A modular **Data Pipeline** combining **Apache Airflow** and **Apache Spark** to orchestrate and process data workflows efficiently. This project is designed for scalability, extensibility, and handles both batch and streaming data processing.

---

## 📋 Table of Contents

- [Features](#features)
- [Project Structure](#project-structure)
- [Technologies](#technologies)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
- [Dockerization](#dockerization)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## ✨ Features

- **Workflow Orchestration**: Leverage Apache Airflow for complex DAG scheduling
- **Distributed Processing**: Apache Spark for scalable batch and streaming data processing
- **Containerized Environment**: Full Docker Compose setup for easy deployment
- **Modular Architecture**: Clean separation of concerns with extensible structure
- **Production Ready**: Includes logging, monitoring, and error handling

---

## 📁 Project Structure

```
DataPipeLine/
├── airflow/                # Airflow core setup
│   ├── Dockerfile         # Airflow container configuration
│   └── airflow.cfg        # Airflow configuration file
├── dags/                   # Airflow DAGs (Directed Acyclic Graphs)
│   └── example_dag.py     # Sample DAG file
├── logs/                   # Airflow execution logs
├── plugins/                # Airflow custom plugins
├── scripts/                # Helper scripts for Spark jobs
│   └── spark_job.py       # Sample Spark processing script
├── spark/                  # Spark configurations
│   ├── Dockerfile         # Spark container configuration
│   └── spark-defaults.conf # Spark configuration
├── .env                    # Environment variables
├── .gitignore             # Git ignore rules
├── docker-compose.yaml    # Multi-container orchestration
└── README.md              # This file
```

---

## 🛠 Technologies

| Technology | Version | Purpose |
|------------|---------|---------|
| **Apache Airflow** | 2.x | Workflow orchestration and scheduling |
| **Apache Spark** | 3.x | Distributed data processing |
| **Docker** | 20.x+ | Containerization |
| **Docker Compose** | 2.x+ | Multi-container orchestration |
| **PostgreSQL** | 13+ | Airflow metadata database |
| **Python** | 3.8+ | Scripting and DAG definitions |

---

## 📦 Prerequisites

Before you begin, ensure you have the following installed:

- **Docker**: [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Compose**: [Install Docker Compose](https://docs.docker.com/compose/install/)
- **Git**: [Install Git](https://git-scm.com/downloads)
- Minimum **8GB RAM** and **20GB disk space** recommended

---

## 🚀 Setup

### 1. Clone the Repository

```bash
git clone https://github.com/MoradBoussagman/DataPipeLine.git
cd DataPipeLine
```

### 2. Configure Environment Variables

Edit the `.env` file to customize your setup:

```bash
# Example .env configuration
AIRFLOW_UID=50000
AIRFLOW_GID=0
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
```

### 3. Build and Start Containers

```bash
# Build Docker images
docker compose build

# Start all services in detached mode
docker compose up -d
```

### 4. Initialize Airflow Database

```bash
# Run database migrations (first time only)
docker compose run airflow-webserver airflow db init
```

### 5. Create Airflow Admin User

```bash
docker compose run airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

### 6. Access the Services

- **Airflow UI**: http://localhost:8080
  - Username: `admin`
  - Password: `admin`
- **Spark Master UI**: http://localhost:8081
- **Spark Worker UI**: http://localhost:8082

---

## 💡 Usage

### Creating DAGs

1. Place your DAG files in the `dags/` directory
2. Use the following template for a basic DAG:

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_pipeline',
    default_args=default_args,
    description='My data pipeline',
    schedule_interval=timedelta(days=1),
)

task1 = BashOperator(
    task_id='run_spark_job',
    bash_command='spark-submit /scripts/spark_job.py',
    dag=dag,
)
```

### Running Spark Jobs

Place your Spark scripts in the `scripts/` directory:

```python
# scripts/spark_job.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MySparkJob") \
    .getOrCreate()

# Your Spark processing logic here
df = spark.read.csv("/data/input.csv")
df.show()
```

### Monitoring

- Check DAG status in Airflow UI
- View logs in the `logs/` directory
- Monitor Spark jobs in Spark Master UI

---

## 🐳 Dockerization

### Services Overview

The `docker-compose.yaml` defines the following services:

| Service | Description | Port |
|---------|-------------|------|
| **postgres** | Airflow metadata database | 5432 |
| **airflow-webserver** | Airflow web interface | 8080 |
| **airflow-scheduler** | Airflow task scheduler | - |
| **spark-master** | Spark master node | 7077, 8081 |
| **spark-worker** | Spark worker node | 8082 |

### Useful Docker Commands

```bash
# View running containers
docker compose ps

# View
