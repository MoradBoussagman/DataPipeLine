# DataPipeline: Airflow + Spark

This project is a **Data Pipeline** combining **Apache Airflow** and **Apache Spark** to orchestrate and process data workflows efficiently. It is designed to be modular, extensible, and suitable for handling batch or streaming data.

---

## Table of Contents
- [Project Structure](#project-structure)
- [Technologies](#technologies)
- [Setup](#setup)
- [Usage](#usage)
- [Dockerization](#dockerization)
- [Notes](#notes)
- [License](#license)

---

## Project Structure

The project has the following structure:

├─ airflow/ # Airflow core setup (config, Dockerfile, etc.)
├─ dags/ # Airflow DAGs (Directed Acyclic Graphs)
├─ logs/ # Airflow logs
├─ plugins/ # Airflow custom plugins
├─ scripts/ # Helper scripts for Spark or data processing
├─ spark/ # Spark configurations and Dockerfile
├─ .env # Environment variables
├─ docker-compose.yaml # Compose file for multi-container setup
└─ .gitignore # Ignore unnecessary files
---

## Technologies

- **Apache Airflow**: Workflow orchestration
- **Apache Spark**: Data processing (batch/streaming)
- **Docker & Docker Compose**: Containerized environment
- **Python**: Scripting and DAG definitions

---

## Setup

1. Clone the repository:

```bash
git clone https://github.com/MoradBoussagman/DataPipeLine.git
cd DataPipeLine
docker compose build
docker compose up -d
```
