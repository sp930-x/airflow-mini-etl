# Airflow Mini ETL Project (Weather Data)

## Overview
This project is a **minimal end-to-end ETL pipeline built with Apache Airflow**, designed to demonstrate the core concepts of data orchestration using a simple, file-based workflow.

The pipeline extracts hourly weather data from a public API, transforms it into a structured CSV format, validates the output, and orchestrates the entire process using Airflow running in Docker.

This project intentionally focuses on **clarity and fundamentals** rather than complexity, making it suitable as a first Airflow-based ETL project.

---

## Architecture

    External Weather API
            ↓
         Extract
            ↓
        Transform
            ↓
      Load / Validate
            ↓
       CSV Output

- **Orchestration**: Apache Airflow
- **Execution Environment**: Docker + Docker Compose
- **Storage**: Local filesystem (CSV)
- **Executor**: LocalExecutor
- **Metadata DB**: PostgreSQL (Airflow internal)

---

## Pipeline Description

The DAG `api_weather_etl` consists of three tasks:

1. **Extract**
   - Fetches hourly temperature data from the Open-Meteo API
   - Stores raw JSON data locally

2. **Transform**
   - Converts raw JSON into a structured CSV format
   - Extracts timestamp and temperature values
   - Performs basic schema validation

3. **Load / Validate**
   - Validates the processed CSV (row count and schema)
   - Ensures data quality before downstream usage

---

## Scheduling & Reliability

- **Schedule**: `@daily`
- **Retries**: 3
- **Retry Delay**: 5 minutes
- **Catchup**: Disabled

The pipeline is configured to retry automatically in case of transient failures (e.g. API issues), reflecting a realistic production setup.

---

## Project Structure

    airflow-mini-etl/
    ├── dags/
    │   └── api_etl_dag.py
    ├── etl/
    │   ├── extract.py
    │   ├── transform.py
    │   ├── load.py
    │   └── __init__.py
    ├── data/
    │   ├── raw_weather.json
    │   └── processed_weather.csv
    ├── docker-compose.yml
    └── README.md

---

## How to Run

1. Start the Airflow environment:
   `docker compose up -d`

2. Open Airflow UI:
   - http://localhost:8080
   - Username: `admin`
   - Password: `admin`

3. Trigger the DAG manually or wait for the scheduled run.

---

## What This Project Demonstrates

- Building an ETL pipeline using Apache Airflow
- Running Airflow locally with Docker
- DAG authoring with PythonOperator
- Task dependencies and orchestration
- Scheduling and retry configuration
- Debugging DAG import and execution issues

---

## Next Steps (Planned)

- Load processed data into PostgreSQL
- Use Airflow Connections and Hooks
- Introduce failure scenarios and alerting
- Refactor tasks using the TaskFlow API
