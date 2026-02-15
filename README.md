# Airflow Mini ETL Project (Weather Data)

## Overview

This project is a minimal end-to-end ETL pipeline built with **Apache Airflow**, designed to demonstrate core data orchestration concepts using a simple workflow.

The pipeline extracts hourly weather data from a public API, transforms it into a structured CSV format, validates the output, and orchestrates the entire process using Airflow running in Docker.

In addition to file-based outputs, the processed data can also be loaded into a PostgreSQL database running in Docker, introducing a basic warehouse-style data layer.

---

## Architecture

```
External Weather API
        ↓
     Extract
        ↓
    Transform
        ↓
     Validate
        ↓
   CSV (Landing Layer)
        ↓
PostgreSQL (Raw Schema)
```

- **Orchestration**: Apache Airflow  
- **Execution Environment**: Docker + Docker Compose  
- **Landing Layer**: Local filesystem (JSON + CSV)  
- **Warehouse Layer**: PostgreSQL (Docker container)  
- **Executor**: LocalExecutor  
- **Metadata DB**: PostgreSQL (Airflow internal)  

---

## Pipeline Description

The DAG `api_weather_etl` consists of three tasks:

### 1. Extract
- Fetches hourly temperature data from the Open-Meteo API  
- Stores raw JSON data locally  

### 2. Transform
- Converts raw JSON into a structured CSV format  
- Extracts timestamp and temperature values  

### 3. Validate
- Validates the processed CSV (row count and schema)  
- Ensures data quality before downstream usage  

---

## PostgreSQL Layer

The processed CSV can be loaded into a PostgreSQL database running in Docker.

A separate schema is used to model data layers:

- `raw` → original ingested data  
- `staging` (planned) → cleaned & typed data  
- `mart` (planned) → aggregated / analytics-ready data  

Example structure:

```
Database: airflow
 ├── public
 └── raw
      └── weather_hourly_raw
```

This introduces a warehouse-style architecture and prepares the project for automated database loading in future iterations.

---

## Scheduling & Reliability

- **Schedule**: `@daily`  
- **Retries**: 3  
- **Retry Delay**: 5 minutes  
- **Catchup**: Disabled  

The pipeline is configured to retry automatically in case of transient failures (e.g. API issues), reflecting a realistic production setup.

---

## Project Structure

```
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
```

---

## How to Run

1. Start the Airflow environment:

```
docker compose up -d
```

2. Open Airflow UI:

```
http://localhost:8080
Username: admin
Password: admin
```

3. Trigger the DAG manually or wait for the scheduled run.

---

## What This Project Demonstrates

- Building an ETL pipeline using Apache Airflow  
- Running Airflow locally with Docker  
- DAG authoring with PythonOperator  
- Persisting intermediate artifacts (JSON & CSV)  
- Loading structured data into PostgreSQL  
- Using database schemas for logical data layer separation  
- Executing SQL (DDL & DML) for data management  

---

## Next Steps

- Automate PostgreSQL loading via an Airflow task  
- Introduce a `staging` schema with typed transformations  
- Add database-level data quality checks  
- Refactor tasks using the TaskFlow API  
