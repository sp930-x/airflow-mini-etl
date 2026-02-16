# Airflow Mini ETL Project (Weather + Energy Pipeline)

## Overview

Energy demand is strongly influenced by external factors such as temperature, seasonality, and regional patterns. 
Understanding these relationships requires structured, reliable, and reproducible data pipelines that can transform raw time-series data into analytics-ready datasets.

This project implements an end-to-end data engineering workflow using **Apache Airflow** and **PostgreSQL**.

The pipeline:

- Extracts hourly weather data from a public API
- Generates synthetic hourly energy load data
- Loads both datasets into a PostgreSQL `raw` layer
- Standardizes and cleans data in a `staging` layer
- Builds a dimensional `mart` layer aggregated at **day × region** grain

The objective is not only to move data, but to design an orchestrated, idempotent, and warehouse-style pipeline that supports analytical queries such as peak demand analysis and temperature-driven load changes.


---

## Architecture

```
External Weather API
        ↓
     Extract (Airflow)
        ↓
Generate Energy (Synthetic)
        ↓
PostgreSQL (raw schema)
        ↓
PostgreSQL (staging schema)
        ↓
PostgreSQL (mart schema)
        ↓
Analytics-ready Fact Table
```

---

## Data Layers

### Raw Layer (`raw`)
Stores source-level data without structural modifications.

- `raw.weather_hourly`
- `raw.energy_load_hourly`

Purpose:
- Preserve original granularity
- Enable reprocessing if needed

---

### Staging Layer (`staging`)
Standardizes data types and grain.

- `staging.weather_hourly_clean`
- `staging.energy_hourly_clean`

Characteristics:
- Explicit primary keys
- `day` column normalized as `date`
- Idempotent upsert logic
- Ready for dimensional modeling

---

### Mart Layer (`mart`)
Dimensional warehouse layer for analytics.

#### Dimensions
- `mart.dim_date`
- `mart.dim_region`

#### Fact
- `mart.fact_energy_load_daily`

**Grain:**  
`day × region`

Aggregations:
- `avg_load_mw`
- `min_load_mw`
- `max_load_mw`
- `n_hours`

This enables analytical queries such as:
- Daily peak demand
- Cold-shock impact analysis
- Weekend vs weekday comparison

---

## Execution Plan & Index Strategy

Time-series queries were validated using `EXPLAIN ANALYZE`.

Observations:

- Range predicates on `ts` can leverage btree indexes.
- The primary key `(ts, region)` supports timestamp filtering.
- For small tables, PostgreSQL may choose sequential scans based on cost estimation.

Key takeaway:

> Index presence does not guarantee usage; PostgreSQL selects execution plans based on estimated cost.

---

## Orchestration (Airflow)

Airflow coordinates:

1. Weather extraction
2. Energy data generation
3. Raw loading
4. Staging upserts
5. Mart aggregation

All SQL transformation scripts are:

- Idempotent
- Safe for repeated execution
- Pipeline-ready

---

## Scheduling & Reliability

- **Schedule:** `@daily`
- **Retries:** 3
- **Retry Delay:** 5 minutes
- **Catchup:** Disabled

Designed to simulate a production-style retry and failure handling mechanism.

---

## Project Structure

```
airflow-mini-etl/
├── dags/
│   ├── api_etl_dag.py
├── etl/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
├── sql/
│   ├── load_staging_weather.sql
│   ├── load_staging_energy.sql
│   └── build_mart_energy_daily.sql
├── docs/
│   ├── query_analysis.md
│   ├── index_strategy.md
│   └── data_modeling.md
├── docker-compose.yml
└── README.md
```

---

## How to Run

### 1. Start Airflow

```
docker compose up -d
```

### 2. Open Airflow UI

```
http://localhost:8080
Username: admin
Password: admin
```

### 3. Trigger the DAG

Run the full weather + energy pipeline.

---

## What This Project Demonstrates

- Building an orchestrated ETL pipeline with Apache Airflow
- Layered warehouse modeling (raw → staging → mart)
- Dimensional modeling (fact & dimensions)
- Idempotent SQL transformations
- Index strategy validation with `EXPLAIN ANALYZE`
- Dockerized local data engineering environment
- Cost-based execution plan interpretation in PostgreSQL

---

## Future Extensions

- Add data quality checks as Airflow tasks
- Introduce dbt for transformation management
- Add alerting on validation failure
- Add dashboard layer (e.g., Metabase)
- Implement incremental loading logic
