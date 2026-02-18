# ⚡ Airflow Mini ETL Project: Weather-Driven Energy Demand Pipeline

> Containerized data engineering pipeline modeling temperature-driven regional energy demand with reproducible SQL analytics and performance-aware schema design.

---

## 🚀 Overview

**Goal**  
Build a containerized, production-like ETL pipeline that transforms hourly weather data into daily, analytics-ready energy aggregates.

**Stack**  
Airflow · PostgreSQL · Docker · Python

**Key Highlights**
- Layered warehouse design (`raw → staging → mart`)
- Idempotent UPSERT with composite PK `(ts, region)`
- SQL-based data quality checks
- Composite index `(region, ts)` validated via `EXPLAIN`
- Reproducible business insights (`analysis_business.sql`)

---

## 🐳 Architecture

### Why Docker?

- Reproducible environment
- Service isolation (Airflow + PostgreSQL)
- Production-like orchestration
- One-command startup

![Architecture](https://github.com/user-attachments/assets/a36995d4-869c-4f00-9113-2869acb35501)

## 🏗 Orchestration

![Airflow DAG Success Flow](https://github.com/user-attachments/assets/6b2d58d8-a6f4-4aad-bab3-72baf7b6e0e5)

Main DAG: `weather_energy_daily_mart`

Pipeline steps:
1. Extract weather data
2. Generate synthetic energy load
3. Load → raw
4. Transform → staging
5. Aggregate → mart
6. Execute data quality checks

All tasks are idempotent and dependency-aware.

---

## 📊 Data Model

| Layer   | Purpose |
|----------|----------|
| `raw`    | Source traceability |
| `staging`| Standardized hourly data |
| `mart`   | Daily aggregated fact table |

Fact table: `mart.fact_energy_load_daily`


---

## 🛡 Data Quality

Automated SQL checks for:
- Duplicate & grain validation
- NULL enforcement
- Temperature sanity range
- Hour-over-hour anomaly detection
- Row-count drift monitoring

---

## ⚡ Performance

- Composite index on `(region, ts)`
- No sequential scans on time-window joins
- Execution time: ~0.7–1.4 ms

---

## 🧠 Business Insights

Derived via `sql/analysis_business.sql`.

- **Cold-shock events:** None observed (≤ -5°C hourly drop, n=0)
- **Peak demand hour:** 06:00 (across 3/3 regions)
- **Weekend effect:** -142.5 MW (~ -10.9%) vs weekdays

---

## 📁 Structure

```
airflow-mini-etl/
├── dags/
├── etl/
├── sql/
├── docs/
├── data/
└── docker-compose.yml
```

---

## 🚀 Quickstart

Start services:

        docker compose up -d

Access Airflow:  
http://localhost:8080  
(admin / admin)

Trigger DAG:  
`weather_energy_daily_mart`

Run analysis:

        cat sql/analysis_business.sql | docker exec -i weather_postgres psql -U airflow -d airflow

(Windows PowerShell)

        Get-Content sql/analysis_business.sql | docker exec -i weather_postgres psql -U airflow -d airflow

Reproducibility check:
- staging.energy_hourly_clean rowcount: 2160
- mart.fact_energy_load_daily rowcount: 90
- analysis output is stable across re-runs (rounded outputs in sql/analysis_business.sql)


---

## 🎯 Focus

This project emphasizes:
- Data pipeline architecture
- SQL-driven analytical reproducibility
- Performance-aware schema design
- Containerized deployment
