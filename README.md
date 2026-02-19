# ⚡ Airflow Mini ETL Project: Weather-Driven Energy Demand Pipeline

> Containerized data engineering pipeline modeling temperature-driven regional energy demand with reproducible SQL analytics and performance-aware schema design.

---

## 🚀 Overview

**Goal**  
Build a containerized, production-like ETL pipeline that transforms hourly weather data into daily, analytics-ready energy aggregates.

**Stack**  
Airflow · PostgreSQL · Docker · Python

**Key Highlights**

- Layered warehouse design (raw → staging → mart)
- Idempotent UPSERT  
  - Staging grain: (ts, region)  
  - Mart grain: (day, region)
- Star schema (dim_date, dim_region, fact_energy_load_daily)
- SQL-based data quality checks
- Composite index (region, ts) validated via EXPLAIN
- Reproducible business insights (analysis_business.sql)

---

## 🐳 Architecture

<p align="center">
  <img src="https://github.com/user-attachments/assets/a36995d4-869c-4f00-9113-2869acb35501" width="75%">
</p>

---

## 🏗 Orchestration

<p align="center">
  <img src="https://github.com/user-attachments/assets/6b2d58d8-a6f4-4aad-bab3-72baf7b6e0e5" width="75%">
</p>

Main DAG: weather_energy_daily_mart

Pipeline steps:
1. Extract weather data  
2. Generate synthetic energy load  
3. Load → raw  
4. Transform → staging  
5. Aggregate → mart  
6. Execute data quality checks  

All tasks are dependency-aware and idempotent via UPSERT (no hard deletes).

---

## 📊 Data Model

| Layer     | Purpose |
|-----------|----------|
| raw       | Source traceability |
| staging   | Standardized hourly data |
| mart      | Daily star-schema model |

Star Schema:
- mart.dim_date
- mart.dim_region
- mart.fact_energy_load_daily  
  (grain: day × region)

---

## 🛡 Data Quality

Automated SQL checks for:

- Duplicate & grain validation
- NULL enforcement
- Temperature sanity range
- Hour-over-hour anomaly detection
- Row-count drift monitoring
- Mart grain validation (day, region)

---

## ⚡ Performance

- Composite index on (region, ts)
- No sequential scans on time-window joins
- Execution time: ~0.7–1.4 ms (validated via EXPLAIN)

---

## 🧠 Business Insights

Derived via sql/analysis/analysis_business.sql.

- Cold-shock events: None observed (≤ -5°C hourly drop, n=0)
- Peak demand hour: 06:00 (across 3/3 regions)
- Weekend effect: -142.5 MW (~ -10.9%) vs weekdays

---

## 📁 Structure

    airflow-mini-etl/
    ├── dags/
    ├── etl/
    ├── sql/
    │   ├── raw/
    │   ├── staging/
    │   ├── mart/
    │   ├── tests/
    │   └── analysis/
    ├── docs/
    ├── data/
    └── docker-compose.yml

---

## 🚀 Quickstart

Start services:

    docker compose up -d

Access Airflow:
http://localhost:8080  
(admin / admin)

Trigger DAG:
weather_energy_daily_mart

Run analysis (macOS / Linux):

    cat sql/analysis/analysis_business.sql | docker exec -i weather_postgres psql -U airflow -d airflow

Windows PowerShell:

    Get-Content sql/analysis/analysis_business.sql | docker exec -i weather_postgres psql -U airflow -d airflow

---

## 🔁 Reproducibility Check

| Item | Expected |
|------|----------|
| Row count (staging.energy_hourly_clean) | 2160 |
| Row count (mart.fact_energy_load_daily) | 90 |
| Mart idempotency | Row count unchanged after re-run |
| Analysis stability | Outputs stable across re-runs |

---

## 🎯 Focus

This project emphasizes:

- Data pipeline architecture
- SQL-driven analytical reproducibility
- Star-schema modeling
- Performance-aware schema design
- Containerized deployment
