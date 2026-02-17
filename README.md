# ⚡ Airflow Mini ETL Project: Weather-Driven Energy Demand Pipeline

> **An end-to-end data engineering workflow simulating German regional energy demand patterns, featuring robust data quality gates and query performance tuning.**

---

## 🚀 Project High-Level Summary
* **Objective:** Build an orchestrated ETL pipeline to analyze the correlation between regional temperature and energy load.
* **Stack:** Apache Airflow, PostgreSQL, Docker, Python (psycopg2).
* **Key Achievement:** Optimized join performance to **<2ms** via composite indexing and implemented **7 automated Data Quality gates**.
* **Domain Focus:** Modeled on German energy market dynamics (e.g., cold-sensitivity in North Rhine-Westphalia).

---

## 🏗 Architecture


1.  **Extract:** Hourly weather data from Open-Meteo API.
2.  **Generate:** Synthetic Energy Load based on temperature & seasonality.
3.  **Load:** Multi-stage loading into PostgreSQL (`raw` → `staging` → `mart`).
4.  **Validate:** Integrated Data Quality (DQ) checks and Execution Plan analysis.

---

## 📊 Data Modeling & Layers

| Layer | Schema | Purpose | Key Engineering Features |
| :--- | :--- | :--- | :--- |
| **Raw** | `raw` | Source Traceability | Original granularity preserved for full reprocessing. |
| **Staging** | `staging` | Standardization | **Idempotent** `UPSERT` logic, Composite PKs `(ts, region)`. |
| **Mart** | `mart` | Analytics-Ready | **Fact/Dim** model, Aggregated at `Day × Region` grain. |

---

## 🛡️ Data Quality & Reliability (Operational Layer)
To ensure **Production-Grade Reliability**, the pipeline executes automated SQL-based checks:

* **QC1-2 (Integrity):** Duplicate & PK-grain validation for Staging/Mart.
* **QC3 (Completeness):** Strict NULL violation checks on critical columns.
* **QC4 (Sanity):** Range validation (e.g., Germany temp range: -40°C to 45°C).
* **QC5 (Observability):** **Outlier Detection** via hour-over-hour load spike analysis (`LAG` window functions).
* **QC6 (Drift):** Row count drift check against expected counts (2,160 rows/cycle).

---

## ⚡ Performance Tuning: Execution Plan Inspection
I utilized `EXPLAIN (ANALYZE, BUFFERS)` to validate the indexing strategy for time-series workloads.

### **Query Optimization Outcome**
* **Pattern:** Regional time-window join between Energy and Weather tables.
* **Optimization:** Implemented a composite index on `(region, ts)`.
* **Impact:** * Avoided expensive Sequential Scans on filtered datasets.
    * Transitioned to efficient **Bitmap Index Scans**.
    * **Execution Time:** Stable at **~0.7ms - 1.4ms**.

> "This validates that the schema design scales naturally for larger time-series datasets."

---

## 🧠 Synthetic Energy Modeling Logic
To simulate realistic analytical scenarios, energy demand is modeled with:
* **Baseline:** 1000 MW constant demand.
* **Diurnal Cycle:** Sinusoidal pattern reflecting day/night usage.
* **Weekend Adjustment:** -15% demand reduction for non-business days.
* **Temperature Sensitivity:** Heat-pump/cooling load factors (`f(temp)`).

---

## 🛠️ How to Run
1.  **Clone & Start:**
    ```bash
    docker compose up -d
    ```
2.  **Access Airflow:**
    * URL: `http://localhost:8080` (admin/admin)
3.  **Trigger DAG:** `weather_energy_daily_mart`

---

## 📈 Potential Extensions
* [ ] **dbt Integration:** Refactor transformation logic into dbt models.
* [ ] **Alerting:** Slack/Email notifications on Quality Check failures.
* [ ] **Visualization:** Connect Metabase or Grafana for real-time dashboards.

---
**Contact:** [Your Name/Email] | [Your LinkedIn]