-- ============================================================
-- Project: Airflow + Postgres mini ETL (Energy load x Weather)
-- File: sql/tests/test_quality_checks.sql
-- Layer: Tests
-- Purpose:
--   Data quality checks (nulls, duplicates, value ranges, row counts).
-- Targets:
--   raw.weather_hourly
--   raw.energy_load_hourly
--   staging.weather_hourly_clean
--   staging.energy_hourly_clean
--   mart.dim_date
--   mart.dim_region
--   mart.fact_energy_load_daily
-- Notes:
--   Designed to fail loudly (returns violating rows / raises via wrapper).
-- ============================================================


-- QC1) Duplicate / PK-grain check (staging energy)
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT (ts, region)) AS distinct_grain,
  COUNT(*) - COUNT(DISTINCT (ts, region)) AS duplicate_rows
FROM staging.energy_hourly_clean;

-- QC2) Duplicate / PK-grain check (staging weather)
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT (ts, region)) AS distinct_grain,
  COUNT(*) - COUNT(DISTINCT (ts, region)) AS duplicate_rows
FROM staging.weather_hourly_clean;

-- QC3) NULL violations (staging energy)
SELECT COUNT(*) AS null_violations
FROM staging.energy_hourly_clean
WHERE ts IS NULL OR region IS NULL OR load_mw IS NULL OR day IS NULL;

-- QC4) Range checks (weather + energy)
-- Weather reasonable range (Germany): [-40, 45] Celsius
SELECT COUNT(*) AS bad_temp_rows
FROM staging.weather_hourly_clean
WHERE temperature_2m < -40 OR temperature_2m > 45;

-- Energy reasonable range (synthetic): [0, 5000] MW (wide bounds)
SELECT COUNT(*) AS bad_load_rows
FROM staging.energy_hourly_clean
WHERE load_mw < 0 OR load_mw > 5000;

-- QC5) Outlier check: hourly load spikes/drops (per region)
-- Flag top changes using a robust threshold.
WITH deltas AS (
  SELECT
    ts,
    region,
    load_mw,
    load_mw - LAG(load_mw, 1) OVER (PARTITION BY region ORDER BY ts) AS delta_mw
  FROM staging.energy_hourly_clean
)
SELECT *
FROM deltas
WHERE delta_mw IS NOT NULL
  AND ABS(delta_mw) >= 400  -- threshold: tune if needed
ORDER BY ABS(delta_mw) DESC
LIMIT 20;

-- QC6) Row count drift check vs expected (30 days * 24 * 3 regions = 2160)
-- This is intentionally strict for the portfolio dataset.
SELECT
  COUNT(*) AS actual_rows,
  2160     AS expected_rows,
  COUNT(*) - 2160 AS diff
FROM staging.energy_hourly_clean;

-- QC7) Mart grain check (daily * region)
SELECT
  COUNT(*) AS fact_rows,
  COUNT(DISTINCT (day, region)) AS distinct_grain,
  COUNT(*) - COUNT(DISTINCT (day, region)) AS duplicate_rows
FROM mart.fact_energy_load_daily;
