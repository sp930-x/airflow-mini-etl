-- ============================================================
-- Project: Airflow + Postgres mini ETL (Energy load x Weather)
-- File: sql/tests/test_validation.sql
-- Layer: Tests
-- Purpose:
--   Sanity checks for mart outputs and referential integrity.
-- Targets:
--   mart.dim_date
--   mart.dim_region
--   mart.fact_energy_load_daily
-- ============================================================

-- V1) Fact rowcount and grain check (unique per day+region due to PK)
SELECT
  COUNT(*) AS fact_rows,
  COUNT(DISTINCT (day, region)) AS distinct_grain
FROM mart.fact_energy_load_daily;

-- V2) Foreign key coverage check (should be zero because FK exists)
SELECT
  COUNT(*) AS missing_dim_keys
FROM mart.fact_energy_load_daily f
LEFT JOIN mart.dim_date d ON d.day = f.day
LEFT JOIN mart.dim_region r ON r.region = f.region
WHERE d.day IS NULL OR r.region IS NULL;

-- V3) n_hours should equal the number of hourly rows aggregated per day+region
SELECT
  f.day,
  f.region,
  f.n_hours,
  h.hourly_rows
FROM mart.fact_energy_load_daily f
JOIN (
  SELECT day, region, COUNT(*) AS hourly_rows
  FROM staging.energy_hourly_clean
  GROUP BY day, region
) h
  ON h.day = f.day AND h.region = f.region
ORDER BY f.day DESC, f.region;

-- V4) Value bounds sanity check (avg between min and max; min <= max)
SELECT
  COUNT(*) AS bad_rows
FROM mart.fact_energy_load_daily
WHERE NOT (min_load_mw <= avg_load_mw AND avg_load_mw <= max_load_mw)
   OR min_load_mw > max_load_mw;

-- V5) Not-null check (should return 0)
SELECT
  COUNT(*) AS null_violations
FROM mart.fact_energy_load_daily
WHERE day IS NULL
   OR region IS NULL
   OR avg_load_mw IS NULL
   OR min_load_mw IS NULL
   OR max_load_mw IS NULL
   OR n_hours IS NULL
   OR updated_at IS NULL;
