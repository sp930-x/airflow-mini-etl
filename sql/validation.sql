-- ============================================================
-- File: sql/validation.sql
-- Purpose: Sanity checks for marts / referential integrity
-- ============================================================

-- V1) Fact rowcount and basic grain check (should be unique per day+region due to PK)
SELECT
  COUNT(*) AS fact_rows,
  COUNT(DISTINCT (day, region)) AS distinct_grain
FROM mart.fact_energy_load_daily;

-- V2) Foreign key coverage check (should be zero because FK exists, but good as explicit validation)
-- If this returns rows > 0, something is inconsistent.
SELECT
  COUNT(*) AS missing_dim_keys
FROM mart.fact_energy_load_daily f
LEFT JOIN mart.dim_date d ON d.day = f.day
LEFT JOIN mart.dim_region r ON r.region = f.region
WHERE d.day IS NULL OR r.region IS NULL;

-- V3) n_hours sanity check (should match number of hourly rows aggregated per day+region)
-- For your current dataset (24 rows, 1 day, 1 region) -> n_hours should be 24.
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
