-- ============================================================
-- Project: Airflow + Postgres mini ETL (Energy load x Weather)
-- File: sql/staging/stg_energy.sql
-- Layer: Staging
-- Purpose:
--   Clean and standardize raw energy load data for analytics and joins.
-- Input:
--   raw.energy_load_hourly
-- Output:
--   staging.energy_hourly_clean
-- Idempotency:
--   Upsertable (ON CONFLICT DO UPDATE).
-- ============================================================


-- 1) Ensure schema exists
CREATE SCHEMA IF NOT EXISTS staging;

-- 2) Ensure table exists with correct grain
CREATE TABLE IF NOT EXISTS staging.energy_hourly_clean (
  ts timestamptz NOT NULL,
  region text NOT NULL,
  load_mw double precision NOT NULL,
  day date NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (ts, region)
);

-- 3) Helpful indexes for time-range queries and joins
CREATE INDEX IF NOT EXISTS idx_stg_energy_ts
  ON staging.energy_hourly_clean (ts);

CREATE INDEX IF NOT EXISTS idx_stg_energy_region_ts
  ON staging.energy_hourly_clean (region, ts);

CREATE INDEX IF NOT EXISTS idx_stg_energy_day_region
  ON staging.energy_hourly_clean (day, region);

-- 4) Upsert from raw
INSERT INTO staging.energy_hourly_clean (ts, region, load_mw, day, updated_at)
SELECT
  e.ts,
  e.region,
  e.load_mw,
  (e.ts AT TIME ZONE 'UTC')::date AS day,
  now() AS updated_at
FROM raw.energy_load_hourly e
ON CONFLICT (ts, region)
DO UPDATE SET
  load_mw = EXCLUDED.load_mw,
  day = EXCLUDED.day,
  updated_at = EXCLUDED.updated_at;