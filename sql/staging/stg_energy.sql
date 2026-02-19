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
--   Rebuildable (TRUNCATE + INSERT or CREATE OR REPLACE).
-- ============================================================

INSERT INTO staging.energy_hourly_clean (ts, region, load_mw, day)
SELECT
  ts,
  region,
  load_mw,
  (ts AT TIME ZONE 'UTC')::date AS day
FROM raw.energy_load_hourly
ON CONFLICT (ts, region)
DO UPDATE SET
  load_mw = EXCLUDED.load_mw,
  day = EXCLUDED.day,
  updated_at = now();
