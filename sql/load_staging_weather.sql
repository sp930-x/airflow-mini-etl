-- ============================================================
-- File: sql/load_staging_weather.sql
-- Purpose: Load & clean weather data from raw to staging (multi-region)
-- Grain: (ts, region)
-- ============================================================

-- 1) Ensure schema exists
CREATE SCHEMA IF NOT EXISTS staging;

-- 2) Ensure table exists with correct grain
CREATE TABLE IF NOT EXISTS staging.weather_hourly_clean (
  ts timestamptz NOT NULL,
  region text NOT NULL,
  temperature_2m double precision NOT NULL,
  day date NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (ts, region)
);

-- 3) Helpful indexes for time-range queries and joins
CREATE INDEX IF NOT EXISTS idx_stg_weather_ts
  ON staging.weather_hourly_clean (ts);

CREATE INDEX IF NOT EXISTS idx_stg_weather_region_ts
  ON staging.weather_hourly_clean (region, ts);

-- 4) Upsert from raw
INSERT INTO staging.weather_hourly_clean (ts, region, temperature_2m, day, updated_at)
SELECT
  w.time AS ts,
  w.region,
  w.temperature_2m,
  (w.time AT TIME ZONE 'UTC')::date AS day,
  now() AS updated_at
FROM raw.weather_hourly w
ON CONFLICT (ts, region) DO UPDATE
SET
  temperature_2m = EXCLUDED.temperature_2m,
  day = EXCLUDED.day,
  updated_at = EXCLUDED.updated_at;
