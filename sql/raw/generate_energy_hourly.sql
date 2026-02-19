-- ============================================================
-- Project: Airflow + Postgres mini ETL (Energy load x Weather)
-- File: sql/raw/generate_energy_hourly.sql
-- Layer: Raw
-- Purpose:
--   Generate synthetic hourly energy load data and load into raw layer.
-- Output:
--   raw.energy_load_hourly
-- Idempotency:
--   Safe to re-run (define strategy used: TRUNCATE+INSERT or INSERT-only).
-- ============================================================


-- Make randomness deterministic
SELECT setseed(0.42);

TRUNCATE raw.energy_load_hourly;

INSERT INTO raw.energy_load_hourly (ts, region, load_mw)
SELECT
  w.time AS ts,
  w.region,
  1000
  + 300 * sin(extract(hour from w.time)/24.0 * 2 * pi())
  + CASE WHEN extract(isodow from w.time) IN (6,7) THEN -150 ELSE 0 END
  + CASE WHEN w.temperature_2m < 15 THEN (15 - w.temperature_2m) * 20 ELSE 0 END
  + CASE WHEN w.temperature_2m > 23 THEN (w.temperature_2m - 23) * 15 ELSE 0 END
  + random() * 50
FROM raw.weather_hourly w
ORDER BY w.time, w.region;
