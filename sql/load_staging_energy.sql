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
