INSERT INTO staging.weather_hourly_clean (ts, temperature_2m, day)
SELECT
  time AS ts,
  temperature_2m,
  (time AT TIME ZONE 'UTC')::date AS day
FROM raw.weather_hourly
ON CONFLICT (ts)
DO UPDATE SET
  temperature_2m = EXCLUDED.temperature_2m,
  day = EXCLUDED.day,
  updated_at = now();


