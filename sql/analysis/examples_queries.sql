-- ============================================================
-- Project: Airflow + Postgres mini ETL (Energy load x Weather)
-- File: sql/analysis/examples_queries.sql
-- Layer: Analysis (Exploratory / showcase queries)
-- Purpose:
--   Demonstrate analytical SQL patterns (joins + window functions)
-- Sources:
--   raw.weather_hourly
--   raw.energy_load_hourly
-- Inputs (used in this file):
--   staging.weather_hourly_clean (ts, region)
--   staging.energy_hourly_clean  (ts, region)
-- Notes:
--   Not executed by the Airflow pipeline (for exploration only).
-- ============================================================



-- Q1) Base join: hourly energy load with hourly weather by (timestamp, region)
-- Use-case: build a single "analysis view" to query correlations / anomalies.
SELECT
  e.ts,
  e.region,
  e.load_mw,
  w.temperature_2m,
  e.day
FROM staging.energy_hourly_clean e
JOIN staging.weather_hourly_clean w
  ON w.ts = e.ts AND w.region = e.region
ORDER BY e.ts, e.region
LIMIT 48;


-- Q2) Rolling average (24h) of load per region
-- Use-case: smooth short-term noise and inspect trend.
SELECT
  ts,
  region,
  load_mw,
  AVG(load_mw) OVER (
    PARTITION BY region
    ORDER BY ts
    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
  ) AS load_roll24h_mw
FROM staging.energy_hourly_clean
ORDER BY ts, region;


-- Q3) Lag + delta (hour-over-hour change) per region
-- Use-case: detect sudden jumps/drops; feature engineering for forecasting.
SELECT
  ts,
  region,
  load_mw,
  LAG(load_mw, 1) OVER (PARTITION BY region ORDER BY ts) AS load_prev_mw,
  load_mw - LAG(load_mw, 1) OVER (PARTITION BY region ORDER BY ts) AS load_delta_mw
FROM staging.energy_hourly_clean
ORDER BY ts, region;


-- Q4) Rank: top-N peak hours within a day (per region)
-- Use-case: find peak demand times; useful for operations / capacity planning.
WITH ranked AS (
  SELECT
    day,
    ts,
    region,
    load_mw,
    DENSE_RANK() OVER (
      PARTITION BY day, region
      ORDER BY load_mw DESC
    ) AS load_rank_in_day
  FROM staging.energy_hourly_clean
)
SELECT *
FROM ranked
WHERE load_rank_in_day <= 3
ORDER BY day DESC, region, load_rank_in_day, ts;


-- Q5) CASE + aggregation: temperature buckets vs average load
-- Use-case: explain how demand changes under cold/comfortable/hot conditions.
WITH joined AS (
  SELECT
    e.region,
    e.load_mw,
    w.temperature_2m
  FROM staging.energy_hourly_clean e
  JOIN staging.weather_hourly_clean w
    ON w.ts = e.ts AND w.region = e.region
),
bucketed AS (
  SELECT
    region,
    load_mw,
    CASE
      WHEN temperature_2m < 0 THEN 'below_0'
      WHEN temperature_2m < 10 THEN '0_to_10'
      WHEN temperature_2m < 20 THEN '10_to_20'
      ELSE '20_plus'
    END AS temp_bucket
  FROM joined
)
SELECT
  region,
  temp_bucket,
  COUNT(*) AS n_hours,
  AVG(load_mw) AS avg_load_mw,
  MIN(load_mw) AS min_load_mw,
  MAX(load_mw) AS max_load_mw
FROM bucketed
GROUP BY region, temp_bucket
ORDER BY region, temp_bucket;


-- Q6) "Cold-shock" detection: large temperature drop + load spike (same hour)
-- Definition (tunable):
--   - temp_drop <= -3.0 (vs previous hour, same region)
--   - load_delta >= +5.0 (vs previous hour, same region)
WITH joined AS (
  SELECT
    e.ts,
    e.region,
    e.load_mw,
    w.temperature_2m
  FROM staging.energy_hourly_clean e
  JOIN staging.weather_hourly_clean w
    ON w.ts = e.ts AND w.region = e.region
),
features AS (
  SELECT
    ts,
    region,
    load_mw,
    temperature_2m,
    temperature_2m - LAG(temperature_2m, 1) OVER (PARTITION BY region ORDER BY ts) AS temp_delta_c,
    load_mw - LAG(load_mw, 1) OVER (PARTITION BY region ORDER BY ts) AS load_delta_mw
  FROM joined
)
SELECT
  ts,
  region,
  temperature_2m,
  temp_delta_c,
  load_mw,
  load_delta_mw
FROM features
WHERE temp_delta_c <= -3.0
  AND load_delta_mw >= 5.0
ORDER BY ts, region;