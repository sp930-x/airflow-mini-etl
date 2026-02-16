-- Ensure schemas/tables exist (safe to run repeatedly)
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.dim_date (
  day date PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS mart.dim_region (
  region text PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS mart.fact_energy_load_daily (
  day date NOT NULL,
  region text NOT NULL,
  avg_load_mw double precision NOT NULL,
  min_load_mw double precision NOT NULL,
  max_load_mw double precision NOT NULL,
  n_hours integer NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (day, region),
  FOREIGN KEY (day) REFERENCES mart.dim_date(day),
  FOREIGN KEY (region) REFERENCES mart.dim_region(region)
);

-- Upsert dimensions
INSERT INTO mart.dim_date (day)
SELECT DISTINCT day
FROM (
  SELECT day FROM staging.weather_hourly_clean
  UNION
  SELECT day FROM staging.energy_hourly_clean
) d
ON CONFLICT (day) DO NOTHING;

INSERT INTO mart.dim_region (region)
SELECT DISTINCT region
FROM staging.energy_hourly_clean
ON CONFLICT (region) DO NOTHING;

-- Upsert fact
INSERT INTO mart.fact_energy_load_daily (
  day, region, avg_load_mw, min_load_mw, max_load_mw, n_hours
)
SELECT
  day,
  region,
  avg(load_mw) AS avg_load_mw,
  min(load_mw) AS min_load_mw,
  max(load_mw) AS max_load_mw,
  count(*)::int AS n_hours
FROM staging.energy_hourly_clean
GROUP BY day, region
ON CONFLICT (day, region)
DO UPDATE SET
  avg_load_mw = EXCLUDED.avg_load_mw,
  min_load_mw = EXCLUDED.min_load_mw,
  max_load_mw = EXCLUDED.max_load_mw,
  n_hours = EXCLUDED.n_hours,
  updated_at = now();
