import os
import csv
from pathlib import Path
from typing import List, Tuple

import psycopg2
from psycopg2.extras import execute_values


# ============================================================
# Project: Airflow + Postgres mini ETL (Energy load x Weather)
# File: etl/load.py
# Purpose:
#   1) Validate processed CSV
#   2) Load processed CSV into Postgres raw.weather_hourly (idempotent upsert)
# Notes:
#   - Supports CSV with or without "region" column (falls back to default_region)
#   - Expects CSV columns at least: time, temperature_2m
#   - Postgres connection comes from env vars:
#       PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
# ============================================================


# =========================
# 1) CSV VALIDATION
# =========================
def validate_processed_csv(processed_path: str) -> str:
    f = Path(processed_path)

    if not f.exists():
        raise FileNotFoundError(f"Processed file not found: {processed_path}")
    if f.stat().st_size < 10:
        raise ValueError(f"Processed file seems too small: {processed_path}")

    with f.open("r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)

    if len(rows) < 200:
        raise ValueError(f"Too few rows: {len(rows)} (expected >= 200)")

    for r in rows:
        if not r.get("time"):
            raise ValueError("Missing time value")
        if not r.get("temperature_2m"):
            raise ValueError("Missing temperature value")

        temp = float(r["temperature_2m"])
        if temp < -50 or temp > 60:
            raise ValueError(f"Temperature out of expected range: {temp}")

    return f"OK: {processed_path} (rows={len(rows)})"


# =========================
# 2) POSTGRES CONNECTION
# =========================
def _pg_conn():
    """
    docker-compose env example:
      PGHOST=postgres
      PGPORT=5432
      PGDATABASE=airflow
      PGUSER=airflow
      PGPASSWORD=airflow
    """
    return psycopg2.connect(
        host=os.getenv("PGHOST", "postgres"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "airflow"),
        user=os.getenv("PGUSER", "airflow"),
        password=os.getenv("PGPASSWORD", "airflow"),
    )


# =========================
# 3) RAW TABLE BOOTSTRAP
# =========================
def ensure_raw_weather_hourly() -> None:
    """
    Ensure raw.weather_hourly exists with the columns required by:
      - sql/staging/stg_weather.sql (expects: time, region, temperature_2m)
      - sql/raw/generate_energy_hourly.sql (expects: time, region, temperature_2m)
    """
    ddl = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.weather_hourly (
        time            timestamptz NOT NULL,
        region          text        NOT NULL,
        temperature_2m  double precision NOT NULL,
        created_at      timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (time, region)
    );

    CREATE INDEX IF NOT EXISTS idx_raw_weather_time
      ON raw.weather_hourly (time);

    CREATE INDEX IF NOT EXISTS idx_raw_weather_region_time
      ON raw.weather_hourly (region, time);
    """
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)


# =========================
# 4) CSV → RAW LOAD (UPSERT)
# =========================
def _read_processed_weather(csv_path: str, default_region: str = "DE") -> List[Tuple[str, str, float]]:
    """
    Reads processed weather rows from CSV.

    CSV must have:
      - time
      - temperature_2m
    CSV may have:
      - region  (if missing, default_region is used)

    Returns:
      List[(time, region, temperature_2m)]
    """
    f = Path(csv_path)
    if not f.exists():
        raise FileNotFoundError(f"Processed file not found: {csv_path}")

    rows: List[Tuple[str, str, float]] = []
    with f.open("r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for r in reader:
            t = r.get("time")
            temp = r.get("temperature_2m")

            region = r.get("region")
            region = (region if region and region.strip() else default_region).strip()

            if not t or temp is None or temp == "":
                continue

            rows.append((t, region, float(temp)))

    return rows


def load_weather_hourly_to_raw(csv_path: str, default_region: str = "DE") -> str:
    """
    Load processed_weather.csv into raw.weather_hourly using idempotent upsert.

    Args:
      csv_path: path to processed CSV
      default_region: used if CSV doesn't include region column

    Upsert key:
      (time, region)
    """
    ensure_raw_weather_hourly()

    rows = _read_processed_weather(csv_path, default_region=default_region)
    if len(rows) == 0:
        raise ValueError("No rows found in processed weather CSV")

    sql = """
        INSERT INTO raw.weather_hourly (time, region, temperature_2m)
        VALUES %s
        ON CONFLICT (time, region) DO UPDATE
        SET temperature_2m = EXCLUDED.temperature_2m;
    """

    with _pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)

    return f"OK: loaded {len(rows)} rows into raw.weather_hourly from {csv_path}"


# =========================
# 5) LOCAL TEST
# =========================
if __name__ == "__main__":
    path = "data/processed_weather.csv"
    print(validate_processed_csv(path))
    print(load_weather_hourly_to_raw(path))