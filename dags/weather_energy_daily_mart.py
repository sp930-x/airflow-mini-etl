from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

DEFAULT_ARGS = {
    "owner": "sinyoung",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

SQL_DIR = Path("/opt/airflow/project/sql")


def read_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text(encoding="utf-8")


with DAG(
    dag_id="weather_energy_daily_mart",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 2, 1),
    schedule="@daily",
    catchup=False,
    tags=["warehouse", "postgres", "dq"],
) as dag:

    # 1) staging weather (raw -> staging)
    load_staging_weather = PostgresOperator(
        task_id="load_staging_weather",
        postgres_conn_id="postgres_default",
        sql=read_sql("load_staging_weather.sql"),
    )

    # 2) generate synthetic energy (based on raw weather)
    generate_energy = PostgresOperator(
        task_id="generate_energy_hourly",
        postgres_conn_id="postgres_default",
        sql=read_sql("generate_energy_hourly.sql"),
    )

    # 3) staging energy (raw -> staging)
    load_staging_energy = PostgresOperator(
        task_id="load_staging_energy",
        postgres_conn_id="postgres_default",
        sql=read_sql("load_staging_energy.sql"),
    )

    # 4) build mart (daily)
    build_mart = PostgresOperator(
        task_id="build_mart_daily",
        postgres_conn_id="postgres_default",
        sql=read_sql("build_mart_energy_daily.sql"),
    )

    # 5) validation report (prints metrics)
    validate_report = PostgresOperator(
        task_id="validate_report",
        postgres_conn_id="postgres_default",
        sql=read_sql("validation.sql"),
    )

    # 6) quality checks report (prints QC outputs / top outliers)
    quality_checks_report = PostgresOperator(
        task_id="quality_checks_report",
        postgres_conn_id="postgres_default",
        sql=read_sql("quality_checks.sql"),
    )

    # 7) FAIL-FAST checks (these should fail the DAG if violated)

    # Weather temperature range must be reasonable
    check_weather_range = SQLCheckOperator(
        task_id="check_weather_range",
        conn_id="postgres_default",
        sql="""
            SELECT COUNT(*) = 0
            FROM staging.weather_hourly_clean
            WHERE temperature_2m < -40 OR temperature_2m > 45;
        """,
    )

    # No duplicates vs PK grain in staging energy
    check_energy_no_duplicates = SQLCheckOperator(
        task_id="check_energy_no_duplicates",
        conn_id="postgres_default",
        sql="""
            SELECT COUNT(*) = COUNT(DISTINCT (ts, region))
            FROM staging.energy_hourly_clean;
        """,
    )

    # Optional strictness: row-count drift must match expected (portfolio dataset)
    check_rowcount_expected = SQLCheckOperator(
        task_id="check_rowcount_expected",
        conn_id="postgres_default",
        sql="""
            SELECT COUNT(*) = 2160
            FROM staging.energy_hourly_clean;
        """,
    )

    # Pipeline
    load_staging_weather >> generate_energy >> load_staging_energy >> build_mart

    # Reports first (so you always get metrics printed), then fail-fast checks
    build_mart >> validate_report >> quality_checks_report
    quality_checks_report >> [check_weather_range, check_energy_no_duplicates, check_rowcount_expected]
