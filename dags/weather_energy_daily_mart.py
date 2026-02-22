from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DEFAULT_ARGS = {
    "owner": "sinyoung",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

SQL_DIR = Path("/opt/airflow/project/sql")


def read_sql(*relative_parts: str) -> str:
    return (SQL_DIR.joinpath(*relative_parts)).read_text(encoding="utf-8")


with DAG(
    dag_id="weather_energy_daily_mart",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 2, 1),
    schedule="@daily",
    catchup=False,
    tags=["warehouse", "postgres", "dq", "dbt"],
) as dag:

    # 1) staging weather (raw -> staging)
    stg_weather = PostgresOperator(
        task_id="stg_weather",
        postgres_conn_id="postgres_default",
        sql=read_sql("staging", "stg_weather.sql"),
    )

    # 2) generate synthetic energy (raw layer)
    gen_energy_hourly = PostgresOperator(
        task_id="gen_energy_hourly",
        postgres_conn_id="postgres_default",
        sql=read_sql("raw", "generate_energy_hourly.sql"),
    )

    # 3) staging energy (raw -> staging)
    stg_energy = PostgresOperator(
        task_id="stg_energy",
        postgres_conn_id="postgres_default",
        sql=read_sql("staging", "stg_energy.sql"),
    )

    # 4) build mart (dims + fact)
    build_fact_energy_load_daily = PostgresOperator(
        task_id="build_fact_energy_load_daily",
        postgres_conn_id="postgres_default",
        sql=read_sql("mart", "fact_energy_load_daily.sql"),
    )

    # 5) validation report
    validate_report = PostgresOperator(
        task_id="validate_report",
        postgres_conn_id="postgres_default",
        sql=read_sql("tests", "test_validation.sql"),
    )

    # 6) quality checks report
    quality_checks_report = PostgresOperator(
        task_id="quality_checks_report",
        postgres_conn_id="postgres_default",
        sql=read_sql("tests", "test_quality_checks.sql"),
    )

    # 7) FAIL-FAST checks
    check_weather_range = SQLCheckOperator(
        task_id="check_weather_range",
        conn_id="postgres_default",
        sql="""
            SELECT COUNT(*) = 0
            FROM staging.weather_hourly_clean
            WHERE temperature_2m < -40 OR temperature_2m > 45;
        """,
    )

    check_energy_no_duplicates = SQLCheckOperator(
        task_id="check_energy_no_duplicates",
        conn_id="postgres_default",
        sql="""
            SELECT COUNT(*) = COUNT(DISTINCT (ts, region))
            FROM staging.energy_hourly_clean;
        """,
    )

    check_rowcount_expected = SQLCheckOperator(
        task_id="check_rowcount_expected",
        conn_id="postgres_default",
        sql="""
            SELECT COUNT(*) = 2160
            FROM staging.energy_hourly_clean;
        """,
    )

    # 8) dbt (deps -> run -> test)
    DBT_IMAGE = "ghcr.io/dbt-labs/dbt-postgres:1.8.2"
    DBT_WORKDIR = "/opt/project"
    DBT_PROJECT_DIR = "/opt/project/weather_dbt"
    DBT_PROFILES_DIR = "/opt/project/.dbt"
    DOCKER_NETWORK = "airflow-mini-etl_default"

    host_project_dir = os.environ.get("HOST_PROJECT_DIR")
    if not host_project_dir:
        raise RuntimeError(
            "HOST_PROJECT_DIR env var is missing. Set it in .env and pass it to airflow-scheduler/webserver."
        )

    project_mount = Mount(
        source=host_project_dir,     # Windows host path (e.g. C:/Users/...)
        target="/opt/project",       # inside dbt container
        type="bind",
    )

    dbt_deps = DockerOperator(
        task_id="dbt_deps",
        image=DBT_IMAGE,
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        auto_remove=True,
        mounts=[project_mount],
        working_dir=DBT_WORKDIR,
        environment={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
        entrypoint="",
        command=f"bash -lc 'dbt deps --project-dir {DBT_PROJECT_DIR}'",
        mount_tmp_dir=False,
    )

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image=DBT_IMAGE,
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        auto_remove=True,
        mounts=[project_mount],
        working_dir=DBT_WORKDIR,
        environment={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
        entrypoint="",
        command=f"bash -lc 'dbt run --project-dir {DBT_PROJECT_DIR}'",
        mount_tmp_dir=False,
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image=DBT_IMAGE,
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        auto_remove=True,
        mounts=[project_mount],
        working_dir=DBT_WORKDIR,
        environment={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
        entrypoint="",
        command=f"bash -lc 'dbt test --project-dir {DBT_PROJECT_DIR}'",
        mount_tmp_dir=False,
    )

    # =========================
    # Pipeline flow
    # =========================
    stg_weather >> gen_energy_hourly >> stg_energy >> build_fact_energy_load_daily
    build_fact_energy_load_daily >> validate_report >> quality_checks_report

    quality_checks_report >> [
        check_weather_range,
        check_energy_no_duplicates,
        check_rowcount_expected,
    ]

    [check_weather_range, check_energy_no_duplicates, check_rowcount_expected] >> dbt_deps >> dbt_run >> dbt_test