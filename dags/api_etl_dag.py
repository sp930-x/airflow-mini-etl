from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.extract import extract_weather
from etl.transform import transform_raw_to_csv
from etl.load import validate_processed_csv

DEFAULT_ARGS = {
    "owner": "sinyoung",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

RAW_PATH = "data/raw_weather.json"
PROCESSED_PATH = "data/processed_weather.csv"

with DAG(
    dag_id="api_weather_etl",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 2, 1),
    schedule="@daily",
    catchup=False,
    tags=["mini", "etl"],
) as dag:


    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_weather,
        op_kwargs={"raw_path": RAW_PATH, "latitude": 51.4556, "longitude": 7.0116},
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_raw_to_csv,
        op_kwargs={"raw_path": RAW_PATH, "processed_path": PROCESSED_PATH},
    )

    load_task = PythonOperator(
        task_id="load_validate",
        python_callable=validate_processed_csv,
        op_kwargs={"processed_path": PROCESSED_PATH},
    )

    extract_task >> transform_task >> load_task