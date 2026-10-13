
import sys
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow/scripts")

from extract_transform import CITIES, extract_city_weather
from load_weather import load_records
from weather_storage import save_records, read_records


@dag(
    dag_id="weather_pipeline",
    description="Extract, transform, and load hourly weather forecasts",
    schedule="0 * * * *",  # Every hour
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "weather",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["weather", "etl", "portfolio"],
)

def weather_pipeline():

    @task
    def extract_weather():
        all_records = []

        for city, (latitude, longitude) in CITIES.items():
            records = extract_city_weather(city, latitude, longitude)
            all_records.extend(records)

        from airflow.operators.python import get_current_context

        context = get_current_context()
        run_id = context["run_id"]

        # Make the run ID safe for use in a filename.
        safe_run_id = "".join(
            char if char.isalnum() or char in "-_" else "_"
            for char in run_id
        )

        path = save_records(
            all_records,
            f"weather_extracted_{safe_run_id}.json"
        )

        print(f"Saved {len(all_records)} records to {path}")

        # Airflow stores this small string in XCom.
        return path


    @task
    def load_weather(path):
        records = read_records(path)
        load_records(records)

    @task
    def check_data_quality():
        import os
        import psycopg

        connection_params = {
            "host": os.environ["POSTGRES_HOST"],
            "port": int(os.getenv("POSTGRES_PORT")),
            "dbname": os.environ["POSTGRES_DB"],
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
        }

        with psycopg.connect(**connection_params) as conn:
            with conn.cursor() as cur: # 2 hours because the DAG runs every hour, so we want to check for at least 2 hours of data
                cur.execute("""
                    SELECT COUNT(*)
                    FROM weather_forecast
                    WHERE extracted_at >= NOW() - INTERVAL '2 hours'
                """)
                recent_count = cur.fetchone()[0]

        if recent_count == 0:
            raise ValueError(
                "Data quality check failed: no recent forecast records"
            )

        print(f"Data quality check passed: {recent_count} recent rows")

    records_path = extract_weather()
    loaded = load_weather(records_path)
    loaded >> check_data_quality()


weather_pipeline()
