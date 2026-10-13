
import os
from datetime import datetime
import psycopg
from dotenv import load_dotenv

from extract_transform import CITIES, extract_city_weather

load_dotenv()

INSERT_SQL = """
    INSERT INTO weather_forecast (
        city,
        latitude,
        longitude,
        forecast_time,
        extracted_at,
        temperature_2m,
        relative_humidity_2m,
        precipitation,
        apparent_temperature,
        wind_speed_10m,
        cloud_cover,
        wind_direction_10m,
        direct_radiation,
        soil_temperature_0cm,
        soil_moisture_0_to_1cm,
        soil_moisture_1_to_3cm
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (city, forecast_time, extracted_at)
    DO NOTHING;
"""
# it skips inserting the record if a record with the same city, forecast_time, and extracted_at already exists in the table.

def load_records(records):
    if not records:
        raise ValueError("No records to load")

    # Use PostgreSQL connection settings from environment variables.
    connection_params = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": int(os.getenv("POSTGRES_PORT")),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.environ["POSTGRES_PASSWORD"],
    }

    values = []

    for record in records:
        values.append((
            record["city"],
            record["latitude"],
            record["longitude"],
            datetime.fromisoformat(record["forecast_time"]),
            datetime.fromisoformat(record["extracted_at"]),
            record.get("temperature_2m"),
            record.get("relative_humidity_2m"),
            record.get("precipitation"),
            record.get("apparent_temperature"),
            record.get("wind_speed_10m"),
            record.get("cloud_cover"),
            record.get("wind_direction_10m"),
            record.get("direct_radiation"),
            record.get("soil_temperature_0cm"),
            record.get("soil_moisture_0_to_1cm"),
            record.get("soil_moisture_1_to_3cm"),
        ))

    # The connection context commits on success and rolls back on error.
    with psycopg.connect(**connection_params) as conn:
        with conn.cursor() as cur:
            cur.executemany(INSERT_SQL, values)

    print(f"Successfully processed {len(values)} records.")


def main():
    print("Starting weather loading...", flush=True)

    all_records = []

    for city, (latitude, longitude) in CITIES.items():
        print(f"Fetching data for {city}...", flush=True)

        records = extract_city_weather(city, latitude, longitude)
        all_records.extend(records)

        print(f"{city}: {len(records)} records extracted", flush=True)

    print(f"Total records to load: {len(all_records)}", flush=True)

    print("Connecting to PostgreSQL...", flush=True)
    load_records(all_records)

    print("Loading complete!", flush=True)

if __name__ == "__main__":
    main()
