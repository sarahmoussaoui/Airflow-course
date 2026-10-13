
import requests, time
from datetime import datetime, timezone

URL = "https://api.open-meteo.com/v1/forecast"

CITIES = {
    "Marseille": (43.2965, 5.3698),
    "Paris": (48.8566, 2.3522),
    "Lyon": (45.7640, 4.8357),
}

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "apparent_temperature",
    "wind_speed_10m",
    "cloud_cover",
    "wind_direction_10m",
    "direct_radiation",
    "soil_temperature_0cm",
    "soil_moisture_0_to_1cm",
    "soil_moisture_1_to_3cm",
]

REQUIRED_FIELDS = [
    "city",
    "latitude",
    "longitude",
    "forecast_time",
    "extracted_at",
]

NUMERIC_FIELDS = HOURLY_VARIABLES  # All hourly variables are numeric fields

def fetch_weather(url, params, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=30
                            )
            response.raise_for_status()
            return response

        except requests.RequestException as exc:
            if attempt == max_retries - 1:
                raise

            wait_seconds = 2 ** attempt
            print(
                f"Request failed: {exc}. "
                f"Retrying in {wait_seconds} seconds..."
            )
            time.sleep(wait_seconds)

def extract_city_weather(city, latitude, longitude):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(HOURLY_VARIABLES),
        "forecast_days": 1,
        "timezone": "auto",
    }

    response = fetch_weather(URL, params)
    data = response.json()

    hourly = data["hourly"]
    times = hourly["time"]

    # Validate that every variable has one value per timestamp.
    for variable in HOURLY_VARIABLES:
        if variable not in hourly:
            raise ValueError(f"Missing variable: {variable}")

        if len(hourly[variable]) != len(times):
            raise ValueError(
                f"Inconsistent array length for {variable} in {city}"
            )

    # One extraction timestamp per city/API response.
    extracted_at = datetime.now(timezone.utc)

    records = []

    for i, forecast_time in enumerate(times):
        record = {
            "city": city,
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "forecast_time": forecast_time,
            "extracted_at": extracted_at,
        }

        for variable in HOURLY_VARIABLES:
            record[variable] = hourly[variable][i]

        records.append(record)

    return records


def main():
    all_records = []

    for city, (latitude, longitude) in CITIES.items():
        records = extract_city_weather(city, latitude, longitude)
        all_records.extend(records)
        print(f"{city}: extracted {len(records)} hourly records")

    print(f"\nTotal records: {len(all_records)}")
    print("\nFirst transformed record:")
    print(all_records[0])


if __name__ == "__main__":
    main()
