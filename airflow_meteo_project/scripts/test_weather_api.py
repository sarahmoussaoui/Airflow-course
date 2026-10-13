
import requests
import json

URL = "https://api.open-meteo.com/v1/forecast"

params = {
    "latitude": 43.3,   # Marseille
    "longitude": 5.369999,
    "hourly": "temperature_2m,relative_humidity_2m,precipitation,apparent_temperature,precipitation,wind_speed_10m,cloud_cover,wind_direction_10m,direct_radiation,soil_temperature_0cm,soil_moisture_0_to_1cm,soil_moisture_1_to_3cm",
    "forecast_days": 1,
    "timezone": "auto",
}

response = requests.get(URL, params=params, timeout=30)
response.raise_for_status()

data = response.json()

print("API request successful!")
print("Location:", data.get("latitude"), data.get("longitude"))
print("Timezone:", data.get("timezone"))
print("Available fields:", list(data["hourly"].keys()))
print("First 3 timestamps:", data["hourly"]["time"][:3])
print("First 3 temperatures:", data["hourly"]["temperature_2m"][:3])

with open("scripts/data/test/weather_sample.json", "w", encoding="utf-8") as file:
    json.dump(data, file, indent=2, ensure_ascii=False)

print("Saved response to weather_sample.json")
