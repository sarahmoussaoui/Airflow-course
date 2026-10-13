
CREATE TABLE IF NOT EXISTS weather_forecast (
    id BIGSERIAL PRIMARY KEY, -- bigserial is a PostgreSQL-specific type that auto-increments 
    city VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    forecast_time TIMESTAMP NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL,  -- even if we're extracting on the same timezone, we use this type for robustness
    temperature_2m DOUBLE PRECISION,
    relative_humidity_2m INTEGER,
    precipitation DOUBLE PRECISION,
    apparent_temperature DOUBLE PRECISION,
    wind_speed_10m DOUBLE PRECISION,
    cloud_cover INTEGER,
    wind_direction_10m INTEGER,
    direct_radiation DOUBLE PRECISION,
    soil_temperature_0cm DOUBLE PRECISION,
    soil_moisture_0_to_1cm DOUBLE PRECISION,
    soil_moisture_1_to_3cm DOUBLE PRECISION,
    UNIQUE (city, forecast_time, extracted_at)
);
