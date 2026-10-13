<!-- @format -->

# WeatherPulse — Automated Weather ETL Pipeline

WeatherPulse is an automated data engineering project that extracts hourly weather forecasts from the [Open-Meteo API](https://open-meteo.com/), processes the data, and loads it into PostgreSQL using Apache Airflow.

The project demonstrates an end-to-end ETL workflow with scheduled orchestration, JSON intermediate storage, Docker containerization, and data quality checks.

## Architecture

```text
Open-Meteo API
      |
      v
Extract hourly forecasts
(Marseille, Paris, Lyon)
      |
      v
Transform and prepare records
      |
      v
Save records to JSON
      |
      v
Pass file path through XCom
      |
      v
Load data into PostgreSQL
      |
      v
Validate recent records
```

## Tech Stack

- **Python** — API extraction, transformation, serialization, and database loading
- **Apache Airflow 3** — DAG orchestration, scheduling, dependencies, and retries
- **PostgreSQL 16** — Persistent weather data storage
- **Docker Compose** — Containerized services and networking
- **Open-Meteo API** — Hourly forecast data
- **JSON** — Intermediate storage between pipeline tasks
- **XCom** — Passing the JSON file path between Airflow tasks

## Project Structure

```text
airflow_meteo_project/
├── dags/
│   └── weather_dag.py          # Main Airflow DAG
├── scripts/
│   ├── extract_transform.py    # API extraction and transformation
│   ├── load_weather.py         # PostgreSQL loading
│   ├── weather_storage.py      # JSON file storage utilities
│   ├── create_table.sql        # Database schema
│   └── test_weather_api.py     # API testing
├── data/                       # Intermediate JSON files
├── docker-compose.yaml
├── .env                        # Local configuration (not committed)
└── README.md
```

## Pipeline Workflow

The `weather_pipeline` DAG is scheduled to run **every hour** using the cron expression `0 * * * *`.

1. **Extract:** Retrieve hourly weather forecasts for Marseille, Paris, and Lyon from Open-Meteo.
2. **Transform:** Prepare the records and normalize timestamps for storage.
3. **Store:** Serialize the extracted records into a JSON file on a shared Docker volume.
4. **Orchestrate:** Pass the JSON file path to the loading task through Airflow XCom rather than transferring the full dataset through XCom.
5. **Load:** Read the JSON file and insert records into PostgreSQL.
6. **Validate:** Query the database to confirm that recent forecast records exist.

The DAG uses task retries to handle transient failures and `catchup=False` to prevent automatic backfilling of missed scheduled intervals.

## Data Model

The `weather_forecast` table stores forecasts for multiple cities and extraction times.

Each record includes:

- **City:** Marseille, Paris, or Lyon
- **Forecast time:** The hour for which the forecast applies
- **Extraction time:** When the API data was retrieved
- **Weather variables:** Temperature, apparent temperature, relative humidity, precipitation, wind speed, wind direction, cloud cover, direct radiation, soil temperature, and soil moisture

The table uses a unique constraint on `(city, forecast_time, extracted_at)` to prevent duplicate records for the same city, forecast hour, and extraction timestamp.

### Why store both timestamps?

`forecast_time` identifies the time being predicted, while `extracted_at` records when the prediction was collected.

Keeping both timestamps allows successive forecast snapshots to be preserved, making it possible to analyze how predictions change between extraction runs.

## Getting Started

### Prerequisites

- Docker Desktop with Docker Compose
- Git
- An internet connection for accessing the Open-Meteo API

### 1. Clone the repository

```bash
git clone <your-repository-url>
cd airflow_meteo_project
```

### 2. Configure environment variables

Create a `.env` file containing the PostgreSQL connection settings expected by the project:

```dotenv
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=weatherdb
POSTGRES_USER=airflow
POSTGRES_PASSWORD=your_local_password
```

Ensure these variables are passed to the relevant Airflow containers through `docker-compose.yaml`, and that the target database exists.

**Never commit your `.env` file or real credentials.**

### 3. Start the services

```bash
docker compose up -d
```

Check that the containers are running:

```bash
docker compose ps
```

### 4. Run the pipeline

Open the Airflow UI at [http://localhost:8080](http://localhost:8080).

Unpause `weather_pipeline` to enable scheduled execution, or trigger it manually to test the workflow immediately.

### 5. Monitor execution

Follow the worker logs:

```bash
docker compose logs -f airflow-worker
```

Check the status of the DAG tasks in the Airflow UI. A successful run should complete extraction, loading, and data quality validation.

## Docker Volume Configuration

The pipeline uses a shared volume so that the extraction and loading tasks can access the same JSON file.

For example, the following mapping makes the local `data/` directory available inside the Airflow containers:

```yaml
- ./data:/opt/airflow/data
```

The `DATA_DIR` setting in `weather_storage.py` must match the corresponding container path. The volume must be available to every container that needs to read or write the files.

## Useful Commands

| Task                | Command                                 |
| ------------------- | --------------------------------------- |
| Start services      | `docker compose up -d`                  |
| Check services      | `docker compose ps`                     |
| Follow worker logs  | `docker compose logs -f airflow-worker` |
| Stop services       | `docker compose down`                   |
| Recreate containers | `docker compose up -d --force-recreate` |

## Future Improvements

- Add automated validation for missing values, duplicate records, and unexpected API responses.
- Store API responses and pipeline metadata for better observability.
- Add unit and integration tests.
- Build analytical dashboards using Power BI or another visualization tool.
- Compare historical forecasts with observed weather data to evaluate forecast accuracy.

## Security

Use environment variables for database credentials, exclude `.env` from version control, and use appropriate secrets management for production deployments.
