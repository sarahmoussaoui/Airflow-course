# Airflow Course

Hands-on notes and example DAGs from learning [Apache Airflow](https://airflow.apache.org/): setup (Python venv and Docker), core concepts, scheduling, connections, hooks, sensors and custom Docker images.

## Repository structure

```
.
├── airflow_project/          # Airflow project: DAGs and Docker setup
├── Airflow cheatsheet.txt    # Step-by-step commands and notes
└── .gitattributes
```

The cheatsheet is the main reference. It points to the Python files in `airflow_project/` wherever it says "view python file".

## Topics covered

1. **Running Airflow**: in a Python virtual environment, or with Docker Compose (LocalExecutor)
2. **Core concepts**: DAGs, tasks, operators, execution date, task instances, DAG runs, task lifecycle
3. **Architecture**: webserver, scheduler, executor, workers, metadata database
4. **Writing DAGs**: `BashOperator`, `PythonOperator`, TaskFlow API
5. **Catchup and backfill**: replaying past runs from the scheduler container
6. **Scheduling**: cron expressions (see [crontab.guru](https://crontab.guru/))
7. **Database connections**: Postgres connection through the Airflow UI, `PostgresOperator`
8. **Installing Python packages in Airflow Docker**: image extending vs. image customising
9. **Sensors**: `S3KeySensor` against a local MinIO bucket
10. **Hooks**: `PostgresHook` and `S3Hook` to move data from Postgres to S3

## Quick start

### Option 1: Python virtual environment

```bash
python -m venv <name-env>
<name-env>\Scripts\activate          # Windows
export AIRFLOW_HOME=.                # PowerShell: $env:AIRFLOW_HOME = "."

airflow db init
airflow users create --username <username> --firstname <firstname> \
  --lastname <lastname> --role Admin --email <email>

airflow webserver -p 8080
airflow scheduler                    # in a second terminal
```

### Option 2: Docker Compose

```bash
curl -LfO "https://airflow.apache.org/docs/apache-airflow/2.10.0/docker-compose.yaml"
mkdir logs plugins dags
echo AIRFLOW_UID=50000 > .env

# In docker-compose.yaml: switch CeleryExecutor to LocalExecutor,
# comment out the Celery-related services, and set
# AIRFLOW__CORE__LOAD_EXAMPLES: "false"

docker compose up airflow-init
docker compose up -d
```

The UI is then available at <http://localhost:8080>.

To stop everything and remove volumes: `docker compose down -v`.

## Useful commands

| Task | Command |
| --- | --- |
| Backfill a DAG | `docker exec -it <scheduler_container_id> bash`, then `airflow dags backfill -s <start-date> -e <end-date> <dag_id>` |
| Restart webserver and scheduler after changing the image | `docker compose up -d --no-deps --build airflow-webserver airflow-scheduler` |
| Check an installed provider version | `pip list \| grep amazon` (or `grep postgre`) inside the scheduler container |

## Notes on the Docker setup

- **Postgres connection from Airflow:** expose port `5432:5432` on the Postgres service, then use `host.docker.internal` as the host in the Airflow connection form.
- **Extra Python packages:**
  - *Image extending:* add a `requirements.txt` and a `Dockerfile` based on the official Airflow image, then `docker build . --tag extending_airflow:latest` and point `docker-compose.yaml` to that tag.
  - *Image customising:* build from the Airflow source (`docker-context-files/`) with `--build-arg AIRFLOW_VERSION=...`.
- **MinIO as S3:** run MinIO in Docker (ports 9000 and 9001), create an `airflow` bucket, install `apache-airflow-providers-amazon`, then create an Amazon Web Services or HTTP connection with the MinIO endpoint (`http://host.docker.internal:9000`) in the connection's *Extra* field.
- **Windows and WSL2:** if Airflow runs out of memory, allocate more in `.wslconfig` (for example `memory=4GB`, `processors=2`), then run `wsl --shutdown` and restart Docker.

> **Security note:** the credentials used in the cheatsheet (Airflow admin password, MinIO root user and password) are local demo values only. Never reuse them outside a local environment.

