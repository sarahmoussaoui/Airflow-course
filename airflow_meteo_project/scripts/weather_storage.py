
import json
import os
from datetime import datetime, date


DATA_DIR = "/opt/airflow/scripts/data"


def save_records(records, filename):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, filename)

    # Convert datetime objects to strings for JSON serialization.
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            records,
            f,
            default=lambda value: ( 
                value.isoformat()
                if isinstance(value, (datetime, date))
                else str(value)
            ),
            indent=2,
        )

    return path


def read_records(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
