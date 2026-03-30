"""
Simple ETL DAG — Airflow 3
Extract fake data → Transform → Load (print)
"""

import pendulum
from airflow.sdk import dag, task
import time


@dag(
    dag_id="simple_etl",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "example"],
)
def simple_etl():
    
    @task(owner="ExternalSystem")
    def extract() -> list[dict]:
        """Generate fake raw records."""
        raw_data = [
            {"id": 1, "name": "alice", "amount": "100.5"},
            {"id": 2, "name": "bob", "amount": "200.0"},
            {"id": 3, "name": "charlie", "amount": "  50.75 "},
        ]
        print(f"Extracted {len(raw_data)} records")
        return raw_data

    @task(owner="StartDataEngineering", retries=2, retry_delay=2)
    def transform(raw: list[dict]) -> list[dict]:
        """Clean and type-cast the raw records."""
        transformed = [
            {
                "id": record["id"],
                "name": record["name"].strip().title(),
                "amount": round(float(record["amount"].strip()), 2),
            }
            for record in raw
        ]
        print(f"Transformed {len(transformed)} records")
        return transformed

    @task()
    def load(records: list[dict]) -> None:
        """Print records to simulate a load step."""
        print(f"Loading {len(records)} records:")
        for record in records:
            print(f"  → {record}")

    # Wire up the pipeline
    raw = extract()
    cleaned = transform(raw)
    load(cleaned)


simple_etl()
