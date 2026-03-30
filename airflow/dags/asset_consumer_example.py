"""
Asset-Based DAGs — Airflow 3

producer_dag : runs every hour, processes data and updates an Asset
consumer_dag : no time-based schedule — triggered automatically by the
               scheduler whenever the Asset is updated by the producer
"""

import pendulum
from airflow.sdk import dag, task, Asset, get_current_context


# ── Shared Asset definition ────────────────────────────────────────────────
# Both DAGs reference the same Asset object.
# The URI is just a logical identifier — Airflow doesn't access it directly.
hourly_report = Asset("s3://my-bucket/hourly-report/data.json")


# ── Consumer DAG ───────────────────────────────────────────────────────────
@dag(
    dag_id="consumer_dag",
    schedule=[hourly_report],  # <-- triggered by asset, not by time
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["asset", "consumer"],
)
def consumer_dag():
    @task()
    def print_asset_details(triggering_asset_events=None) -> None:
        """
        triggering_asset_events is injected directly by Airflow as a context
        parameter — no get_current_context() or inlets= needed.
        """
        if not triggering_asset_events:
            print("No triggering asset events found.")
            return

        for asset, asset_events in triggering_asset_events.items():
            print("=" * 50)
            print(f"Asset URI : {asset.uri}")
            print("=" * 50)
            for event in asset_events:
                # dag_run = event.source_dag_run
                print(f"    Timestamp            : {event.timestamp}")
                print(f"    Extra                : {event.extra}")
                print(f"    Producer DAG ID      : {event.source_dag_id}")
                print(f"Full event detail: {event}")

    print_asset_details()


consumer_dag()
