
import pendulum
from airflow.sdk import dag, task, Asset, get_current_context


# ── Shared Asset definition ────────────────────────────────────────────────
# Both DAGs reference the same Asset object.
# The URI is just a logical identifier — Airflow doesn't access it directly.
sample_data_asset = Asset("s3://my-bucket/sample-data-asset/data.json")


# ── Producer DAG ───────────────────────────────────────────────────────────
@dag(
    dag_id="simple_data_pipeline",
    schedule="* * * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["asset", "producer"],
)
def producer_dag():
    @task(outlets=[sample_data_asset])  # <-- marks the asset as updated on success
    def create_sample_data_asset() -> None:
        print("Processing sample_data_asset")

    create_sample_data_asset()


producer_dag()
