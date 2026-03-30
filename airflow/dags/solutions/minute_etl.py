from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="minute_etl",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["exercise", "parallel"],
)
def parallel_pipeline():
    """
    Runs task_a and task_b in parallel, then task_c once both complete.
    """

    @task()
    def task_a():
        print("Running task_a")

    @task()
    def task_b():
        print("Running task_b")

    @task()
    def task_c():
        print(f"Running task_c with inputs: {result_a}, {result_b}")

    [task_a(), task_b()] >> task_c()


parallel_pipeline()