"""
Every-minute DAG — Airflow 3
Uses CronDataIntervalTimetable — the interval IS the cron period (1 minute).
Prints data_interval_start, data_interval_end, and window duration in seconds.
"""

import pendulum
from airflow.sdk import dag, task, get_current_context
from airflow.timetables.interval import CronDataIntervalTimetable


@dag(
    dag_id="minutely_interval_printer",
    schedule=CronDataIntervalTimetable(
        "* * * * *",  # every minute
        timezone=pendulum.timezone("UTC"),
    ),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "timetable"],
)
def minutely_interval_printer():
    @task()
    def print_interval() -> None:
        context = get_current_context()

        start = context["data_interval_start"]
        end = context["data_interval_end"]
        duration_seconds = (end - start).total_seconds()

        print(f"data_interval_start : {start}")
        print(f"data_interval_end   : {end}")
        print(f"Window duration     : {duration_seconds:.0f} seconds")
        # run script with data_time >= start and data_time < end

    print_interval()


minutely_interval_printer()
