"""
Hourly DAG — Airflow 3
Uses CronTriggerTimetable with a 1-hour interval.
Single task that prints the data_interval_start and data_interval_end.
"""

import pendulum
from datetime import timedelta
from airflow.sdk import dag, task, get_current_context
from airflow.timetables.trigger import CronTriggerTimetable


@dag(
    dag_id="custom_interval",
    schedule=CronTriggerTimetable(
        "* * * * *",  # every minute
        timezone="UTC",
        interval=timedelta(days=1),  # data window = previous day (same time as now's time) → now
    ),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "timetable"],
)
def daily_interval_printer():
    @task()
    def print_interval() -> None:
        context = get_current_context()

        start = context["data_interval_start"]
        end = context["data_interval_end"]

        print(f"data_interval_start : {start}")
        print(f"data_interval_end   : {end}")
        total = int((end - start).total_seconds())
        hours, remainder = divmod(total, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"Window duration : {hours}h {minutes}m {seconds}s")

    print_interval()


daily_interval_printer()
