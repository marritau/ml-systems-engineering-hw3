"""Airflow graph that publishes a dataset quality HTML report."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.dq.tasks import build_quality_report


@dag(
    dag_id="data_quality",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "data-quality"],
)
def data_quality():
    """Publish a new HTML report without modifying the labeled showcase."""
    task(build_quality_report, task_id="build_quality_report")()


data_quality()
