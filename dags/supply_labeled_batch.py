"""Airflow graph for idempotent monthly labeled data supply."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.supply.tasks import advance_supply_month, append_month_batch, prepare_month_batch, prune_history


@dag(
    dag_id="supply_labeled_batch",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "supply"],
)
def supply_labeled_batch():
    """Connect deterministic batch generation, insert, pruning and state advance."""
    prepared = task(prepare_month_batch, task_id="prepare_month_batch")()
    appended = task(append_month_batch, task_id="append_month_batch")(prepared)
    pruned = task(prune_history, task_id="prune_history")(appended)
    task(advance_supply_month, task_id="advance_supply_month")(pruned)


supply_labeled_batch()
