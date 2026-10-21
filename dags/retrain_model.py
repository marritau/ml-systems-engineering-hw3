"""Airflow graph for scheduled time-aware model retraining."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.retrain.tasks import (
    check_quality,
    extract_training_window,
    preprocess_window,
    publish_candidate,
    split_by_time,
    train_candidate,
)


@dag(
    dag_id="retrain_model",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "retraining"],
)
def retrain_model():
    """Connect independent preparation, training, quality and publication tasks."""
    window = task(extract_training_window, task_id="extract_training_window")()
    split = task(split_by_time, task_id="split_by_time")(window)
    preprocessed = task(preprocess_window, task_id="preprocess_window")(split)
    candidate = task(train_candidate, task_id="train_candidate")(preprocessed)
    approved = task(check_quality, task_id="check_quality")(candidate)
    task(publish_candidate, task_id="publish_candidate")(approved)


retrain_model()
