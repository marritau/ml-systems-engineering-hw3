import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from airflow.exceptions import AirflowSkipException
from catboost import CatBoostClassifier
from dateutil.relativedelta import relativedelta
from sklearn.metrics import roc_auc_score

from pipeline.db import get_conn, month_end, read_state
from pipeline.settings import FRAUD_SRC, MODELS_DIR, RUNTIME_DIR, WINDOW_MONTHS


def extract_training_window() -> dict:
    """Read the next complete month plus prior months and write a raw window parquet.

    The task reads pipeline_state and labeled_transactions, writes only a deterministic
    temporary parquet, and does not advance a pointer, so a retry cannot skip a month.
    """
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    try:
        state = read_state(conn)
        latest = state["next_training_month"]
        first = latest - relativedelta(months=WINDOW_MONTHS - 1)
        frame = pd.read_sql(
            "SELECT * FROM labeled_transactions WHERE transaction_time >= %s AND transaction_time < %s ORDER BY transaction_time",
            conn, params=(first, month_end(latest)),
        )
    finally:
        conn.close()
    periods = pd.to_datetime(frame["transaction_time"]).dt.to_period("M") if not frame.empty else []
    if frame.empty or periods.nunique() < WINDOW_MONTHS:
        raise AirflowSkipException(f"Window ending {latest} is not complete yet")
    path = RUNTIME_DIR / f"window_{latest:%Y%m}.parquet"
    frame.to_parquet(path, index=False)
    return {"window_path": str(path), "latest_month": latest.isoformat()}


def split_by_time(window_info: dict) -> dict:
    """Split the raw window into earlier train months and the newest validation month.

    Reads the immutable temporary window and writes raw train/test parquet files; reruns
    overwrite the same files and do not affect PostgreSQL state.
    """
    frame = pd.read_parquet(window_info["window_path"])
    times = pd.to_datetime(frame["transaction_time"])
    latest_period = times.dt.to_period("M").max()
    train, test = frame.loc[times.dt.to_period("M") < latest_period], frame.loc[times.dt.to_period("M") == latest_period]
    if train.empty or test.empty:
        raise RuntimeError("Time split produced an empty train or validation set")
    train_path, test_path = RUNTIME_DIR / "train_raw.parquet", RUNTIME_DIR / "test_raw.parquet"
    train.to_parquet(train_path, index=False)
    test.to_parquet(test_path, index=False)
    return {"train_path": str(train_path), "test_path": str(test_path), "latest_month": window_info["latest_month"]}


def preprocess_window(split_info: dict) -> dict:
    """Build preprocessing statistics from train only and transform train and validation.

    The task reads raw split files and writes staged processed files plus an artifact.
    Statistics never use the newest validation month, preventing time leakage on retry.
    """
    if FRAUD_SRC not in sys.path:
        sys.path.insert(0, FRAUD_SRC)
    from preprocessing import build_preprocessing_artifact, run_preproc, save_artifact
    train, test = pd.read_parquet(split_info["train_path"]), pd.read_parquet(split_info["test_path"])
    artifact = build_preprocessing_artifact(train)
    train_proc, test_proc = run_preproc(artifact, train), run_preproc(artifact, test)
    train_proc["target"] = train["target"].to_numpy()
    test_proc["target"] = test["target"].to_numpy()
    train_path, test_path, artifact_path = RUNTIME_DIR / "train_proc.parquet", RUNTIME_DIR / "test_proc.parquet", RUNTIME_DIR / "candidate_preprocessing.json"
    train_proc.to_parquet(train_path, index=False)
    test_proc.to_parquet(test_path, index=False)
    save_artifact(artifact, artifact_path)
    return {"train_path": str(train_path), "test_path": str(test_path), "artifact_path": str(artifact_path), "latest_month": split_info["latest_month"]}


def train_candidate(preproc_info: dict) -> dict:
    """Fit a CPU CatBoost candidate and write a staged binary without publishing it.

    The task reads transformed train/test data, stores a versioned temporary model, and
    leaves latest.json untouched so failure or retry cannot change online scoring.
    """
    train, test = pd.read_parquet(preproc_info["train_path"]), pd.read_parquet(preproc_info["test_path"])
    x_train, y_train = train.drop(columns="target"), train["target"]
    x_test, y_test = test.drop(columns="target"), test["target"]
    categorical_features = [name for name in x_train.columns if name.endswith("_cat")]
    model = CatBoostClassifier(
        iterations=80, depth=6, learning_rate=0.1, loss_function="Logloss", eval_metric="AUC",
        auto_class_weights="Balanced", early_stopping_rounds=15, random_seed=42, allow_writing_files=False,
        thread_count=1, verbose=False,
    )
    model.fit(
        x_train,
        y_train,
        cat_features=categorical_features,
        eval_set=(x_test, y_test),
        use_best_model=True,
    )
    probabilities = model.predict_proba(x_test)[:, 1]
    roc_auc = float(roc_auc_score(y_test, probabilities)) if y_test.nunique() == 2 else None
    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    staging = MODELS_DIR / f".candidate_{version}.cbm"
    model.save_model(staging)
    return {**preproc_info, "version": version, "staging_model": str(staging), "roc_auc": roc_auc,
            "train_rows": int(len(train)), "test_rows": int(len(test))}


def check_quality(candidate: dict) -> dict:
    """Compare the candidate ROC-AUC with the deployed version before publication.

    The task reads state only. A rejected candidate raises skip and therefore keeps the
    current model and month pointer unchanged; an execution retry is safe.
    """
    conn = get_conn()
    try:
        state = read_state(conn)
    finally:
        conn.close()
    baseline, score = state["published_roc_auc"], candidate["roc_auc"]
    if baseline is not None and (score is None or score < baseline):
        Path(candidate["staging_model"]).unlink(missing_ok=True)
        raise AirflowSkipException(f"Candidate ROC-AUC {score} is below deployed {baseline}")
    return candidate


def publish_candidate(candidate: dict) -> str:
    """Atomically publish model/artifact, then advance the training month in PostgreSQL.

    It reads staged files, writes immutable versioned files and latest.json, then updates
    pipeline_state in one database transaction. A retry finds the final files and never
    advances the month before a publish succeeded.
    """
    version = candidate["version"]
    final_model = MODELS_DIR / f"model_{version}.cbm"
    final_artifact = MODELS_DIR / f"preprocessing_{version}.json"
    staging = Path(candidate["staging_model"])
    if staging.exists():
        os.replace(staging, final_model)
    elif not final_model.exists():
        raise FileNotFoundError(staging)
    staging_artifact = Path(candidate["artifact_path"])
    if staging_artifact.exists():
        temporary_artifact = MODELS_DIR / f".preprocessing_{version}.tmp"
        shutil.copyfile(staging_artifact, temporary_artifact)
        os.replace(temporary_artifact, final_artifact)
    elif not final_artifact.exists():
        raise FileNotFoundError(staging_artifact)
    metadata = {"version": version, "model_file": final_model.name, "artifact_file": final_artifact.name,
                "roc_auc": candidate["roc_auc"], "validation_month": candidate["latest_month"]}
    (MODELS_DIR / f"model_{version}.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    temporary = MODELS_DIR / ".latest.tmp"
    temporary.write_text(json.dumps(metadata), encoding="utf-8")
    os.replace(temporary, MODELS_DIR / "latest.json")
    current_month = datetime.fromisoformat(candidate["latest_month"]).date()
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """UPDATE pipeline_state SET training_window_start = %s, training_window_end = %s,
                   next_training_month = %s, published_model_version = %s, published_roc_auc = %s,
                   updated_at = CURRENT_TIMESTAMP WHERE id = 1""",
                (current_month - relativedelta(months=WINDOW_MONTHS - 1), current_month,
                 current_month + relativedelta(months=1), version, candidate["roc_auc"]),
            )
        conn.commit()
    finally:
        conn.close()
    return version
