import json
from pathlib import Path

import numpy as np
import pandas as pd
from geopy.distance import great_circle


TARGET_COLUMN = "target"
INPUT_COLUMNS = [
    "transaction_time",
    "merch",
    "cat_id",
    "amount",
    "name_1",
    "name_2",
    "gender",
    "street",
    "one_city",
    "us_state",
    "post_code",
    "lat",
    "lon",
    "population_city",
    "jobs",
    "merchant_lat",
    "merchant_lon",
]
CATEGORICAL_COLUMNS = ["gender", "merch", "cat_id", "one_city", "us_state", "jobs"]
TIME_COLUMNS = ["hour", "year", "month", "day_of_month", "day_of_week"]
CONTINUOUS_COLUMNS = ["amount", "population_city", "distance"]
DROP_COLUMNS = ["name_1", "name_2", "street", "post_code"]
COORDINATE_COLUMNS = ["lat", "lon", "merchant_lat", "merchant_lon"]
MAX_FREQUENT_CATEGORIES = 50
FEATURE_ORDER = [
    "hour", "year", "month", "day_of_month", "day_of_week",
    "gender_cat", "merch_cat", "cat_id_cat", "one_city_cat", "us_state_cat", "jobs_cat",
    "gender_cat_mean_enc", "merch_cat_mean_enc", "cat_id_cat_mean_enc",
    "one_city_cat_mean_enc", "us_state_cat_mean_enc", "jobs_cat_mean_enc",
    "hour_mean_enc", "year_mean_enc", "month_mean_enc", "day_of_month_mean_enc",
    "day_of_week_mean_enc", "amount_log", "population_city_log", "distance_log",
]


def add_time_features(frame: pd.DataFrame) -> pd.DataFrame:
    times = pd.to_datetime(frame["transaction_time"], errors="raise")
    frame["hour"] = times.dt.hour
    frame["year"] = times.dt.year
    frame["month"] = times.dt.month
    frame["day_of_month"] = times.dt.day
    frame["day_of_week"] = times.dt.dayofweek
    return frame.drop(columns="transaction_time")


def add_distance(frame: pd.DataFrame) -> pd.DataFrame:
    frame["distance"] = frame.apply(
        lambda row: great_circle(
            (row["lat"], row["lon"]),
            (row["merchant_lat"], row["merchant_lon"]),
        ).km,
        axis=1,
    )
    return frame.drop(columns=COORDINATE_COLUMNS)


def _category_mapping(frame: pd.DataFrame, column: str) -> dict[str, str]:
    counts = frame.groupby(column, dropna=False)[TARGET_COLUMN].count().sort_values(ascending=False)
    mapping = {}
    for index, value in enumerate(counts.index):
        if pd.notna(value):
            mapping[str(value)] = f"cat_{min(index, MAX_FREQUENT_CATEGORIES)}"
    return mapping


def build_preprocessing_artifact(train: pd.DataFrame) -> dict:
    """Build statistics from the train part only to avoid test-month leakage."""
    if TARGET_COLUMN not in train.columns:
        raise ValueError("Training frame must contain target")
    missing = set(INPUT_COLUMNS).difference(train.columns)
    if missing:
        raise ValueError(f"Missing input columns: {sorted(missing)}")

    required = INPUT_COLUMNS
    work = add_time_features(train.drop(columns=DROP_COLUMNS).copy())
    mappings = {}
    for column in CATEGORICAL_COLUMNS:
        mappings[column] = _category_mapping(work, column)
        work[f"{column}_cat"] = work[column].astype("string").map(mappings[column]).fillna("cat_NAN")
    means = {}
    for column in [f"{name}_cat" for name in CATEGORICAL_COLUMNS] + TIME_COLUMNS:
        means[column] = {
            str(key): float(value)
            for key, value in work.groupby(column, dropna=False)[TARGET_COLUMN].mean().items()
            if pd.notna(key)
        }
    work = add_distance(work)
    return {
        "version": 1,
        "required_input_columns": required,
        "category_mappings": mappings,
        "mean_encodings": means,
        "continuous_means": {name: float(work[name].mean()) for name in CONTINUOUS_COLUMNS},
        "default_mean_encoding": float(train[TARGET_COLUMN].mean()),
        "feature_order": FEATURE_ORDER,
    }


def save_artifact(artifact: dict, path: Path) -> None:
    path.write_text(json.dumps(artifact, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def load_artifact(path: Path) -> dict:
    artifact = json.loads(path.read_text(encoding="utf-8"))
    if artifact.get("version") != 1:
        raise ValueError("Unsupported preprocessing artifact")
    return artifact


def run_preproc(artifact: dict, input_frame: pd.DataFrame) -> pd.DataFrame:
    """Transform online or offline raw transactions into the model feature order."""
    missing = [name for name in artifact["required_input_columns"] if name not in input_frame.columns]
    if missing:
        raise ValueError("Missing transaction fields: " + ", ".join(missing))
    frame = input_frame.loc[:, artifact["required_input_columns"]].copy()
    frame = frame.drop(columns=DROP_COLUMNS)
    frame = add_time_features(frame)
    for column in CATEGORICAL_COLUMNS:
        encoded = f"{column}_cat"
        frame[encoded] = frame[column].astype("string").map(artifact["category_mappings"][column]).fillna("cat_NAN")
        frame = frame.drop(columns=column)
    for column in [f"{name}_cat" for name in CATEGORICAL_COLUMNS] + TIME_COLUMNS:
        frame[f"{column}_mean_enc"] = (
            frame[column].astype(str).map(artifact["mean_encodings"][column]).fillna(artifact["default_mean_encoding"])
        )
    frame = add_distance(frame)
    for column in CONTINUOUS_COLUMNS:
        values = frame[column].fillna(artifact["continuous_means"][column])
        if (values < -1).any():
            raise ValueError(f"{column} contains values below -1")
        frame[f"{column}_log"] = np.log1p(values)
        frame = frame.drop(columns=column)
    return frame.loc[:, artifact["feature_order"]]
