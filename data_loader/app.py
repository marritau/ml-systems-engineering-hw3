import io
import os
import time
from pathlib import Path

import pandas as pd
import psycopg2


CSV_PATH = Path(os.getenv("LABELED_CSV_PATH", "/data/train.csv"))
WINDOW_START = pd.Timestamp("2019-01-01")
WINDOW_END = pd.Timestamp("2019-03-01")
COLUMNS = [
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2", "gender", "street",
    "one_city", "us_state", "post_code", "lat", "lon", "population_city", "jobs", "merchant_lat",
    "merchant_lon", "target",
]


def connect():
    for _ in range(30):
        try:
            return psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"), port=os.getenv("POSTGRES_PORT", "5432"),
                dbname=os.getenv("POSTGRES_DB", "fraud_detection"), user=os.getenv("POSTGRES_USER", "fraud_user"),
                password=os.getenv("POSTGRES_PASSWORD", "fraud_password"),
            )
        except psycopg2.OperationalError:
            time.sleep(2)
    raise RuntimeError("PostgreSQL is unavailable")


def load_csv() -> pd.DataFrame:
    """Read the labeled competition data once and assign stable source row identifiers."""
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Training CSV is not found: {CSV_PATH}")
    frame = pd.read_csv(CSV_PATH)
    missing = [name for name in COLUMNS if name not in frame.columns]
    if missing:
        raise ValueError("CSV columns are missing: " + ", ".join(missing))
    frame = frame.loc[:, COLUMNS].copy()
    frame["transaction_time"] = pd.to_datetime(frame["transaction_time"])
    frame.insert(0, "record_id", [f"source-{index}" for index in range(len(frame))])
    frame["batch_month"] = frame["transaction_time"].dt.to_period("M").dt.to_timestamp().dt.date
    return frame


def copy_frame(conn, frame: pd.DataFrame) -> None:
    """Insert the immutable source data into the labeled showcase in one transaction."""
    columns = ["record_id", *COLUMNS, "batch_month"]
    buffer = io.StringIO()
    frame.loc[:, columns].to_csv(buffer, index=False, header=False, date_format="%Y-%m-%d %H:%M:%S")
    buffer.seek(0)
    with conn.cursor() as cursor:
        cursor.copy_expert(
            f"COPY labeled_transactions ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV)", buffer
        )
    conn.commit()


def initialize_state(conn, frame: pd.DataFrame) -> None:
    """Create month pointers once; later runs retain them and never rewind the window."""
    next_supply = (frame["transaction_time"].max().to_period("M") + 1).to_timestamp().date()
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pipeline_state WHERE id = 1")
        if cursor.fetchone() is None:
            cursor.execute(
                """INSERT INTO pipeline_state
                (id, training_window_start, training_window_end, next_training_month, next_supply_month)
                VALUES (1, %s, %s, %s, %s)""",
                (WINDOW_START.date(), WINDOW_END.date(), (WINDOW_END + pd.DateOffset(months=1)).date(), next_supply),
            )
    conn.commit()


def main():
    conn = connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM labeled_transactions")
            populated = cursor.fetchone()[0] > 0
        frame = load_csv()
        if not populated:
            copy_frame(conn, frame)
        initialize_state(conn, frame)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
