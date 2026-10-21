import os
from datetime import date

import pandas as pd
import psycopg2
from dateutil.relativedelta import relativedelta


LABELED_COLUMNS = [
    "record_id", "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2", "gender", "street",
    "one_city", "us_state", "post_code", "lat", "lon", "population_city", "jobs", "merchant_lat",
    "merchant_lon", "target", "batch_month",
]


def get_conn():
    """Open a connection to the shared labeled showcase and pipeline state."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"), port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "fraud_detection"), user=os.getenv("POSTGRES_USER", "fraud_user"),
        password=os.getenv("POSTGRES_PASSWORD", "fraud_password"),
    )


def month_end(month: date) -> date:
    return month + relativedelta(months=1)


def read_state(conn) -> dict:
    """Read month pointers without modifying them; callers advance only after publication."""
    with conn.cursor() as cursor:
        cursor.execute("""SELECT training_window_start, training_window_end, next_training_month,
                              next_supply_month, published_model_version, published_roc_auc
                       FROM pipeline_state WHERE id = 1""")
        row = cursor.fetchone()
    if row is None:
        raise RuntimeError("pipeline_state is empty; data-loader must finish first")
    return dict(zip(("window_start", "window_end", "next_training_month", "next_supply_month", "published_version", "published_roc_auc"), row))


def read_month(conn, month: date) -> pd.DataFrame:
    """Read one calendar month from the labeled showcase in chronological order."""
    return pd.read_sql(
        "SELECT " + ", ".join(LABELED_COLUMNS) + " FROM labeled_transactions "
        "WHERE transaction_time >= %s AND transaction_time < %s ORDER BY transaction_time",
        conn, params=(month, month_end(month)),
    )
