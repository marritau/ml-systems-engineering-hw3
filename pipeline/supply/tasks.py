import calendar

import pandas as pd
from psycopg2.extras import execute_values

from pipeline.db import LABELED_COLUMNS, get_conn, read_state
from pipeline.settings import HISTORY_MONTHS, RUNTIME_DIR, SUPPLY_BATCH_SIZE


def prepare_month_batch() -> dict:
    """Sample a deterministic source batch and shift timestamps into the next supply month.

    Reads the supply pointer and showcase, writes a month-named parquet with stable record
    identifiers, and does not update state. A retry recreates identical rows.
    """
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    try:
        state = read_state(conn)
        target_month = state["next_supply_month"]
        frame = pd.read_sql(
            "SELECT * FROM labeled_transactions ORDER BY record_id LIMIT %s", conn, params=(SUPPLY_BATCH_SIZE,)
        )
    finally:
        conn.close()
    if frame.empty:
        raise RuntimeError("No source rows are available for supply")
    source_time = pd.to_datetime(frame["transaction_time"])
    last_day = calendar.monthrange(target_month.year, target_month.month)[1]
    frame["transaction_time"] = pd.to_datetime({
        "year": target_month.year, "month": target_month.month,
        "day": source_time.dt.day.clip(upper=last_day), "hour": source_time.dt.hour, "minute": source_time.dt.minute,
    })
    frame["record_id"] = [f"supply-{target_month:%Y%m}-{value}" for value in frame["record_id"]]
    frame["batch_month"] = target_month
    path = RUNTIME_DIR / f"supply_{target_month:%Y%m}.parquet"
    frame.to_parquet(path, index=False)
    return {"path": str(path), "month": target_month.isoformat(), "rows": int(len(frame))}


def append_month_batch(batch_info: dict) -> dict:
    """Insert the prepared batch with ON CONFLICT DO NOTHING for retry safety.

    Reads the parquet from prepare_month_batch and writes only absent stable record IDs.
    The supply pointer remains unchanged until prune and advance succeed.
    """
    frame = pd.read_parquet(batch_info["path"])
    values = [tuple(row) for row in frame.loc[:, LABELED_COLUMNS].itertuples(index=False, name=None)]
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                "INSERT INTO labeled_transactions (" + ", ".join(LABELED_COLUMNS) + ") VALUES %s ON CONFLICT (record_id) DO NOTHING",
                values,
            )
            inserted = cursor.rowcount
        conn.commit()
    finally:
        conn.close()
    return {**batch_info, "inserted": int(inserted)}


def prune_history(batch_info: dict) -> dict:
    """Delete records outside the configured month horizon after a successful append.

    Reads the target month, deletes only data older than HISTORY_MONTHS, and leaves the
    state pointer untouched. Repeating the same delete is harmless.
    """
    month = pd.Timestamp(batch_info["month"]).date()
    cutoff = (pd.Timestamp(month) - pd.DateOffset(months=HISTORY_MONTHS)).date()
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM labeled_transactions WHERE transaction_time < %s", (cutoff,))
            deleted = cursor.rowcount
        conn.commit()
    finally:
        conn.close()
    return {**batch_info, "deleted": int(deleted)}


def advance_supply_month(batch_info: dict) -> str:
    """Advance the supply pointer only after the batch is present and retention completed.

    Reads the committed batch metadata and writes the following month to pipeline_state.
    Because earlier tasks use stable IDs, rerunning before this update cannot duplicate data.
    """
    month = pd.Timestamp(batch_info["month"]).date()
    next_month = (pd.Timestamp(month) + pd.DateOffset(months=1)).date()
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE pipeline_state SET next_supply_month = %s, updated_at = CURRENT_TIMESTAMP WHERE id = 1 AND next_supply_month = %s",
                (next_month, month),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("Supply state changed by another run")
        conn.commit()
    finally:
        conn.close()
    return next_month.isoformat()
