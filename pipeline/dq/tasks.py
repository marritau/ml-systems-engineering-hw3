from datetime import datetime, timezone
from html import escape

import pandas as pd

from pipeline.db import get_conn
from pipeline.settings import DQ_REPORTS_DIR


def build_quality_report() -> str:
    """Run dataset rules on the labeled showcase and write an HTML report.

    Reads labeled_transactions, writes an immutable timestamped report in the shared DQ
    directory, and never mutates source data, so any retry produces another safe report.
    """
    conn = get_conn()
    try:
        frame = pd.read_sql("SELECT * FROM labeled_transactions", conn)
    finally:
        conn.close()
    rules = [
        ("Rows are present", len(frame) > 0, f"rows={len(frame)}"),
        ("Required columns", {"record_id", "transaction_time", "amount", "target"}.issubset(frame.columns), "schema check"),
        ("Target is binary", frame["target"].isin([0, 1]).all(), f"invalid={(~frame['target'].isin([0, 1])).sum()}"),
        ("Amounts are positive", (frame["amount"] > 0).all(), f"invalid={(frame['amount'] <= 0).sum()}"),
        ("Transaction times are valid", pd.to_datetime(frame["transaction_time"], errors="coerce").notna().all(), "timestamp parse check"),
        ("Coordinates are in range", frame["lat"].between(-90, 90).all() and frame["lon"].between(-180, 180).all(), "latitude/longitude check"),
        ("Record identifiers are unique", not frame["record_id"].duplicated().any(), f"duplicates={frame['record_id'].duplicated().sum()}"),
    ]
    status = "PASS" if all(passed for _, passed, _ in rules) else "FAIL"
    rows = "".join(f"<tr><td>{escape(name)}</td><td>{'PASS' if passed else 'FAIL'}</td><td>{escape(detail)}</td></tr>" for name, passed, detail in rules)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    DQ_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = DQ_REPORTS_DIR / f"dq_report_{timestamp}.html"
    path.write_text(
        "<html><body><h1>Data quality report</h1>"
        f"<p>Overall status: <strong>{status}</strong></p>"
        "<table border='1'><tr><th>Rule</th><th>Status</th><th>Details</th></tr>" + rows + "</table></body></html>",
        encoding="utf-8",
    )
    return str(path)
