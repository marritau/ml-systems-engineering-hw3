import os
from pathlib import Path


RUNTIME_DIR = Path("/opt/airflow/runtime")
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/opt/airflow/models"))
DQ_REPORTS_DIR = Path(os.getenv("DQ_REPORTS_DIR", "/opt/airflow/dq_reports"))
FRAUD_SRC = os.getenv("FRAUD_SRC", "/opt/airflow/fraud_src")
WINDOW_MONTHS = int(os.getenv("TRAINING_WINDOW_MONTHS", "3"))
HISTORY_MONTHS = int(os.getenv("HISTORY_MONTHS", "36"))
SUPPLY_BATCH_SIZE = int(os.getenv("SUPPLY_BATCH_SIZE", "10000"))
