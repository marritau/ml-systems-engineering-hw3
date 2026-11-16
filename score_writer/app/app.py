import json
import os
import signal
import time

import psycopg2
from confluent_kafka import Consumer
from prometheus_client import Counter, Histogram, start_http_server
from psycopg2.extras import execute_values


UPSERT = """
INSERT INTO transaction_scores (transaction_id, score, fraud_flag, model_version)
VALUES %s
ON CONFLICT (transaction_id) DO UPDATE SET
  score = EXCLUDED.score,
  fraud_flag = EXCLUDED.fraud_flag,
  model_version = EXCLUDED.model_version,
  created_at = CURRENT_TIMESTAMP
"""

METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))
SCORE_BUCKETS = [index / 20 for index in range(21)]
WRITTEN_SCORES = Counter(
    "score_writer_written_scores_total",
    "Scores committed to PostgreSQL.",
)
WRITTEN_FRAUDS = Counter(
    "score_writer_written_fraud_total",
    "Fraud scores committed to PostgreSQL.",
)
WRITE_DURATION = Histogram(
    "score_writer_write_seconds",
    "Average PostgreSQL write duration per score in a Kafka batch.",
    buckets=(0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2),
)
WRITTEN_SCORE_DISTRIBUTION = Histogram(
    "score_writer_score",
    "Fraud score distribution committed to PostgreSQL.",
    buckets=SCORE_BUCKETS,
)


def connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"), port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "fraud_detection"), user=os.getenv("POSTGRES_USER", "fraud_user"),
        password=os.getenv("POSTGRES_PASSWORD", "fraud_password"),
    )


def parse_score(payload: bytes) -> tuple[str, float, int, str]:
    """Validate score payload including the published model version."""
    value = json.loads(payload.decode("utf-8"))
    required = {"transaction_id", "score", "fraud_flag", "model_version"}
    if set(value) != required or value["fraud_flag"] not in (0, 1) or not 0 <= float(value["score"]) <= 1:
        raise ValueError("Invalid score event")
    return value["transaction_id"], float(value["score"]), int(value["fraud_flag"]), value["model_version"]


def main():
    running = True
    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    start_http_server(METRICS_PORT)
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id": os.getenv("KAFKA_GROUP_ID", "score-writer"), "auto.offset.reset": "earliest",
        "enable.auto.commit": False, "enable.auto.offset.store": False,
    })
    consumer.subscribe([os.getenv("KAFKA_SCORES_TOPIC", "scores")])
    conn = None
    try:
        while running:
            if conn is None:
                try:
                    conn = connection()
                except psycopg2.OperationalError:
                    time.sleep(2)
                    continue
            messages = [message for message in consumer.consume(500, timeout=1) if not message.error()]
            if not messages:
                continue
            rows = []
            for message in messages:
                try:
                    rows.append(parse_score(message.value()))
                except (ValueError, TypeError, json.JSONDecodeError):
                    pass
            try:
                if rows:
                    started_at = time.perf_counter()
                    with conn.cursor() as cursor:
                        execute_values(cursor, UPSERT, rows)
                    conn.commit()
                    WRITE_DURATION.observe((time.perf_counter() - started_at) / len(rows))
                    WRITTEN_SCORES.inc(len(rows))
                    WRITTEN_FRAUDS.inc(sum(fraud_flag for _, _, fraud_flag, _ in rows))
                    for _, score, _, _ in rows:
                        WRITTEN_SCORE_DISTRIBUTION.observe(score)
                for message in messages:
                    consumer.store_offsets(message=message)
                consumer.commit(asynchronous=False)
            except psycopg2.Error:
                conn.rollback()
                conn.close()
                conn = None
    finally:
        if conn is not None:
            conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
