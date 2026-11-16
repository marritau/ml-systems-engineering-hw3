import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict, deque
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer, KafkaError, Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from preprocessing import run_preproc
from scorer import ModelRegistry, predict


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")
BATCH_SIZE = int(os.getenv("SCORING_BATCH_SIZE", "500"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
SCORE_BUCKETS = [index / 20 for index in range(21)]

PROCESSED_TRANSACTIONS = Counter(
    "fraud_detector_processed_transactions_total",
    "Transactions successfully scored and sent to Kafka.",
    ["us_state", "merch"],
)
FRAUD_TRANSACTIONS = Counter(
    "fraud_detector_fraud_transactions_total",
    "Transactions marked as fraud after scoring.",
    ["us_state", "merch"],
)
PROCESSING_DURATION = Histogram(
    "fraud_detector_processing_seconds",
    "Average end-to-end processing duration per transaction in a scored batch.",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
SCORE_DISTRIBUTION = Histogram(
    "fraud_detector_score",
    "Predicted fraud score distribution.",
    ["us_state", "merch"],
    buckets=SCORE_BUCKETS,
)
SCORING_BATCH_SIZE = Histogram(
    "fraud_detector_batch_size",
    "Number of valid transactions scored in one model call.",
    buckets=(1, 10, 50, 100, 250, 500, 1000),
)
MODEL_VERSION_CHANGES = Counter(
    "fraud_detector_model_version_changes_total",
    "Hot-reload events for published models.",
    ["version"],
)
ACTIVE_MODEL_VERSION = Gauge(
    "fraud_detector_active_model_version",
    "Currently loaded model version.",
    ["version"],
)
RECENT_CATEGORY_FRAUD_SHARE = Gauge(
    "fraud_detector_recent_1000_category_fraud_share",
    "Fraud share by category among the latest 1000 scored transactions.",
    ["cat_id"],
)


def deserialize_transaction(payload: bytes) -> tuple[str, dict]:
    """Validate a transaction event without changing the Kafka contract."""
    message = json.loads(payload.decode("utf-8"))
    transaction_id, data = message.get("transaction_id"), message.get("data")
    if not isinstance(transaction_id, str) or not transaction_id or not isinstance(data, dict):
        raise ValueError("Transaction must contain transaction_id and data")
    return transaction_id, data


def metric_label(value) -> str:
    return str(value) if value not in (None, "") else "unknown"


class ProcessingService:
    def __init__(self):
        self.running = True
        self.registry = ModelRegistry()
        self.registry.bootstrap()
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": os.getenv("KAFKA_GROUP_ID", "ml-scorer"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        })
        self.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "enable.idempotence": True, "acks": "all"})
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.model_version = None
        self.recent_outcomes = deque(maxlen=1000)
        start_http_server(METRICS_PORT)
        logger.info("Prometheus metrics are available on port %s", METRICS_PORT)

    def stop(self, *_):
        self.running = False

    def process_batch(self, messages):
        started_at = time.perf_counter()
        transactions = []
        for message in messages:
            try:
                transactions.append(deserialize_transaction(message.value()))
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                logger.warning("Skipping malformed transaction at offset %s: %s", message.offset(), error)
        if transactions:
            model, artifact, version = self.registry.current()
            self.record_model_version(version)
            raw = pd.DataFrame([data for _, data in transactions])
            processed = run_preproc(artifact, raw)
            predictions = predict(model, processed)
            for (transaction_id, _), prediction in zip(transactions, predictions.itertuples(index=False)):
                payload = json.dumps({
                    "transaction_id": transaction_id,
                    "score": float(prediction.score),
                    "fraud_flag": int(prediction.fraud_flag),
                    "model_version": version,
                }).encode("utf-8")
                self.producer.produce(SCORES_TOPIC, key=transaction_id.encode(), value=payload)
            if self.producer.flush(30):
                raise RuntimeError("Kafka score delivery timed out")
            for (_, data), prediction in zip(transactions, predictions.itertuples(index=False)):
                labels = {"us_state": metric_label(data.get("us_state")), "merch": metric_label(data.get("merch"))}
                PROCESSED_TRANSACTIONS.labels(**labels).inc()
                SCORE_DISTRIBUTION.labels(**labels).observe(float(prediction.score))
                if prediction.fraud_flag:
                    FRAUD_TRANSACTIONS.labels(**labels).inc()
                self.recent_outcomes.append((metric_label(data.get("cat_id")), int(prediction.fraud_flag)))
            self.update_recent_category_fraud_share()
            SCORING_BATCH_SIZE.observe(len(transactions))
            PROCESSING_DURATION.observe((time.perf_counter() - started_at) / len(transactions))
            logger.info("Scored %s messages with model %s", len(transactions), version)
        for message in messages:
            self.consumer.store_offsets(message=message)
        self.consumer.commit(asynchronous=False)

    def record_model_version(self, version: str) -> None:
        if self.model_version == version:
            return
        if self.model_version is not None:
            ACTIVE_MODEL_VERSION.labels(version=self.model_version).set(0)
            MODEL_VERSION_CHANGES.labels(version=version).inc()
            logger.info("Model changed from %s to %s", self.model_version, version)
        ACTIVE_MODEL_VERSION.labels(version=version).set(1)
        self.model_version = version

    def update_recent_category_fraud_share(self) -> None:
        totals = defaultdict(int)
        frauds = defaultdict(int)
        for category, fraud_flag in self.recent_outcomes:
            totals[category] += 1
            frauds[category] += fraud_flag
        for category, total in totals.items():
            RECENT_CATEGORY_FRAUD_SHARE.labels(cat_id=category).set(frauds[category] / total)

    def run(self):
        while self.running:
            messages = [message for message in self.consumer.consume(BATCH_SIZE, timeout=1) if not message.error()]
            if not messages:
                continue
            while self.running:
                try:
                    self.process_batch(messages)
                    break
                except Exception:
                    logger.exception("Scoring batch failed; retrying")
                    time.sleep(3)

    def close(self):
        self.producer.flush(10)
        self.consumer.close()


def main():
    service = ProcessingService()
    signal.signal(signal.SIGTERM, service.stop)
    signal.signal(signal.SIGINT, service.stop)
    try:
        service.run()
    finally:
        service.close()


if __name__ == "__main__":
    main()
