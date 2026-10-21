import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer, KafkaError, Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from preprocessing import run_preproc
from scorer import ModelRegistry, predict


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")
BATCH_SIZE = int(os.getenv("SCORING_BATCH_SIZE", "500"))


def deserialize_transaction(payload: bytes) -> tuple[str, dict]:
    """Validate a transaction event without changing the Kafka contract."""
    message = json.loads(payload.decode("utf-8"))
    transaction_id, data = message.get("transaction_id"), message.get("data")
    if not isinstance(transaction_id, str) or not transaction_id or not isinstance(data, dict):
        raise ValueError("Transaction must contain transaction_id and data")
    return transaction_id, data


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

    def stop(self, *_):
        self.running = False

    def process_batch(self, messages):
        transactions = []
        for message in messages:
            try:
                transactions.append(deserialize_transaction(message.value()))
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                logger.warning("Skipping malformed transaction at offset %s: %s", message.offset(), error)
        if transactions:
            model, artifact, version = self.registry.current()
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
            logger.info("Scored %s messages with model %s", len(transactions), version)
        for message in messages:
            self.consumer.store_offsets(message=message)
        self.consumer.commit(asynchronous=False)

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
