import json
import os
import random
import time
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer


CSV_PATH = Path(os.getenv("TRANSACTIONS_CSV_PATH", "/data/train.csv"))
INTERVAL = float(os.getenv("SEND_INTERVAL_SECONDS", "1"))
FEATURE_COLUMNS = [
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2", "gender", "street",
    "one_city", "us_state", "post_code", "lat", "lon", "population_city", "jobs", "merchant_lat", "merchant_lon",
]


def json_value(value):
    return None if pd.isna(value) else value.item() if hasattr(value, "item") else value


def main():
    frame = pd.read_csv(CSV_PATH, usecols=FEATURE_COLUMNS)
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")})
    topic = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
    while True:
        row = frame.iloc[random.randrange(len(frame))]
        data = {column: json_value(row[column]) for column in FEATURE_COLUMNS}
        data["transaction_time"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        message = {"transaction_id": str(uuid.uuid4()), "data": data}
        producer.produce(topic, value=json.dumps(message).encode("utf-8"))
        producer.poll(0)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
