# Airflow retraining for fraud scoring

Homework 3 extends the Kafka fraud-scoring service with scheduled model retraining in Apache Airflow.

## Architecture

```text
train.csv -> data-loader -> PostgreSQL labeled_transactions
                           -> Airflow supply/retraining/DQ DAGs
Kafka transactions -> fraud-detector -> Kafka scores -> score-writer -> PostgreSQL transaction_scores
Airflow -> model_registry -> fraud-detector hot reload
```

The scorer reads transaction events from Kafka and publishes `transaction_id`, `score`, `fraud_flag` and `model_version`. It checks `model_registry/latest.json` before each scoring batch and loads a changed model without restarting its container.

## Services

- `zookeeper`, `kafka`, `kafka-setup`, `kafka-ui`: message transport and topic inspection.
- `postgres`: labeled showcase, pipeline state and score results.
- `data-loader`: initial one-time import of `train.csv`.
- `transaction-simulator`: sends transaction events to Kafka.
- `fraud-detector`: preprocessing and CPU CatBoost inference.
- `score-writer`: persists scores and model versions.
- `airflow`: scheduled supply, retraining and data-quality DAGs.

## Data

Download `train.csv` from the competition and place it at `data/train.csv`. The file is intentionally excluded from Git. In this workspace it was copied from the previous homework.

Create the local environment file:

```powershell
Copy-Item .env.example .env
```

If the CSV is stored elsewhere, set its absolute or relative path in `TRAIN_DATA_CSV` inside `.env`.

## Start

```powershell
docker compose up -d --build
docker compose ps
```

The first launch imports the labeled showcase and creates the initial model registry entry. Service URLs:

- Airflow: http://localhost:8081, credentials `admin` / `admin`
- Kafka UI: http://localhost:8080

## Airflow DAGs

All DAGs run every five minutes with `catchup=False` and can also be triggered manually from the Airflow UI.

### `supply_labeled_batch`

The DAG prepares, inserts, prunes and confirms a new labeled month through four dependent tasks. It copies 10,000 source rows, changes their `transaction_time` to the next calendar month and stores stable record identifiers. A retry inserts no duplicates because PostgreSQL uses `ON CONFLICT DO NOTHING`.

History depth is 36 months. Rows older than 36 months relative to the supplied month are deleted. The pointer to the next month advances only after the batch is inserted and pruning succeeds.

### `retrain_model`

The DAG has the following task chain:

```text
extract_training_window -> split_by_time -> preprocess_window
-> train_candidate -> check_quality -> publish_candidate
```

The training window has three months. The two earlier months are training data and the newest month is validation data; no random split is used. Preprocessing statistics are fitted only on the training part. CatBoost runs with one CPU thread.

The quality gate compares candidate ROC-AUC with the currently published ROC-AUC. The first candidate is published. Later candidates are published only when their ROC-AUC is not lower. A rejected candidate does not update `latest.json` or advance the training month, so the current production model stays active.

Successful publication writes three files into the shared registry:

- `model_<version>.cbm`
- `preprocessing_<version>.json`
- `latest.json`

The final file is replaced atomically; the scoring service uses its new `version` for subsequent Kafka events.

### `data_quality`

This DAG produces a timestamped HTML report in `/opt/airflow/dq_reports`. It checks:

1. the showcase is not empty;
2. required columns are present;
3. `target` contains only `0` and `1`;
4. transaction amounts are positive;
5. timestamps can be parsed;
6. latitude and longitude are in valid ranges;
7. `record_id` values are unique.

## Verify retraining

1. Open Airflow and trigger `retrain_model`.
2. Wait for every task to finish successfully.
3. Check the published version:

```powershell
docker compose exec airflow ls -la /opt/airflow/models
```

4. Check that recently scored transactions use the version:

```powershell
docker compose exec -T postgres psql -U fraud_user -d fraud_detection -c "SELECT transaction_id, score, fraud_flag, model_version, created_at FROM transaction_scores ORDER BY created_at DESC LIMIT 20;"
```

5. Trigger `data_quality` and inspect reports:

```powershell
docker compose exec airflow ls -la /opt/airflow/dq_reports
```

## Reset

```powershell
docker compose down -v
```

This removes Kafka, PostgreSQL, Airflow state, model registry and DQ reports. The next start imports the CSV again.
