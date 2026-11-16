# Monitoring fraud scoring service

Домашнее задание №5 по MLOps. Kafka несёт непрерывный поток транзакций, `fraud-detector` рассчитывает скор CatBoost, а `score-writer` сохраняет результат в PostgreSQL. Prometheus собирает метрики сервисов, Grafana автоматически подключает источники и дашборды.

## Architecture

```text
transaction-simulator -> Kafka transactions -> fraud-detector -> Kafka scores -> score-writer -> PostgreSQL
                                               |                                |
                                               +---------- Prometheus ----------+
node-exporter -------------------------------+
Prometheus -> Grafana
Airflow retraining -> model-registry -> fraud-detector
```

`fraud-detector` проверяет файл `latest.json` перед каждым пакетом Kafka-сообщений и без перезапуска подхватывает опубликованную модель. При смене версии он обновляет Prometheus-метрику, а Grafana показывает аннотацию на дашборде.

## Start

Положите размеченный `train.csv` в `data/train.csv` или укажите другой путь в `.env`:

```powershell
Copy-Item .env.example .env
docker compose up -d --build
docker compose ps
```

Первичная загрузка данных выполняется сервисом `data-loader`. После её завершения поток начинает генерировать `transaction-simulator`.

## Services

| Service | Address | Credentials |
| --- | --- | --- |
| Grafana | [http://localhost:3000](http://localhost:3000) | `admin` / `admin` |
| Prometheus | [http://localhost:9090](http://localhost:9090) | - |
| Airflow | [http://localhost:8081](http://localhost:8081) | `admin` / `admin` |
| Kafka UI | [http://localhost:8080](http://localhost:8080) | - |
| Fraud detector metrics | [http://localhost:8000/metrics](http://localhost:8000/metrics) | - |
| Score writer metrics | [http://localhost:8001/metrics](http://localhost:8001/metrics) | - |

Prometheus collects every target once per second. In **Status -> Targets** all three targets must be `UP`: `fraud-detector`, `score-writer` and `node-exporter`.

## Metrics

`fraud-detector` publishes:

- `fraud_detector_processed_transactions_total` and `fraud_detector_fraud_transactions_total` with `us_state` and `merch` labels;
- `fraud_detector_processing_seconds` for per-transaction scoring time;
- `fraud_detector_score` for score distribution;
- `fraud_detector_model_version_changes_total` and `fraud_detector_active_model_version` for hot model reloads;
- `fraud_detector_batch_size` for transactions per model call;
- `fraud_detector_recent_1000_category_fraud_share` for the fraud share by product category among the latest 1000 transactions.

`fraud_detector_batch_size` is the additional metric absent from the seminar example. It helps distinguish a slower model from a larger Kafka batch: the P95 batch size is shown beside the P95 scoring duration.

`score-writer` publishes `score_writer_written_scores_total`, `score_writer_written_fraud_total`, `score_writer_write_seconds` and `score_writer_score`. Node Exporter publishes host CPU, memory, network and disk metrics.

## Dashboards

Grafana provisions all dashboards automatically into the `Fraud monitoring` folder:

- **Fraud processing**: TPS, P95 scoring duration, fraud share, score density, P95 batch size, category and state fraud views. Filters `State` and `Merchant` apply to TPS and score density.
- **Score writer**: PostgreSQL write throughput and duration, written fraud count, score density, plus a PostgreSQL panel with score count and fraud share by `model_version`.
- **System metrics**: CPU, memory, network and free disk space from Node Exporter.

The **Fraud processing** dashboard includes an annotation for every new model version. To demonstrate it, open Airflow, manually run `supply_labeled_batch`, then run `retrain_model`. Once the new model is published, the detector reloads it during the next scoring batch and Grafana shows the version marker.

## Alerts

Grafana provisions two alert rules in **Alerting**:

- **High fraud share in the scoring stream**: fraud share exceeds 20% for 30 seconds.
- **Scoring stream is stalled**: processing throughput stays below 0.01 transaction/s for 30 seconds.

To verify the stalled-stream alert, stop the simulator for one minute, check **Alerting**, then start it again:

```powershell
docker compose stop transaction-simulator
docker compose start transaction-simulator
```

To verify the high-fraud alert, temporarily set `FRAUD_THRESHOLD: 0` for `fraud-detector` in `docker-compose.yaml`, recreate that service, wait 30 seconds and then restore `0.98`.

## Stop

```powershell
docker compose down
```

For a complete reset of PostgreSQL, model registry, Prometheus and Grafana data:

```powershell
docker compose down -v
```
