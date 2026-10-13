# Member 3: Real-Time Streaming Feature Store (PaySim)

This module implements the production-grade Spark Structured Streaming feature engine and Kafka producer/consumer pipeline for real-time PaySim transaction feature engineering.

---

## 1. Architecture & Topics

- **Kafka Topics**:
  - `financial_transactions_paysim`: Inbound raw financial transaction stream.
  - `financial_features_paysim`: Outbound enriched feature stream (42-column Tier A schema).
  - `financial_transactions_paysim_dlq`: Dead Letter Queue for malformed, invalid, or late events.
- **State Store Provider**:
  - `org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider` (format version 5).
  - RocksDB offloads state to local disk / SST files, supporting multi-million unique destination accounts (`nameDest`) without JVM heap exhaustion.
- **Feature Version**: `v1.0`

---

## 2. Output Schema & Contract (42 Columns)

The output stream strictly conforms to `data/features/paysim_features.parquet` and `MEMBER3_OUTPUT_CONTRACT.md`:

1. **Transaction Metadata & Raw Fields**: `step`, `type`, `amount`, `nameOrig`, `oldbalanceOrg`, `nameDest`, `oldbalanceDest`, `isFraud`, `txn_id`.
2. **Stateless / Temporal Features**: `step_hour`, `hour_sin`, `hour_cos`, `sim_day`, `history_window_complete_flag`, `type_transfer`, `type_cash_out`, `type_payment`, `type_cash_in`, `type_debit`, `split`.
3. **Stateless Financial Ratios & Balance Indicators**: `amount_log1p`, `amount_zero_flag`, `amt_to_oldbalOrg`, `amt_to_oldbalDest`, `orig_drain_flag`, `amount_exceeds_orig_balance_flag`, `orig_balance_zero_flag`, `dest_balance_zero_flag`.
4. **Stateful Cumulative Destination Features**: `dest_prior_txn_count`, `dest_first_seen_flag`, `dest_prior_amount_sum`, `dest_prior_amount_mean`, `dest_prior_amount_std`, `dest_prior_amount_max`, `dest_steps_since_last_txn`, `dest_amount_zscore`, `dest_prior_cashout_share`.
5. **Stateful Trailing Window Aggregates**:
   - 24-Hour Windows: `dest_txn_count_prev_24h`, `dest_amount_sum_prev_24h`.
   - 168-Hour (7-Day) Windows: `dest_txn_count_prev_168h`, `dest_amount_sum_prev_168h`.
6. **Metadata**: `feature_version` (`v1.0`).

### Tier B Exclusion
Post-transaction balance fields (`newbalanceOrig`, `newbalanceDest`) are excluded to prevent outcome leakage into the real-time feature vector, as specified in `member2_spark/docs/OUTPUT_CONTRACT.md`.

### `split` Partitioning
In accordance with `member2_spark/docs/OUTPUT_CONTRACT.md`:
- `train`: `step <= 500` (Days 1–20)
- `valid`: `501 <= step <= 600` (Days 21–25)
- `test`: `601 <= step <= 743` (Days 26–31)

### Null Semantics
JSON outputs preserve explicit `null` fields (`ignoreNullFields = false`).
- For first-seen destinations (`dest_first_seen_flag == 1`), prior statistics (`dest_prior_amount_mean`, `dest_prior_amount_std`, `dest_prior_amount_max`, `dest_steps_since_last_txn`, `dest_amount_zscore`, `dest_prior_cashout_share`) evaluate strictly to `null`.
- Standard deviation and Z-score are `null` when `dest_prior_txn_count < 2` or `std == 0.0`.

---

## 3. Dead Letter Queue (DLQ) & Guards

Malformed or out-of-order events are routed to `financial_transactions_paysim_dlq` without terminating the pipeline.

### DLQ Payload Schema
```json
{
  "raw_value": "<raw_unmodified_json_string>",
  "reason_code": "<REASON_CODE>",
  "ingested_at": "<ISO-8601 UTC timestamp>"
}
```

### Reason Codes
| Reason Code | Description |
|---|---|
| `MALFORMED_JSON` | Payload is not valid JSON or not a JSON object |
| `MISSING_FIELD` | One or more required fields are missing or null |
| `BAD_STEP_TYPE` | Field 'step' is not an integer (e.g. string or boolean) |
| `BAD_TYPE` | Field 'type' is not one of the allowed transaction types (TRANSFER, CASH_OUT, PAYMENT, CASH_IN, DEBIT) |
| `BAD_FIELD_TYPE` | A non-step field has the wrong JSON type (e.g. amount is non-numeric string or boolean, name is non-string) |
| `NEGATIVE_AMOUNT` | Transaction amount is negative (< 0.0) |
| `INVALID_STEP` | Simulation step is out of bounds (step < 1, step > 743, or step == 9999) |
| `INVALID_LABEL` | Target label 'isFraud' is not in {0, 1} |
| `LATE_EVENT` | Transaction timestamp/step is strictly older than key's committed latest or pending step |

### Late-Event Guard Rule
To guard against race conditions and mid-step ties, the streaming state store maintains:
- `committed_last_step`: Step of the latest folded transaction history.
- `pending_step`: Step of the currently buffered uncommitted transactions.
- **Rule**: `row_step < max(committed_last_step, pending_step)` -> routes to DLQ as `LATE_EVENT`. Rows matching the pending step are accepted as same-step ties. Rows with `row_step > max(...)` fold pending state into committed state.

---

## 4. Crash Recovery & Limitations

- Spark Structured Streaming provides **at-least-once** delivery guarantees.
- In crash tests involving hard kills (`taskkill /F /T`) mid-batch:
  - RocksDB restores state store checkpoints exactly up to the last committed batch.
  - Spark re-executes the uncommitted batch upon restart, producing byte-identical duplicate records in Kafka.
  - Downstream consumers **must deduplicate on `txn_id`**.
- **Known Recovery Limitation**: Under abrupt termination during metadata commit writing in Spark 4.1.1, an uncommitted orphan offset file (`offsets/N` without `commits/N`) can cause `NullPointerException` on restart if not rolled back prior to starting the query.

---

## 5. Execution Guide

### Reset Topics & Checkpoints
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic financial_transactions_paysim,financial_features_paysim,financial_transactions_paysim_dlq --if-exists
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_transactions_paysim --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_features_paysim --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_transactions_paysim_dlq --partitions 1 --replication-factor 1
```

### Run Producer
```bash
python member3_streaming/producer_paysim.py --limit 20000 --delay 0
```

### Run Consumer
```bash
python -m member3_streaming.consumer_paysim --bootstrap-servers localhost:9092
```

### Run Pytest Suite
```bash
pytest member3_streaming/tests/ -v --durations=0
```
