# Member 3 Streaming Output Contract

## 1. Primary Output Specification

- **Topic Name**: `financial_features_paysim`
- **Key**: `txn_id` (UTF-8 String, SHA-256 hash of `step|type|amount|nameOrig|nameDest`)
- **Key Semantics**: Uniquely and deterministically identifies every financial transaction. Used as the partition key and deduplication anchor for downstream consumers.
- **Value Format**: UTF-8 JSON object containing complete transaction metadata and engineered features.
- **Feature Version**: `v1.0` (included in schema and payload).

---

## 2. Full Schema (42 Columns)

The streaming output schema matches the batch feature store contract (`data/features/paysim_features.parquet`) produced by Member 2 with zero discrepancies, plus the required `feature_version` column.

| # | Column Name | PySpark DataType | Nullable | Description & Domain Semantics |
|---|---|---|---|---|
| 1 | `step` | `int` | True | Simulation timestep in hours (1..743) |
| 2 | `type` | `string` | True | Raw transaction type (TRANSFER, CASH_OUT, PAYMENT, CASH_IN, DEBIT) |
| 3 | `amount` | `double` | True | Transaction amount in simulated currency |
| 4 | `nameOrig` | `string` | True | Originating customer account identifier |
| 5 | `oldbalanceOrg` | `double` | True | Originator pre-transaction balance |
| 6 | `nameDest` | `string` | True | Destination recipient account identifier |
| 7 | `oldbalanceDest` | `double` | True | Recipient pre-transaction balance |
| 8 | `isFraud` | `int` | True | Ground-truth fraud label (0 or 1) |
| 9 | `step_hour` | `int` | True | Hour of day derived as step % 24 (0..23) |
| 10 | `txn_id` | `string` | True | Unique SHA-256 transaction identifier |
| 11 | `hour_sin` | `double` | True | Cyclical hour feature: sin(2 * pi * step_hour / 24) |
| 12 | `hour_cos` | `double` | True | Cyclical hour feature: cos(2 * pi * step_hour / 24) |
| 13 | `sim_day` | `int` | True | Simulation day derived as floor((step - 1) / 24) |
| 14 | `history_window_complete_flag` | `tinyint` | True | 1 if step >= 169 (full 7-day 168h history available), else 0 |
| 15 | `type_transfer` | `tinyint` | True | One-hot binary indicator: 1 if type == 'TRANSFER', else 0 |
| 16 | `type_cash_out` | `tinyint` | True | One-hot binary indicator: 1 if type == 'CASH_OUT', else 0 |
| 17 | `type_payment` | `tinyint` | True | One-hot binary indicator: 1 if type == 'PAYMENT', else 0 |
| 18 | `type_cash_in` | `tinyint` | True | One-hot binary indicator: 1 if type == 'CASH_IN', else 0 |
| 19 | `type_debit` | `tinyint` | True | One-hot binary indicator: 1 if type == 'DEBIT', else 0 |
| 20 | `split` | `string` | True | Dataset split partition: 'train' for step <= 500, 'valid' for 501 <= step <= 600, 'test' for 601 <= step <= 743 |
| 21 | `amount_log1p` | `double` | True | log1p(max(0, amount)) |
| 22 | `amount_zero_flag` | `tinyint` | True | 1 if amount == 0.0, else 0 |
| 23 | `amt_to_oldbalOrg` | `double` | True | Ratio: amount / (oldbalanceOrg + 1.0) |
| 24 | `amt_to_oldbalDest` | `double` | True | Ratio: amount / (oldbalanceDest + 1.0) |
| 25 | `orig_drain_flag` | `tinyint` | True | 1 if oldbalanceOrg > 0 and abs(amount - oldbalanceOrg) < 0.005, else 0 |
| 26 | `amount_exceeds_orig_balance_flag` | `tinyint` | True | 1 if amount > oldbalanceOrg, else 0 |
| 27 | `orig_balance_zero_flag` | `tinyint` | True | 1 if oldbalanceOrg == 0.0, else 0 |
| 28 | `dest_balance_zero_flag` | `tinyint` | True | 1 if oldbalanceDest == 0.0 and type != 'PAYMENT', else 0 |
| 29 | `dest_prior_txn_count` | `int` | True | Cumulative prior transactions seen for this destination account |
| 30 | `dest_first_seen_flag` | `tinyint` | True | 1 if destination is appearing for the very first time (dest_prior_txn_count == 0), else 0 |
| 31 | `dest_prior_amount_sum` | `double` | True | Cumulative sum of amounts previously received by this destination |
| 32 | `dest_prior_amount_mean` | `double` | True | Prior average amount received; null when dest_prior_txn_count == 0 |
| 33 | `dest_prior_amount_std` | `double` | True | Sample standard deviation of prior amounts; null when dest_prior_txn_count < 2 |
| 34 | `dest_prior_amount_max` | `double` | True | Maximum prior amount received; null when dest_prior_txn_count == 0 |
| 35 | `dest_steps_since_last_txn` | `int` | True | step - last_seen_step; null when destination is appearing for the first time |
| 36 | `dest_amount_zscore` | `double` | True | Z-score of current amount vs destination prior history; null when dest_prior_txn_count < 2 or std == 0.0 |
| 37 | `dest_txn_count_prev_24h` | `int` | True | Prior transaction count received by destination in trailing 24 hours (step - 24 <= prev_step < step) |
| 38 | `dest_txn_count_prev_168h` | `int` | True | Prior transaction count received by destination in trailing 168 hours (step - 168 <= prev_step < step) |
| 39 | `dest_amount_sum_prev_24h` | `double` | True | Prior transaction amount sum received by destination in trailing 24 hours |
| 40 | `dest_amount_sum_prev_168h` | `double` | True | Prior transaction amount sum received by destination in trailing 168 hours |
| 41 | `dest_prior_cashout_share` | `double` | True | Fraction of prior destination transactions that were CASH_OUT; null when dest_prior_txn_count == 0 |
| 42 | `feature_version` | `string` | True | Contract feature store version string (v1.0) |

---

## 3. Null Semantics

1. **Preservation of Nulls**:
   JSON payloads produced to `financial_features_paysim` are serialized with `ignoreNullFields = false`. Keys corresponding to null fields are explicitly written as `"column_name": null`, NEVER omitted from the JSON structure.
2. **First-Appearance Columns**:
   When a destination appears for the first time (`dest_first_seen_flag == 1`), the following columns are legitimately `null`:
   - `dest_prior_amount_mean = null`
   - `dest_prior_amount_std = null`
   - `dest_prior_amount_max = null`
   - `dest_steps_since_last_txn = null`
   - `dest_amount_zscore = null`
   - `dest_prior_cashout_share = null`
3. **Variance Columns**:
   `dest_prior_amount_std` and `dest_amount_zscore` are strictly `null` for `dest_prior_txn_count < 2`. If `std == 0.0` (all prior transactions identical in amount), `dest_amount_zscore` evaluates to `null` (not infinity or NaN).

---

## 4. Ordering & Delivery Guarantees

1. **Partition Ordering**:
   Within each Kafka partition and for any given `nameDest` grouping key, transactions are processed and emitted in monotonic order by simulation `step`.
2. **Same-Step Tie-Breaking**:
   Transactions arriving with the same `step` for the same destination are batched together. Current-step transactions do NOT observe each other's state updates, matching the exact batch temporal contract.
3. **At-Least-Once Delivery**:
   Spark Structured Streaming with RocksDB state stores guarantees at-least-once delivery. If an executor or driver process terminates unexpectedly mid-batch, uncommitted batches will be re-processed upon restart.
4. **Deduplication on `txn_id`**:
   Downstream consumers (model serving, feature store ingest, or feature monitoring) MUST treat `txn_id` as the idempotent deduplication primary key. Duplicates produced due to crash recovery are guaranteed to be byte-identical.

---

## 5. Dead-Letter Queue (DLQ) Specification

- **DLQ Topic**: `financial_transactions_paysim_dlq`
- **Key**: Originating `txn_id` or transaction identifier string (or null if unparseable).
- **Payload Schema**:
  ```json
  {
    "raw_value": "<raw_unmodified_json_string>",
    "reason_code": "<REASON_CODE>",
    "ingested_at": "2026-10-13T18:30:00.000000+00:00"
  }
  ```

### Reason Codes Reference Table (Generated from Code Constants)

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

---

## 6. Tier B Exclusion

The streaming feature engine strictly implements **Tier A Features** (temporal, stateless, destination stateful, and trailing window aggregates).

- **Tier B Balance Leakage Fields**: Post-transaction balances (`newbalanceOrig`, `newbalanceDest`) are intentionally **excluded** from this streaming feature engine.
- **Rationale**: Post-transaction balances leak the actual financial outcome / resolution of the transaction into the input feature space. In production inference, future balances are unavailable at transaction arrival time. As specified in `member2_spark/docs/OUTPUT_CONTRACT.md`, `newbalanceOrig` and `newbalanceDest` are excluded to avoid severe data leakage.

---

## 7. Crash Recovery Procedure & Known Limitations

1. **Delivery Guarantee**:
   The pipeline operates under **at-least-once** delivery semantics. Downstream consumers must implement idempotent deduplication using `txn_id`.
2. **State Store Consistency**:
   The engine uses RocksDB (`org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider`). When the engine crashes abruptly (e.g. `SIGKILL` / `taskkill /F /T`), the RocksDB state store restarts from the last committed micro-batch checkpoint.
3. **Recovery Limitation & Spark 4.1.1 Micro-Batch Workaround**:
   In Apache Spark Structured Streaming, if a process terminates hard while writing checkpoint offset metadata, an uncommitted orphan offset file (`offsets/N` without matching `commits/N`) can cause restart failures with `NullPointerException` in `MicroBatchExecution.constructNextBatch` unless the uncommitted trailing offset file is rolled back prior to restart. Downstream systems must not assume zero-duplicate exactly-once semantics without deduplication on `txn_id`.

---

## 8. How to Replay the Stream

To replay transactions from an arbitrary point in time or re-generate features:

### Full Stream Replay (Cold Start):
1. Stop the active consumer process:
   ```bash
   taskkill /F /PID <consumer_pid>
   ```
2. Purge or specify a fresh checkpoint directory:
   ```bash
   rmdir /S /Q member3_streaming\checkpoints\paysim
   ```
3. Re-run consumer with `--checkpoint-location <new_chk>` and `--input-topic financial_transactions_paysim`:
   ```bash
   python -m member3_streaming.consumer_paysim --checkpoint-location member3_streaming/checkpoints/paysim_replay
   ```
   Spark will read from `startingOffsets earliest` and regenerate all features deterministically from step 1.

### Point-in-Time Replay:
1. Identify the target Kafka offset corresponding to the desired simulation step.
2. Configure `startingOffsets` to the target offset JSON:
   ```python
   .option("startingOffsets", '{"financial_transactions_paysim":{"0": 5000}}')
   ```
3. Start the consumer with a dedicated checkpoint directory to avoid conflicting with existing commit logs.
