# Member 3: Real-Time Streaming Feature Store (PaySim & IEEE-CIS)

This module implements the production-grade Spark Structured Streaming feature engines and Kafka producer/consumer pipelines for real-time transaction feature engineering on both the PaySim and IEEE-CIS datasets.

---

## 1. Architecture & Topic Topology

The streaming pipelines ingest raw financial transactions from Kafka, compute stateless and stateful features in PySpark Structured Streaming via `applyInPandasWithState`, and emit enriched feature events to outbound Kafka topics.

| Pipeline | Inbound Input Topic | Outbound Feature Topic | Dead Letter Queue (DLQ) | State Key |
|---|---|---|---|---|
| **PaySim** | `financial_transactions_paysim` | `financial_features_paysim` | `financial_transactions_paysim_dlq` | `nameDest` |
| **IEEE-CIS** | `financial_transactions_ieee` | `financial_features_ieee` | `financial_transactions_ieee_dlq` | `card_proxy_id` |

### State Store Provider
Both pipelines utilize `org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider` (format version 5). RocksDB offloads state to local disk / SST files, supporting high-cardinality keys (2.7M+ destinations in PaySim, millions of card proxies in IEEE) without JVM heap exhaustion.

---

## 2. Output Schemas & Contracts

- **PaySim Contract**: [`member3_streaming/MEMBER3_OUTPUT_CONTRACT.md`](MEMBER3_OUTPUT_CONTRACT.md)
  - 42 columns (100% parity with `data/features/paysim_features.parquet`, plus `feature_version`).
  - Deduplication key: `txn_id` (SHA-256 hash string).
- **IEEE-CIS Contract**: [`member3_streaming/MEMBER3_IEEE_OUTPUT_CONTRACT.md`](MEMBER3_IEEE_OUTPUT_CONTRACT.md)
  - 483 columns (472 stateless + 11 stateful card history features, 100% parity with `data/features/ieee_features.parquet`).
  - Deduplication key: `TransactionID` (int).

### Tier B Exclusion
Post-transaction balance fields (`newbalanceOrig`, `newbalanceDest`) are excluded from PaySim to prevent target/resolution leakage, as specified in `member2_spark/docs/OUTPUT_CONTRACT.md`. For IEEE-CIS, `isFraud` is retained as a label only without future leakage.

### Null Semantics & Deduplication
- All JSON outputs preserve explicit nulls (`ignoreNullFields = false`).
- Downstream scoring consumers **must deduplicate incoming records** using `txn_id` (PaySim) or `TransactionID` (IEEE). Duplicates produced upon crash recovery are guaranteed to be **100% byte-identical**.

---

## 3. Dead Letter Queue (DLQ) & Guards

Malformed, invalid, or out-of-order events are routed to the respective DLQ topic without stopping active streaming queries:
```json
{
  "raw_value": "<serialized JSON payload>",
  "reason_code": "<REASON_CODE>",
  "ingested_at": "<ISO-8601 UTC timestamp>"
}
```

### Reason Codes
- **PaySim (9 codes)**: `MALFORMED_JSON`, `MISSING_FIELD`, `BAD_STEP_TYPE`, `BAD_TYPE`, `BAD_FIELD_TYPE`, `NEGATIVE_AMOUNT`, `INVALID_STEP`, `INVALID_LABEL`, `LATE_EVENT`.
- **IEEE-CIS (6 codes)**: `MALFORMED_JSON`, `MISSING_FIELD`, `BAD_FIELD_TYPE`, `NEGATIVE_AMOUNT`, `INVALID_LABEL`, `INVALID_TRANSACTIONDT`, `LATE_EVENT`.

**Payload behavior**: For validation errors, `raw_value` contains the raw unmodified Kafka input string. For `LATE_EVENT`, `raw_value` contains the enriched row serialized at state-evaluation time.

---

## 4. Fault Tolerance & Operator Recovery Procedure

- Both pipelines provide **at-least-once** delivery guarantees.
- In crash tests involving hard kills (`taskkill /F /T`) mid-batch:
  - RocksDB restores state store checkpoints exactly up to the last committed batch.
  - Spark 4.1.1 exhibits a `KafkaMicroBatchStream.metrics` NullPointerException when replaying an uncommitted batch of stateful queries, with provider independence shown (RocksDB and HDFS), upstream ticket not verified.
- **OPERATOR WORKAROUND (Documented Rollback Procedure)**:
  1. Terminate running consumer (`taskkill /F /T`).
  2. Inspect checkpoint directory `offsets/` vs `commits/`.
  3. Identify uncommitted orphan offset index $N$ where `offsets/N` exists but `commits/N` does NOT exist.
  4. Delete ONLY `offsets/N` and `offsets/.N.crc`. **Never delete committed batch files**.
  5. Restart consumer stream. Spark plans batch $N$ cleanly from committed offset $N-1$, restoring complete state continuity with byte-identical duplicate output.

---

## 5. Execution Guide

### Topic Reset (Docker Kafka)
```bash
# PaySim topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic financial_transactions_paysim,financial_features_paysim,financial_transactions_paysim_dlq --if-exists
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_transactions_paysim --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_features_paysim --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_transactions_paysim_dlq --partitions 1 --replication-factor 1

# IEEE-CIS topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic financial_transactions_ieee,financial_features_ieee,financial_transactions_ieee_dlq --if-exists
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_transactions_ieee --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_features_ieee --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_transactions_ieee_dlq --partitions 1 --replication-factor 1
```

### Run Producers
```bash
# PaySim Producer
python member3_streaming/producer_paysim.py --limit 20000 --delay 0

# IEEE-CIS Producer
python member3_streaming/producer_ieee.py --limit 20000 --delay 0
```

### Run Consumers
```bash
# PaySim Streaming Consumer
python -m member3_streaming.consumer_paysim --bootstrap-servers localhost:9092

# IEEE-CIS Streaming Consumer
python -m member3_streaming.consumer_ieee --bootstrap-servers localhost:9092
```

### Run Pytest Suite
```bash
pytest member3_streaming/tests/ -v --durations=0
```
