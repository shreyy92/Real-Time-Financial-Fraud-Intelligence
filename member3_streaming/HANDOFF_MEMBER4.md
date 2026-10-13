# Member 4 Handoff: Real-Time PaySim Feature Stream

## 1. Stream Specification & Contract

- **Output Topic**: `financial_features_paysim`
- **DLQ Topic**: `financial_transactions_paysim_dlq`
- **Contract Path**: [`member3_streaming/MEMBER3_OUTPUT_CONTRACT.md`](file:///e:/vs%20code/projects/data-science/Real-Time-Financial-Fraud-Intelligence/member3_streaming/MEMBER3_OUTPUT_CONTRACT.md)
- **Feature Version**: `v1.0`
- **Schema**: 42 columns (identical to Member 2's Tier A batch feature store contract in `data/features/paysim_features.parquet`, plus `feature_version`).
- **Primary Deduplication Key**: `txn_id` (SHA-256 hash string).

---

## 2. How to Start the Streaming Feature Engine

Run the Spark Structured Streaming consumer from repository root:

```bash
python -m member3_streaming.consumer_paysim \
    --bootstrap-servers localhost:9092 \
    --input-topic financial_transactions_paysim \
    --output-topic financial_features_paysim \
    --dlq-topic financial_transactions_paysim_dlq \
    --checkpoint-location member3_streaming/checkpoints/paysim
```

---

## 3. How to Consume: 15-Line Python Consumer (with `txn_id` Deduplication)

Downstream consumers (model serving, feature store ingest, inference pipelines) must implement idempotent deduplication on `txn_id`:

```python
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "financial_features_paysim",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="member4_inference_consumer",
)
seen_txn_ids = set()
for msg in consumer:
    record = json.loads(msg.value.decode("utf-8"))
    txn_id = record["txn_id"]
    if txn_id in seen_txn_ids:
        continue
    seen_txn_ids.add(txn_id)
    # Feed record directly to real-time inference model
    print(f"Inference record: {txn_id} (step={record['step']}, amount={record['amount']})")
```

---

## 4. Operational Guarantees & Known Limitations

- **Guarantees**:
  - **At-Least-Once Delivery**: No transactions are lost during stream execution.
  - **Feature Parity**: Stateful cumulative aggregates and trailing 24h/168h windows match Member 2's batch ground truth with **0 mismatches** across all 41 compared columns.
  - **Deterministic Duplicates**: Any duplicate records re-emitted due to crash recovery replay are 100% byte-identical.
  - **Dead-Letter Queue (DLQ)**: Malformed JSON, bad types, negative amounts, invalid steps, and late out-of-order events are routed to `financial_transactions_paysim_dlq` without interrupting active streaming queries.
- **Known Limitations**:
  - **Duplicate Mitigation**: Downstream systems must deduplicate on `txn_id`.
  - **Crash Recovery Workaround**: In Apache Spark 4.1.1 on Windows (`SPARK-57438`), an abrupt mid-batch hard crash (`taskkill /F /T`) can leave an uncommitted trailing offset file in checkpoint storage (`offsets/N` without matching `commits/N`). Restarting requires rolling back the uncommitted trailing offset before starting the consumer.

---

## 5. Ready-to-Send Message to Member 4

```text
Hi Member 4,

The PaySim real-time streaming feature pipeline (Member 3) is live and ready for downstream integration.

- Output Kafka Topic: financial_features_paysim
- Output Contract: member3_streaming/MEMBER3_OUTPUT_CONTRACT.md
- Schema: 42 columns (full Tier A schema strictly matching data/features/paysim_features.parquet with 0 mismatches)
- Partitioning / Key: txn_id (deterministic SHA-256 hash)

What is Guaranteed:
1. At-least-once delivery with zero data loss.
2. 100% feature numerical and null parity against Member 2's batch feature store.
3. Byte-identical duplicates upon crash replay.
4. Malformed/late records safely diverted to financial_transactions_paysim_dlq.

What is NOT Guaranteed:
- Zero-duplicate exactly-once delivery without consumer-side deduplication. You MUST deduplicate incoming records using `txn_id`.

Integration Question:
Do you want to consume directly from this Kafka topic (`financial_features_paysim`), or do you need a different sink destination (e.g., Parquet / Delta Lake directory, or gRPC endpoint)?

(Default decision: Kafka topic is the primary integration sink unless you request otherwise.)
```
