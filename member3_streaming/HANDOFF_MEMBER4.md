# Member 4 Handoff: Real-Time Streaming Feature Pipelines (PaySim & IEEE-CIS)

**Handoff Status:** `drafted, awaiting Member 4 reply`  
**Last Updated:** 2026-10-17  

---

## 1. Stream Specifications & Contracts

Member 3 provides two real-time feature streaming engines matching Member 2's batch ground truth:

| Dataset | Output Kafka Topic | DLQ Kafka Topic | Primary Dedupe Key | Total Columns | Contract Document |
|---|---|---|---|---|---|
| **PaySim** | `financial_features_paysim` | `financial_transactions_paysim_dlq` | `txn_id` (SHA-256 string) | 42 | [`member3_streaming/MEMBER3_OUTPUT_CONTRACT.md`](MEMBER3_OUTPUT_CONTRACT.md) |
| **IEEE-CIS** | `financial_features_ieee` | `financial_transactions_ieee_dlq` | `TransactionID` (int) | 483 | [`member3_streaming/MEMBER3_IEEE_OUTPUT_CONTRACT.md`](MEMBER3_IEEE_OUTPUT_CONTRACT.md) |

---

## 2. Startup Commands for Feature Engines

Run the PySpark Structured Streaming engines from the repository root:

### PaySim Feature Engine:
```bash
python -m member3_streaming.consumer_paysim \
    --bootstrap-servers localhost:9092 \
    --input-topic financial_transactions_paysim \
    --output-topic financial_features_paysim \
    --dlq-topic financial_transactions_paysim_dlq \
    --checkpoint-location member3_streaming/checkpoints/paysim
```

### IEEE-CIS Feature Engine:
```bash
python -m member3_streaming.consumer_ieee \
    --bootstrap-servers localhost:9092 \
    --input-topic financial_transactions_ieee \
    --output-topic financial_features_ieee \
    --dlq-topic financial_transactions_ieee_dlq \
    --checkpoint-location member3_streaming/checkpoints/ieee_stateful
```

---

## 3. Mandatory Downstream Deduplication Requirement

Because Spark Structured Streaming operates under **at-least-once delivery**, unexpected driver/executor restarts may replay uncommitted batches, re-emitting records. Replayed duplicate records have been verified to be **100% byte-identical**.

Downstream consumers (model scoring, feature ingestion, monitoring) **MUST deduplicate incoming stream records** using:
- `txn_id` for PaySim
- `TransactionID` for IEEE-CIS

### Example Python Consumer with Deduplication:
```python
import json
from kafka import KafkaConsumer

# Example consumer for PaySim (substitute 'financial_features_ieee' and 'TransactionID' for IEEE)
consumer = KafkaConsumer(
    "financial_features_paysim",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="member4_inference_group",
)

seen_ids = set()
for msg in consumer:
    record = json.loads(msg.value.decode("utf-8"))
    key = record["txn_id"]  # or record["TransactionID"] for IEEE
    if key in seen_ids:
        continue
    seen_ids.add(key)
    
    # Pass record to real-time inference model
    print(f"Scoring event: key={key}")
```

---

## 4. Operational Guarantees & Known Limitations

### What is Guaranteed:
1. **At-Least-Once Delivery**: No transactions are lost during stream execution.
2. **Feature Parity**: Stateful cumulative aggregates and trailing rolling windows match Member 2's batch ground truth with **0 mismatches** across all tested features.
3. **Byte-Identical Duplicates**: Any duplicate records re-emitted due to crash recovery replay are 100% byte-identical.
4. **Dead-Letter Queue (DLQ)**: Malformed JSON, bad types, negative amounts, out-of-bounds fields, and out-of-order late events are safely diverted to the respective DLQ topic without stopping active streaming queries.

### Known Limitations:
1. **Consumer Deduplication**: Consumers must filter duplicate keys.
2. **Crash Recovery Operator Workaround**: In Spark 4.1.1, a mid-batch hard crash (`taskkill /F /T`) leaves an orphan uncommitted offset file in the checkpoint (`offsets/N` without matching `commits/N`). Replaying this uncommitted batch triggers a `KafkaMicroBatchStream.metrics` NullPointerException (upstream ticket not verified). The verified operational procedure is to delete the orphan `offsets/N` file before restarting, allowing Spark to cleanly re-read from committed offset `N-1`.

---

## 5. Ready-to-Send Message to Member 4

```text
Hi Member 4,

The real-time streaming feature pipelines for both PaySim and IEEE-CIS are implemented and ready for integration.

1. PaySim Pipeline:
   - Output Topic: financial_features_paysim
   - Output Contract: member3_streaming/MEMBER3_OUTPUT_CONTRACT.md
   - Schema: 42 columns (identical to data/features/paysim_features.parquet)
   - Dedupe Key: txn_id

2. IEEE-CIS Pipeline:
   - Output Topic: financial_features_ieee
   - Output Contract: member3_streaming/MEMBER3_IEEE_OUTPUT_CONTRACT.md
   - Schema: 483 columns (identical to data/features/ieee_features.parquet)
   - Dedupe Key: TransactionID

Key Guarantees:
- At-least-once delivery with zero data loss.
- 100% numerical and null parity against Member 2's batch feature tables.
- Byte-identical duplicate messages upon crash recovery.
- Full validation guards and DLQ routing.

Requirement:
Downstream scoring consumers must deduplicate incoming records on txn_id (PaySim) or TransactionID (IEEE).

Integration Question:
Are Kafka topics (financial_features_paysim and financial_features_ieee) your preferred ingestion sink for model inference, or do you require an alternative destination (such as Parquet directory, Delta Lake table, or gRPC stream)?

(Current default: Kafka topics pending your confirmation.)
```
