# Chunk 5 Report: Member 4 Handoff

**Execution Mode**: Real Kafka: **YES** | Real Spark Structured Streaming query: **N/A** (Downstream Consumer Validation)  
**Date**: 2026-10-13  
**Status**: **PASSED**

---

## 1. Handoff File & Message Summary

The handoff documentation and ready-to-send message for Member 4 were created at:
[`member3_streaming/HANDOFF_MEMBER4.md`](file:///e:/vs%20code/projects/data-science/Real-Time-Financial-Fraud-Intelligence/member3_streaming/HANDOFF_MEMBER4.md)

### Key Items Communicated:
- **Primary Feature Topic**: `financial_features_paysim`
- **Contract Path**: `member3_streaming/MEMBER3_OUTPUT_CONTRACT.md`
- **Output Schema**: 42 columns (100% parity with batch Tier A feature store)
- **Primary Deduplication Key**: `txn_id`
- **Pipeline Startup Instructions**: CLI command for starting the streaming consumer
- **Operational Guarantees & Known Limitations**: At-least-once delivery; byte-identical duplicates; requirement for downstream deduplication on `txn_id`; Apache Spark 4.1.1 uncommitted orphan offset metadata recovery workaround on crash (Spark 4.1.1 NullPointerException in KafkaMicroBatchStream.metrics when replaying an uncommitted batch (upstream ticket not verified)).
- **Direct Integration Question**: "Do you want to consume this Kafka topic, or do you need a different sink (e.g. Parquet, Delta Lake, gRPC)?"

---

## 2. Example Consumer Execution & Proof

A 15-line example consumer was implemented in [`member3_streaming/example_consumer_member4.py`](file:///e:/vs%20code/projects/data-science/Real-Time-Financial-Fraud-Intelligence/member3_streaming/example_consumer_member4.py) and executed against live Kafka topic `financial_features_paysim`.

### Raw Execution Output:

```text
Processed unique txn_id: 07dea4e0818f1a1d3d80dcd9f1c89e5af451d15fa5f8d321c920b519c5af27a0 (step=1, amount=215310.3)
Processed unique txn_id: c8c42e7d6e2d8adff71d50b23f591e1ce8106c58d40d407324091bd415d0a198 (step=1, amount=63255.29)
Processed unique txn_id: e792bdbd20122dc667aecad385a3d58b55d3460e66131792021249be48c6871f (step=1, amount=142577.44)
Processed unique txn_id: 0243f8824760d9cc52d9faea4a78b9a207cb54f812a2de7e9386f591178c94b0 (step=1, amount=8727.74)
Processed unique txn_id: a44ea95986bbc6d4beecb6a84a4dc3cb89cceca901c35a2d9047bdc2a3b65886 (step=1, amount=114712.48)
Processed unique txn_id: d724fc13df8a5e8c8ff0d7ce48fb944c88383389a72b46bc3f31c252053a6481 (step=1, amount=259753.27)
Processed unique txn_id: 92ef135ca0545928b93395902852ebf84ad2d43d2407b0e17ec83f51f4dbb31f (step=1, amount=11996.58)
Processed unique txn_id: 42869e72c61475210d1f6d26f402e0119cffb461125fdf159ef1e73558cbcf33 (step=1, amount=26004.52)
Processed unique txn_id: ea2d0edf1e7d219c38974600fd6160803f1ec548a59d9f544b3171141ed0c067 (step=1, amount=13289.87)
Processed unique txn_id: 7a732e9b8424b1027bd3d7dcf34f352bb50f99862011ee8456cd505eebe91e8e (step=1, amount=522264.81)
Total unique records consumed and deduplicated: 10
```

---

## 3. Decision Log Verification

The default design decision in case Member 4 does not request an alternative sink was recorded in [`member3_streaming/DECISIONS.md`](file:///e:/vs%20code/projects/data-science/Real-Time-Financial-Fraud-Intelligence/member3_streaming/DECISIONS.md):
- **Choice**: Kafka topic `financial_features_paysim` is the primary sink interface.
- **Alternatives considered**: Parquet partition files, Delta table, gRPC.
- **Rationale**: Real-time pub/sub decoupling enables sub-second model scoring with consumer group scaling.
