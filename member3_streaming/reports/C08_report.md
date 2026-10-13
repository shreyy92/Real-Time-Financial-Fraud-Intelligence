# Chunk 8 Report: IEEE Stateless Consumer and Parity Gate

## 1. Metadata
- **Chunk:** Chunk 8
- **Start Time:** 2026-10-13T23:15:00+05:30
- **End Time:** 2026-10-13T23:38:00+05:30
- **Status:** PASSED

## 2. Infrastructure Honesty
- **Real Kafka:** yes (Broker `localhost:9092` via Docker KRaft container `c0812e01f318`)
- **Real Spark Structured Streaming query:** yes (`readStream.format("kafka")` -> `add_stateless_features_ieee` -> `writeStream.format("kafka")`, PID 11256)

## 3. Discovery Findings
From Chunk 6 discovery:
- Raw IEEE schema contains 434 fields in `data/processed/ieee_clean.parquet`.
- Member 2's stateless transformation pipeline (`src/ieee/missingness_features.py`, `card_features.py`, `temporal_features.py`, `aggregate_features.py`, `build_features.py`) adds missingness block flags, card proxy hashes, cyclical temporal features, split boundaries, amount decimal fingerprints, and masked family aggregations (M, C, D), while dropping 14 low-information near-constants (`UNINFORMATIVE_NEAR_CONSTANTS`), and adding `feature_version="v1.0"`.
- Total stateless feature count: exactly 472 columns (out of 483 total columns in `ieee_features.parquet`, excluding the 11 stateful card history features).
- Required contract fields checked for DLQ missingness/type: `TransactionID`, `isFraud`, `TransactionDT`, `TransactionAmt`, `ProductCD`, `card1`. The ~234 null fields per transaction are legitimate domain missingness preserved as nulls.

## 4. Files Created or Changed
- `member3_streaming/ieee_raw_schema.py`: PySpark `StructType` schema containing all 434 raw typed IEEE columns.
- `member3_streaming/consumer_ieee.py`: Real-time Spark Structured Streaming consumer for IEEE stateless pipeline with DLQ branching.
- `member3_streaming/verify_stateless_ieee.py`: End-to-end verification script testing DLQ routing, schema matching, and 20,000-row column-by-column parity against `data/features/ieee_features.parquet`.

## 5. Gate Results Table

| # | Check | Result | Evidence Pointer |
|---|---|---|---|
| 8.1 | Real streaming query | PASS | `consumer_ieee.py` executed `readStream.format('kafka')` with PID 11256 on real broker `localhost:9092` |
| 8.2 | Parity on first 20,000 rows (by `TransactionDT`) | PASS | 0 mismatches across all 471 non-ID stateless columns evaluated vs batch parquet |
| 8.3 | Hash columns equality | PASS | `card_proxy_id` mismatches: 0, `card_addr_proxy_id` mismatches: 0 (strict string equality) |
| 8.4 | Null equivalence | PASS | Null <-> null treated as equal, null <-> non-null counted as mismatch; 0 mismatches |
| 8.5 | DLQ evaluation | PASS | 6 distinct bad messages injected -> 6 DLQ records routed with correct codes (`MALFORMED_JSON`, `MISSING_FIELD`, `BAD_FIELD_TYPE`, `NEGATIVE_AMOUNT`, `INVALID_LABEL`, `INVALID_TRANSACTIONDT`), query stayed active |
| 8.6 | Schema consistency | PASS | Observed 472 columns, expected 472 columns; symmetric difference `diff = set()` |

## 6. Raw Output
```text
Step 1: Resetting Kafka topics and checkpoint directory...
Step 2: Producing 20000 valid rows to financial_transactions_ieee using producer_ieee.py...
Producer finished successfully.
Step 3: Injecting 6 bad messages to test every DLQ reason code...
Injected 6 bad messages.
Step 4: Launching real Spark Structured Streaming consumer (consumer_ieee.py)...
Consumer process started with PID 11256. Polling financial_features_ieee and financial_transactions_ieee_dlq...
Consumed 0 / 20000 output records (1.0s elapsed)...
Consumed 0 / 20000 output records (4.0s elapsed)...
Consumed 0 / 20000 output records (7.0s elapsed)...
Consumed 0 / 20000 output records (10.0s elapsed)...
Consumed 0 / 20000 output records (13.0s elapsed)...
Consumed 0 / 20000 output records (16.0s elapsed)...
Consumed 0 / 20000 output records (19.0s elapsed)...
Consumed 0 / 20000 output records (22.0s elapsed)...
Consumed 20000 / 20000 output records (56.0s elapsed)...
Terminating consumer process...

================================================================================
GATE 8 VERIFICATION AUDIT
================================================================================
Gate 8.1: Real streaming query: YES (Executed readStream.format('kafka') on PID 11256)
Real Kafka: YES (Broker: localhost:9092)

Gate 8.5: DLQ Record Audit (Total received: 6 / 6):
  DLQ Code 'MALFORMED_JSON': PASSED
  DLQ Code 'MISSING_FIELD': PASSED
  DLQ Code 'BAD_FIELD_TYPE': PASSED
  DLQ Code 'NEGATIVE_AMOUNT': PASSED
  DLQ Code 'INVALID_LABEL': PASSED
  DLQ Code 'INVALID_TRANSACTIONDT': PASSED
Gate 8.5 PASSED: Exactly 1 DLQ record per bad message, query stayed active, valid rows unaffected.

Gate 8.6: Schema Column Count Check:
  Expected stateless columns count: 472
  Observed stream output columns count: 472
  Column diff: set()
Gate 8.6 PASSED: Output column set strictly matches Member 2's stateless set.

Gate 8.2 & 8.3 & 8.4: Parity Evaluation on 20000 Transactions vs Parquet Ground Truth:
Merged row count: 20000 / 20000

Gate 8.3: Hash Columns Equality:
  card_proxy_id mismatches: 0
  card_addr_proxy_id mismatches: 0
Gate 8.3 PASSED: Hash columns show strict equality (0 mismatches).

Total Stateless Columns Evaluated: 472
Total Column Mismatches: 0
Gate 8.2 & 8.4 PASSED: 0 mismatches across all 471 stateless compared columns on 20,000 rows!

================================================================================
ALL CHUNK 8 GATES PASSED WITH 100% SUCCESS!
================================================================================
```

## 7. Fix Attempts
1. **Windows Hadoop NativeIO Binding:** Initial run in task-1186 encountered `UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0'` because Windows Hadoop `hadoop.dll` was not explicitly loaded in `consumer_ieee.py`. Added explicit `HADOOP_HOME` configuration and `jvm.java.lang.System.load(r"C:\hadoop\bin\hadoop.dll")` to `consumer_ieee.py` and `verify_stateless_ieee.py`, mirroring `consumer_paysim.py`. The second run succeeded with 0 errors.

## 8. Surprises / Limitations
- None. All 472 stateless columns matched Member 2's batch Parquet ground truth with 0 mismatches.

## 9. NOT DONE
- None. All Gate 8 requirements passed.
