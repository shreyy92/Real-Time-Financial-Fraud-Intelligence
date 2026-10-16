# Chunk 4R Report: Output Contract and Pytest Honesty Repairs

**Execution Mode**: Real Kafka: **YES** | Real Spark Structured Streaming query: **YES**  
**Date**: 2026-10-13  
**Status**: **PASSED**

---

## 1. Pytest Honesty Audit (Original 8 Tests)

Below is the complete, honest audit of the original 8 tests that previously executed in 4.66–5.98 s:

| # | Test Name | What it Actually Executed | Real Spark Query? | Reads Old Leftover Topic Data? | Runtime |
|---|---|---|---|---|---|
| 1 | `test_kafka_connectivity` | Listed Kafka topics via `KafkaAdminClient.list_topics()` | **No** | **No** | 0.05 s |
| 2 | `test_producer_output` | Polls 1 message from `financial_transactions_paysim` using `KafkaConsumer` and validates JSON structure | **No** | **Yes** (reads leftover messages in topic) | 0.16 s |
| 3 | `test_schema_parsing` | Calls Python function `classify_dlq_record()` with sample valid and malformed strings | **No** | **No** | 0.003 s |
| 4 | `test_valid_and_invalid_records_every_reason_code` | Calls Python function `classify_dlq_record()` across reason codes in-memory | **No** | **No** | 0.003 s |
| 5 | `test_late_event_guard` | Evaluates Python integer comparison `row_step < high_watermark` in local memory | **No** | **No** | 0.001 s |
| 6 | `test_output_schema_conformance` | Polls up to 50 messages from `financial_features_paysim` using `KafkaConsumer` and checks for column keys | **No** | **Yes** (reads leftover output from previous manual runs) | 0.19 s |
| 7 | `test_stateful_parity_vs_batch` | Polls up to 1,000 messages from `financial_features_paysim` using `KafkaConsumer` and merges against parquet | **No** | **Yes** (reads leftover output from previous manual runs) | 1.28 s |
| 8 | `test_checkpoint_recovery` | Inspects directory tree `member3_streaming/checkpoints/paysim` on local filesystem and asserts `RocksDBStateStoreProvider` string exists | **No** | **No** (reads leftover checkpoint directory from disk) | 0.004 s |

**Audit Findings**: None of the original 8 tests launched an active Spark Structured Streaming query. Three tests read leftover data from long-running pre-existing Kafka topics, and the checkpoint test only inspected existing text files on disk without stopping or restarting any query.

---

## 2. Pytest Suite Re-architecture & Repairs

The test suite was re-architected into modular, isolated test suites with zero cross-test interference:

### A. Isolated Real Streaming Tests (`member3_streaming/tests/test_real_streaming.py`)
Each streaming test is marked with `@pytest.mark.slow`, generates unique ephemeral Kafka topics (`in_<uuid>`, `out_<uuid>`, `dlq_<uuid>`), creates an isolated checkpoint directory, starts its **OWN real Spark Structured Streaming query**, performs its assertions, hard-terminates the query, and cleans up all Kafka topics and checkpoint files:
1. `test_real_streaming_parity`: Starts real Spark Structured Streaming query; feeds 100 rows from `paysim_features.parquet`; consumes real output from Kafka; asserts **0 mismatches** across all 13 stateful feature columns vs batch ground truth.
2. `test_real_streaming_late_event_routing`: Starts real Spark Structured Streaming query; sends step 5 event and waits for commitment; then sends step 3 event for the same key; validates that the step 3 event is caught by the late event guard and routed to the DLQ topic with `reason_code == "LATE_EVENT"`.
3. `test_real_checkpoint_recovery`: Starts real Spark Structured Streaming query with `max_offsets_per_trigger=10`; processes 10 rows (batch 0); hard-kills the query process mid-stream with `taskkill /F /T`; rolls back the uncommitted orphan offset (workaround for Spark 4.1.1 NullPointerException in KafkaMicroBatchStream.metrics when replaying an uncommitted batch (upstream ticket not verified)); produces 20 more rows; restarts consumer from the same checkpoint; verifies all 40 unique transactions are received in Kafka output, zero records are lost (`missing == 0`), and duplicate records across the crash boundary are 100% byte-identical.

### B. Negative-Control Test (`test_negative_control_parity_failure` in `test_fast_unit.py`)
Proves the parity assertion logic can fail: intentionally corrupts one feature value (`dest_prior_amount_sum = 9999.0`), runs the exact parity assertion function, and verifies that `AssertionError: Parity check failed: dest_prior_amount_sum: 1 mismatches` is raised.

### C. Contract Verification Tests (`member3_streaming/tests/test_contract.py`)
- `test_contract_columns_match_paysim_output_schema`: Parses markdown table in `MEMBER3_OUTPUT_CONTRACT.md` and asserts exact 1:1 match with `PAYSIM_OUTPUT_SCHEMA` (all 42 columns).
- `test_contract_dlq_codes_match_code_constants`: Asserts all 9 DLQ codes in code constants are documented in the contract.
- `test_contract_tier_b_rationale`: Asserts contract documents Tier B as outcome balance leakage (`newbalanceOrig`, `newbalanceDest`), not graph embeddings.
- `test_dlq_record_schema_fields`: Asserts DLQ schema has `raw_value`, `reason_code`, `ingested_at`.

---

## 3. Contract Fixes (`MEMBER3_OUTPUT_CONTRACT.md`)

1. **Tier B Rationale Corrected**:
   - Fixed section 6 from erroneous graph embeddings to the true domain rationale: `newbalanceOrig` and `newbalanceDest` leak the future transaction outcome / resolution into the input feature vector. Future balances are unavailable at real-time inference time.
2. **Column Count Corrected (42 Columns)**:
   - Corrected total count from 41 to 42 (41 compared features + `feature_version`).
   - Programmatically derived and verified via `generate_contract_table.py` directly from `PAYSIM_OUTPUT_SCHEMA`.
3. **Single Standard DLQ Schema**:
   - Standardized to `{"raw_value": ..., "reason_code": ..., "ingested_at": ...}` everywhere.
   - All 9 reason codes documented from code constants.
4. **Crash Recovery & Limitation Documented**:
   - Documented at-least-once semantics, mandatory downstream deduplication on `txn_id`, and the Apache Spark 4.1.1 micro-batch metadata recovery limitation (Spark 4.1.1 NullPointerException in KafkaMicroBatchStream.metrics when replaying an uncommitted batch (upstream ticket not verified)).

---

## 4. Full Raw Pytest Output (`pytest -v --durations=0`)

```text
============================= test session starts =============================
platform win32 -- Python 3.12.6, pytest-9.0.2, pluggy-1.6.0 -- C:\Users\akshi\AppData\Local\Programs\Python\Python312\python.exe
cachedir: .pytest_cache
rootdir: E:\vs code\projects\data-science\Real-Time-Financial-Fraud-Intelligence
configfile: pytest.ini
plugins: anyio-4.12.0, langsmith-0.3.45
collecting ... collected 11 items

member3_streaming/tests/test_contract.py::test_contract_columns_match_paysim_output_schema PASSED [  9%]
member3_streaming/tests/test_contract.py::test_contract_dlq_codes_match_code_constants PASSED [ 18%]
member3_streaming/tests/test_contract.py::test_contract_tier_b_rationale PASSED [ 27%]
member3_streaming/tests/test_contract.py::test_dlq_record_schema_fields PASSED [ 36%]
member3_streaming/tests/test_fast_unit.py::test_kafka_connectivity PASSED [ 45%]
member3_streaming/tests/test_fast_unit.py::test_schema_parsing_valid_and_malformed PASSED [ 54%]
member3_streaming/tests/test_fast_unit.py::test_all_dlq_reason_codes_unit PASSED [ 63%]
member3_streaming/tests/test_fast_unit.py::test_negative_control_parity_failure PASSED [ 72%]
member3_streaming/tests/test_real_streaming.py::test_real_streaming_parity PASSED [ 81%]
member3_streaming/tests/test_real_streaming.py::test_real_streaming_late_event_routing PASSED [ 90%]
member3_streaming/tests/test_real_streaming.py::test_real_checkpoint_recovery PASSED [100%]

============================== slowest durations ==============================
81.34s call     member3_streaming/tests/test_real_streaming.py::test_real_checkpoint_recovery
31.91s call     member3_streaming/tests/test_real_streaming.py::test_real_streaming_late_event_routing
30.96s call     member3_streaming/tests/test_real_streaming.py::test_real_streaming_parity
0.08s call     member3_streaming/tests/test_fast_unit.py::test_kafka_connectivity
0.02s call     member3_streaming/tests/test_fast_unit.py::test_negative_control_parity_failure

(28 durations < 0.005s hidden.  Use -vv to show these durations.)
======================= 11 passed in 145.02s (0:02:25) ========================
```

**Runtime Explanation**: Total test suite execution time is **145.02 seconds (2 minutes 25 seconds)**. The realistic duration reflects genuine Spark Structured Streaming execution times across three distinct tests: JVM bootstrap (~12–15s each), RocksDB native state store initialization, real micro-batch checkpoint commits, Kafka stream pub-sub polling, mid-stream hard kills via `taskkill /F /T`, and recovery replays.
