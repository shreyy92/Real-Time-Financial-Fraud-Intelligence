# Member 3 Final Delivery Summary: Real-Time Streaming Feature Store

## 1. Project Overview & Deliverables Summary
Member 3 implements the real-time financial fraud feature ingestion, processing, and outbound streaming architecture for the Real-Time Financial Fraud Intelligence system. The module processes streaming events for two distinct datasets:
1. **PaySim Pipeline**: Real-time mobile money transactions, computing 42 output columns with stateful destination history over 1-hour, 24-hour, and 168-hour windows keyed on `nameDest`.
2. **IEEE-CIS Pipeline**: High-dimensional e-commerce transactions, computing 483 output columns (472 stateless + 11 stateful card metrics over 1-hour, 24-hour, and 7-day windows keyed on `card_proxy_id`).

Both engines run on **Apache Kafka 3.9.0 (KRaft)** and **PySpark Structured Streaming 4.1.1** with **RocksDB StateStoreProvider** (format v5). Both pipelines feature deterministic Dead Letter Queue (DLQ) routing, explicit null preservation, at-least-once delivery, and byte-identical duplicate crash recovery.

---

## 2. Roadmap Cross-Check Table

| Roadmap Phase | Milestone Description | Evidence Pointer | Status | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **0. Environment** | Kafka KRaft broker, PySpark 4.1.1, Java 17, Hadoop native lib setup | Smoke test report, `reports/raw/PC_pytest_full.txt` | **passed** | Tested on Windows with real Kafka at `127.0.0.1:9092` and RocksDB state provider. |
| **1. Producers (both)** | PaySim and IEEE raw event streaming producers with stable sort ordering | C07 report, Phase A report (`PA_report.md`), `reports/raw/PA_clean_sort_check.txt` | **passed** | Verified stable sorting on `step` (PaySim) and `(TransactionDT, TransactionID)` (IEEE). Zero payload loss. |
| **2. Stateless Consumers & Validation** | JSON parsing, DLQ routing guards, and stateless feature extraction | C01R, C08, `PA_report.md`, `reports/raw/PC_pytest_full.txt` | **passed** | 472 stateless columns for IEEE; Tier B target/balance leakage fields excluded from PaySim. |
| **3. Stateful Windowed Features** | `applyInPandasWithState` on `nameDest` (PaySim) and `card_proxy_id` (IEEE) | C02R, R4, C09, `PA_report.md`, `PC_report.md`, `reports/raw/PC_negative_controls.txt` | **passed** | Exact parity against batch ground truth at tolerance 1e-6. Negative controls confirmed detectable. |
| **4. Checkpointing & Fault Tolerance** | State continuity, crash recovery, duplicate detection, and operator rollback | C02R, R3, C09 gate 1.5, `PC_report.md`, `reports/raw/PC_pytest_full.txt` | **passed** | 10 duplicates verified on crash recovery for both pipelines (`dup_count > 0`, byte-identical). Operator rollback documented for Spark 4.1.1 uncommitted batch replay NPE. |
| **5. Output & Member 4 Integration** | Contracts, drift checks, handoff specifications, downstream dedupe consumer | `PB_report.md`, `MEMBER3_OUTPUT_CONTRACT.md`, `MEMBER3_IEEE_OUTPUT_CONTRACT.md`, `HANDOFF_MEMBER4.md` | **passed** | 42 cols (PaySim) and 483 cols (IEEE). Schema drift tests pass. Member 4 sink status recorded. |
| **6. Testing, Docs, Git Hygiene** | Monolithic test suite (25/25 pass), clean end-to-end demo, README, MR docs | `PC_report.md`, `PD_report.md`, `reports/raw/PC_pytest_full.txt`, `reports/raw/PD_e2e_demo.txt`, `README.md`, `MERGE_REQUEST.md` | **passed** | Forward-only git history on `member3-streaming`; zero amends, rebases, or force pushes. |

---

## 3. What Was Built & Verified

### 3.1 Streaming Pipelines
- **PaySim Ingestion & Feature Engine**: `member3_streaming/consumer_paysim.py`
  - Ingests from `financial_transactions_paysim`.
  - Classifies and routes malformed payloads across 9 DLQ reason codes to `financial_transactions_paysim_dlq`.
  - Maintains state on `nameDest` via `applyInPandasWithState`.
  - Emits 42 columns to `financial_features_paysim`.
- **IEEE-CIS Ingestion & Feature Engine**: `member3_streaming/consumer_ieee.py`
  - Ingests from `financial_transactions_ieee`.
  - Classifies and routes malformed payloads across 6 DLQ reason codes to `financial_transactions_ieee_dlq`.
  - Maintains state on `card_proxy_id` via `applyInPandasWithState` using Welford algorithm for running variance and bisect-based trailing window eviction.
  - Emits 483 columns (472 base + 11 stateful) to `financial_features_ieee`.

### 3.2 Automated Test Suite (25 Tests)
All 25 automated tests run against **real Kafka (`127.0.0.1:9092`)** and **real PySpark Structured Streaming** queries using ephemeral topics and isolated checkpoints:
- **`test_contract.py`** (4 tests): Verifies 42 PaySim output columns against schema, DLQ reason codes set equality, Tier B leakage exclusion rationale, and DLQ record schema fields.
- **`test_fast_unit.py`** (4 tests): Fast unit verification of Kafka connectivity, valid/malformed JSON parsing, all 9 DLQ reason codes, and negative control parity failure detection.
- **`test_ieee_contract.py`** (3 tests): Verifies 483 IEEE output columns against PySpark schema and parquet ground truth, DLQ reason codes set equality, and DLQ record schema fields.
- **`test_ieee_streaming.py`** (7 tests): Fast negative control, stateless small parity (500 rows, 471 compared cols), synthetic tie/boundary oracle, long-span 7-day eviction parity, late event routing to DLQ, all 6 DLQ reason codes integration, and checkpoint crash recovery with operator rollback (`dup_count == 10 > 0`).
- **`test_real_streaming.py`** (7 tests): Real PaySim streaming parity (1000 rows, 41 compared cols), all 9 DLQ reason codes integration, late event routing to DLQ, late event pending step logic, checkpoint crash recovery (`dup_count == 10 > 0`), plain restart NPE documentation, and long-span 24h/168h window eviction parity.

**Evidence**: Full monolithic test run passed in `859.53s (14:19)` saved to [`reports/raw/PC_pytest_full.txt`](raw/PC_pytest_full.txt).

### 3.3 End-to-End Demonstration
Clean end-to-end execution verified via `member3_streaming/run_phase_d_e2e_demo.py`:
- Ingested fresh transactions for both PaySim and IEEE pipelines into freshly initialized topics.
- PySpark consumers processed transactions and wrote enriched vectors to output topics.
- Downstream deduplicating consumer read all records, asserted exactly 25 deduplicated records per pipeline, and verified schema compliance (42 cols PaySim, 483 cols IEEE).
- **Evidence**: [`reports/raw/PD_e2e_demo.txt`](raw/PD_e2e_demo.txt).

### 3.4 Fresh-Clone Clean Import
- Executed `run_phase_d_fresh_clone.py` on fresh isolated clone of `origin/member3-streaming`.
- Verified clean import of `member3_streaming.consumer_paysim` and `member3_streaming.consumer_ieee` without missing dependencies or untracked file references.
- **Evidence**: [`reports/raw/PD_fresh_clone_pytest.txt`](raw/PD_fresh_clone_pytest.txt).

---

## 4. Known Limitations & Honest Disclosures

1. **Micro-Batch Barrier Reordering**: Events arriving within a single PySpark micro-batch are sorted ascending by timestamp/step inside the state function. Consequently, intra-batch out-of-order events are reordered rather than rejected as `LATE_EVENT`. Only inter-batch late arrivals strictly older than the committed watermark are routed to the DLQ.
2. **Spark 4.1.1 Crash Recovery Replay NPE**: Hard-killing a stateful consumer mid-batch leaves an uncommitted offset file (`offsets/N`) without a corresponding commit file (`commits/N`). Restarting without assistance triggers an internal `KafkaMicroBatchStream.metrics` NPE (reproduced and documented in `test_real_checkpoint_plain_restart_failure`). Recovery requires the documented Operator Workaround: deleting `offsets/N` and `offsets/.N.crc` prior to restart.
3. **Sequential Test Suite Load on Windows**: Running all 25 streaming integration tests sequentially in rapid succession generates heavy OS socket churn on Windows, which can lead to temporary `BlockManagerMasterEndpoint` timeouts unless socket release cooldowns are observed between test runs.
4. **Member 4 Sink Status**: Integration interfaces and deduplicating downstream consumers are fully implemented and demonstrated in `member3_streaming/example_consumer_member4.py` and `HANDOFF_MEMBER4.md`. However, confirmation from the downstream modeling team regarding final production sinks remains **unanswered as of 2026-10-17**.

---

## 5. Member 4 / Member 5 Handoff Status

- **Outbound Topics Available**:
  - `financial_features_paysim` (42 columns, JSON, keyed on `nameDest`, deduplicated on `txn_id`)
  - `financial_features_ieee` (483 columns, JSON, keyed on `card_proxy_id`, deduplicated on `TransactionID`)
  - `financial_transactions_paysim_dlq` (Dead letter queue for PaySim)
  - `financial_transactions_ieee_dlq` (Dead letter queue for IEEE-CIS)
- **Null Semantics**: Explicit null preservation (`ignoreNullFields = false`).
- **Downstream Consumer Guide**: Complete reference implementation available at [`member3_streaming/example_consumer_member4.py`](../example_consumer_member4.py).
- **Handoff Document**: [`member3_streaming/HANDOFF_MEMBER4.md`](../HANDOFF_MEMBER4.md).
