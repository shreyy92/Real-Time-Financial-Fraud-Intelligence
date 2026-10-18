# Member 3: Real-Time Streaming Feature Store (PaySim & IEEE-CIS)

> **Historical Evidence Archive:** Full raw execution logs, benchmark text outputs, and prior phase reports are archived at git tag `member3-evidence-archive`. Retrieve evidence files with: `git show member3-evidence-archive:<path>`.

Production-grade PySpark Structured Streaming engines and Kafka streaming pipelines for real-time transaction feature engineering on both the PaySim and IEEE-CIS datasets.

---

## 1. Architecture Diagram

```mermaid
flowchart TD
    subgraph Ingestion["Kafka Ingestion Layer"]
        P1[PaySim Producer] -->|JSON Transactions| T_PAY_IN["financial_transactions_paysim"]
        P2[IEEE Producer] -->|JSON Transactions| T_IEEE_IN["financial_transactions_ieee"]
    end

    subgraph Processing["Spark Structured Streaming Layer (PySpark 4.1.1)"]
        T_PAY_IN --> SP_PAY["PaySim Consumer Engine"]
        T_IEEE_IN --> SP_IEEE["IEEE Consumer Engine"]

        SP_PAY -->|Payload Validation| V1{Valid?}
        V1 -->|No| DLQ_PAY["financial_transactions_paysim_dlq"]
        V1 -->|Yes| ST_PAY["Stateless Transforms + applyInPandasWithState(nameDest)"]

        SP_IEEE -->|Payload Validation| V2{Valid?}
        V2 -->|No| DLQ_IEEE["financial_transactions_ieee_dlq"]
        V2 -->|Yes| ST_IEEE["Stateless Transforms + applyInPandasWithState(card_proxy_id)"]

        subgraph StateStore["State Management"]
            ROCKS_PAY[("RocksDB State Store (nameDest)")] <--> ST_PAY
            ROCKS_IEEE[("RocksDB State Store (card_proxy_id)")] <--> ST_IEEE
        end
    end

    subgraph Delivery["Outbound Delivery & Consumption"]
        ST_PAY -->|42 Columns| T_PAY_OUT["financial_features_paysim"]
        ST_IEEE -->|483 Columns| T_IEEE_OUT["financial_features_ieee"]

        T_PAY_OUT --> C_M4_PAY["Member 4 / Inference Service (Dedupe txn_id)"]
        T_IEEE_OUT --> C_M4_IEEE["Member 4 / Inference Service (Dedupe TransactionID)"]
    end
```

---

## 2. Topic Topology & Contracts Table

| Dataset | Inbound Input Topic | Outbound Feature Topic | Dead Letter Queue (DLQ) | State Key | Column Count | Deduplication Key | Contract File |
|---|---|---|---|---|---|---|---|
| **PaySim** | `financial_transactions_paysim` | `financial_features_paysim` | `financial_transactions_paysim_dlq` | `nameDest` | 42 | `txn_id` (SHA-256 string) | [`MEMBER3_OUTPUT_CONTRACT.md`](MEMBER3_OUTPUT_CONTRACT.md) |
| **IEEE-CIS** | `financial_transactions_ieee` | `financial_features_ieee` | `financial_transactions_ieee_dlq` | `card_proxy_id` | 483 | `TransactionID` (int) | [`MEMBER3_IEEE_OUTPUT_CONTRACT.md`](MEMBER3_IEEE_OUTPUT_CONTRACT.md) |

### State Store Provider
Both pipelines utilize `org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider` (format version 5). RocksDB offloads state to local disk / SST files, supporting high-cardinality keys (2.7M+ destinations in PaySim, millions of card proxies in IEEE) without JVM heap exhaustion.

---

## 3. Prerequisites & Environment

1. **Operating System**: Windows 10/11 or Linux / macOS.
2. **Docker Desktop**: Running local Kafka broker via Docker.
   - Recommended image: `apache/kafka:3.9.0` (KRaft mode).
   - Kafka bootstrap server: `127.0.0.1:9092`.
3. **Java Runtime**: Eclipse Adoptium JDK 17 (Java 17 LTS).
4. **Python**: Python 3.12.
   - Install dependencies using the pinned environment: `pip install -r requirements.txt`.
   - Run consumers with the environment's Python interpreter:
     ```bash
     python -m member3_streaming.consumer_paysim --bootstrap-servers localhost:9092
     ```
5. **PySpark**: Version 4.1.1 (Kafka connector package: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1`).
6. **Data Files (`data/` is not in git)**:
   - Excluded via `.gitignore`. The streaming producers and verification require:
     - `data/processed/paysim_clean.parquet`
     - `data/processed/ieee_clean.parquet`
     - `data/features/*.parquet` (`data/features/paysim_features.parquet`, `data/features/ieee_features.parquet`)
   - Link locally with a directory junction: `cmd /c mklink /J data <path_to_data>`
7. **Windows Hadoop Native Libraries**:
   - On Windows, PySpark requires Hadoop binaries.
   - Set environment variable: `HADOOP_HOME=C:\hadoop`.
   - Ensure `C:\hadoop\bin\hadoop.dll` and `C:\hadoop\bin\winutils.exe` are present.
   - Both consumers automatically invoke `System.load(r"C:\hadoop\bin\hadoop.dll")` on initialization if detected.

---

## 4. Execution & Startup Guide

### 4.1 Start Kafka (Docker KRaft)
```bash
docker run -d --name kafka -p 9092:9092 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_NUM_PARTITIONS=1 apache/kafka:3.9.0
```

### 4.2 Standard Topic Creation / Reset
Automated idempotent topic creation (provisions all 6 topics):
```bash
python member3_streaming/create_topics.py
```

Manual single-line CLI topic reset:
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

### 4.3 Run Producers
```bash
# PaySim Producer (streams raw transactions sorted by step)
python member3_streaming/producer_paysim.py --limit 20000 --delay 0

# IEEE-CIS Producer (streams raw transactions sorted stably by TransactionDT, TransactionID)
python member3_streaming/producer_ieee.py --limit 20000 --delay 0
```

### 4.4 Run Consumers
```bash
# PaySim Streaming Consumer (writes 42 columns to financial_features_paysim)
python -m member3_streaming.consumer_paysim --bootstrap-servers localhost:9092

# IEEE-CIS Streaming Consumer (writes 483 columns to financial_features_ieee)
python -m member3_streaming.consumer_ieee --bootstrap-servers localhost:9092
```

### 4.5 Run Test Suite
```bash
# Fast unit & contract drift tests
pytest member3_streaming/tests/test_contract.py member3_streaming/tests/test_ieee_contract.py member3_streaming/tests/test_fast_unit.py -v

# Full automated test suite (includes slow real-streaming tests)
pytest member3_streaming/tests/ -v --durations=0
```

---

## 5. Dead Letter Queue (DLQ) & Guards

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

## 6. Fault Tolerance & Recovery Procedure

- Both pipelines provide **at-least-once** delivery guarantees.
- In crash tests involving hard kills (`taskkill /F /T`) mid-batch:
  - RocksDB restores state store checkpoints exactly up to the last committed batch.
  - Spark 4.1.1 exhibits a `KafkaMicroBatchStream.metrics` NullPointerException when replaying an uncommitted batch of stateful queries, with provider independence shown (RocksDB and HDFS), upstream ticket not verified.

### 6.1 IEEE Stateful Crash Recovery (Restart-Twice, No File Edits)
- After a hard crash mid-batch, restart the consumer; the first restart completes the interrupted batch and then exits non-zero with the Spark 4.1.1 `KafkaMicroBatchStream.metrics` NPE; restart it again and it continues normally. No file edits.
- **Evidence:** `git show member3-evidence-archive:member3_streaming/reports/raw/PDG_restart_twice.txt` (IEEE stateful, one run, kill at 2,123, 412 byte-identical duplicates, 0 missing, 0 stateful mismatches).
- **Supervisor Requirement:** Unattended recovery requires a supervisor that restarts the process on a non-zero exit; this was not built or tested.
- **Scope & Limitations:** Verified on IEEE stateful only, one run; PaySim restart-twice is UNTESTED (contract still documents the PaySim operator workaround). Duplicates up to the in-flight batch size are expected; downstream consumers must deduplicate on `TransactionID` (IEEE) or `txn_id` (PaySim).
- **Legacy procedure (not needed for IEEE):** The previous manual operator rollback procedure (stopping consumer, deleting uncommitted `offsets/N` and `offsets/.N.crc`) is not needed. Note that deleting committed files is unsafe; **never delete `commits/`**.

### 6.2 PaySim Crash Recovery (Documented Operator Workaround)
- **OPERATOR WORKAROUND (Documented Rollback Procedure for PaySim)**:
  1. Terminate running consumer (`taskkill /F /T` or `kill -9`).
  2. Inspect checkpoint directory `offsets/` vs `commits/`.
  3. Identify uncommitted orphan offset index $N$ where `offsets/N` exists but `commits/N` does NOT exist.
  4. Delete ONLY `offsets/N` and `offsets/.N.crc`. **Never delete committed batch files (`commits/`)**.
  5. Restart consumer stream. Spark plans batch $N$ cleanly from committed offset $N-1$, restoring complete state continuity with byte-identical duplicate output.
  6. Downstream consumers deduplicate records on `txn_id`.

---

## 7. Known Limitations (Honest List)

1. **Micro-Batch Reordering**: Events arriving within a single micro-batch are sorted ascending by timestamp/step inside the state function, meaning intra-batch out-of-order events are absorbed and reordered, not rejected as `LATE_EVENT`.
2. **Spark 4.1.1 Mid-Batch Replay Crash**: Hard-killing a stateful consumer mid-batch leaves an uncommitted offset file that triggers a `KafkaMicroBatchStream.metrics` NPE on unassisted single restart; on IEEE stateful, restart-twice recovers with no file edits (unattended recovery requires a process supervisor; PaySim restart-twice is untested, operator rollback documented).
3. **Windows Test Suite Flakiness**: Even with the pinned environment, the streaming tests are occasionally flaky on Windows (BlockManager NPE); run them one at a time with python member3_streaming/run_slow_tests_isolated.py. Fast tests pass. The streaming tests pass when run alone, but runs on Windows are flaky during sequential execution (BlockManager NPE or timeouts, cause UNVERIFIED). Observed fresh-clone results: continuous runs 19 passed / 6 failed, then 24 passed / 1 failed; isolated per-test runner with 60 s cooldown ran 23 passed / 2 failed (batch failures: `test_ieee_stateless_parity_small` and `test_real_streaming_parity`, with `test_ieee_stateless_parity_small` passing on individual re-run; root cause UNVERIFIED); working copy continuous 25 passed (once). When using virtual environments on Windows, PySpark workers crash on launch (cause UNVERIFIED); when using the verified Python interpreter, tests and demo pass.
4. **Member 4 Sink Status**: Member 4 integration is fully drafted with downstream deduplication examples (`example_consumer_member4.py`), but downstream team confirmation of final production sinks remains unanswered as of 2026-10-18.

---

## 8. Directory Layout

```text
member3_streaming/
├── __init__.py                        # Package root
├── docker-compose.yml                 # Single-node Apache Kafka (KRaft) broker
├── create_topics.py                   # Automated Kafka topic provisioning
├── producer_paysim.py                 # PaySim streaming Kafka producer
├── producer_ieee.py                   # IEEE-CIS streaming Kafka producer
├── produce_ieee_subset.py             # Deterministic subset producer for 15 long-span cards (used by restart-twice test)
├── consumer_paysim.py                 # PaySim Spark Structured Streaming consumer (42 cols)
├── consumer_ieee.py                   # IEEE-CIS Spark Structured Streaming consumer (483 cols)
├── ieee_raw_schema.py                 # Raw input schema for IEEE-CIS
├── ieee_output_schema.py              # Enriched 483-column output schema for IEEE-CIS
├── example_consumer_member4.py        # Reference downstream deduplicating consumer
├── python_worker.bat                  # Windows PySpark environment wrapper
├── MEMBER3_OUTPUT_CONTRACT.md         # 42-column PaySim output contract
├── MEMBER3_IEEE_OUTPUT_CONTRACT.md    # 483-column IEEE-CIS output contract
├── MEMBER3_WORK_SUMMARY.md            # Comprehensive summary of Member 3 work, architecture, & verification
├── MEMBER4_START_HERE.md              # Quickstart and integration guide for Member 4 downstream modeling
├── MERGE_REQUEST.md                   # Formal Merge Request documentation (kept for reference)
├── README.md                          # This documentation file
├── gen_ieee_output_schema.py          # Contract generator for IEEE schema
├── generate_contract_table.py         # Contract generator for PaySim table
├── generate_ieee_contract.py          # Contract generator for IEEE table
├── run_phase_d_e2e_demo.py            # Demo script: live PaySim + IEEE 25-record pipeline demo (manual tool, not part of pytest)
├── run_slow_tests_isolated.py         # Isolated test runner: runs each @pytest.mark.slow test with 60s cooldown (manual tool, not part of pytest)
├── run_restart_twice_test.py          # Restart-twice reproducer: runs IEEE mid-batch hard kill and verifies recovery (manual tool, not part of pytest)
└── tests/                             # Automated test suite (25 tests)
    ├── __init__.py
    ├── conftest.py                    # Ephemeral Kafka topic & checkpoint fixtures
    ├── test_contract.py               # PaySim contract & schema drift tests
    ├── test_fast_unit.py              # Unit tests for DLQ codes & Kafka connectivity
    ├── test_ieee_contract.py          # IEEE-CIS contract & schema drift tests
    ├── test_ieee_streaming.py         # IEEE-CIS streaming integration & recovery tests
    └── test_real_streaming.py         # PaySim streaming integration & recovery tests
```
