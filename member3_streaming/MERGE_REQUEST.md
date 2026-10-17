# Merge Request: Member 3 Real-Time Streaming Feature Store

## 1. Summary of Deliverables
This Merge Request delivers the complete production-grade PySpark Structured Streaming and Kafka feature engineering pipelines for **Member 3** (Real-Time Financial Fraud Intelligence system).

Key components delivered:
1. **PaySim Feature Engine (`consumer_paysim.py`)**:
   - Ingests raw JSON mobile money transactions from Kafka.
   - Evaluates input validity across 9 Dead Letter Queue (DLQ) codes.
   - Computes stateless transforms and stateful destination metrics (`applyInPandasWithState` on `nameDest` over 1h, 24h, 168h windows).
   - Emits 42-column enriched feature vectors to `financial_features_paysim`.
2. **IEEE-CIS Feature Engine (`consumer_ieee.py`)**:
   - Ingests raw JSON e-commerce transactions from Kafka.
   - Evaluates input validity across 6 DLQ codes.
   - Computes stateless transforms (472 columns) and stateful card behavioral metrics (`applyInPandasWithState` on `card_proxy_id` over 1h, 24h, 7d windows).
   - Emits 483-column enriched feature vectors to `financial_features_ieee`.
3. **Producers (`producer_paysim.py`, `producer_ieee.py`)**:
   - Deterministic streaming event producers with verified stable sort ordering.
4. **State Management & Checkpointing**:
   - RocksDB State Store (`RocksDBStateStoreProvider`, format v5) for scalable off-heap state management.
   - Byte-identical duplicate recovery after mid-batch crash (`dup_count > 0` verified on both pipelines).
   - Documented Operator Rollback Procedure for Spark 4.1.1 uncommitted batch replay NPE.
5. **Downstream Integration & Handoff**:
   - Complete reference deduplicating consumer (`example_consumer_member4.py`).
   - Detailed output contracts (`MEMBER3_OUTPUT_CONTRACT.md`, `MEMBER3_IEEE_OUTPUT_CONTRACT.md`) and handoff guide (`HANDOFF_MEMBER4.md`).
6. **Automated Test Suite**:
   - 25 automated tests across contract drift, fast unit, and real Kafka/Spark streaming integration suites.

---

## 2. Changed Areas
- `member3_streaming/consumer_paysim.py`: Real-time streaming consumer for PaySim.
- `member3_streaming/consumer_ieee.py`: Real-time streaming consumer for IEEE-CIS.
- `member3_streaming/producer_paysim.py`: Streaming producer for PaySim.
- `member3_streaming/producer_ieee.py`: Streaming producer for IEEE-CIS.
- `member3_streaming/ieee_raw_schema.py` & `ieee_output_schema.py`: IEEE input and 483-col output schema definitions.
- `member3_streaming/MEMBER3_OUTPUT_CONTRACT.md` & `MEMBER3_IEEE_OUTPUT_CONTRACT.md`: Output specifications and column schemas.
- `member3_streaming/example_consumer_member4.py`: Reference consumer implementation for downstream services.
- `member3_streaming/README.md`: Architecture diagram, startup instructions, rollback guide, and limitations.
- `member3_streaming/PROGRESS.md`: Complete Phase ledger tracking Phases A through D.
- `member3_streaming/reports/`: Detailed verification reports (`PA_report.md`, `PB_report.md`, `PC_report.md`, `PD_report.md`, `FINAL_SUMMARY.md`) and raw logs in `raw/`.
- `member3_streaming/tests/`: 25 automated tests covering contracts, schemas, DLQ routing, parity, and checkpoint recovery.

---

## 3. How to Run

### 3.1 Start Kafka & Initialize Topics
```bash
# Run local Kafka broker (Docker KRaft)
docker run -d --name kafka -p 9092:9092 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_NUM_PARTITIONS=1 apache/kafka:3.9.0

# Initialize topics
python member3_streaming/run_phase_d_e2e_demo.py
```

### 3.2 Run Automated Tests
```bash
# Run full 25-test suite
pytest member3_streaming/tests/ -v --durations=0
```

---

## 4. Test Evidence
- **Automated Test Suite (Workspace)**: 25 passed in 859.53s (`reports/raw/PC_pytest_full.txt`).
- **Fresh-Clone Test Suite**: Initial continuous run yielded 19 passed / 6 failed (`reports/raw/PD_fresh_clone_pytest.txt`). In Phase D follow-up (PDF), isolated reruns of the 6 failed tests in a fresh clone achieved 100% pass (6/6 passed, `reports/raw/PDF_rerun_*.txt`), and a full suite rerun in the fresh clone achieved 24 passed / 1 failed (`reports/raw/PDF_pytest_full.txt`).
- **End-to-End Demo**: Clean run for PaySim (25 records, 42 cols) and IEEE (25 records, 483 cols) with downstream deduplication (`reports/raw/PD_e2e_demo.txt`).
- **Checkpoint Crash Recovery**: PaySim (10 byte-identical duplicates) and IEEE (10 byte-identical duplicates) verified (`reports/PC_report.md`).
- **Negative Controls**: Proven failure detection on corrupted feature values (`reports/raw/PC_negative_controls.txt`).
- **Fresh Clone Import**: Clean import verified without missing dependencies (`reports/raw/PD_fresh_clone_pytest.txt`).

---

## 5. Known Limitations
1. **Micro-Batch Barrier Reordering**: Events arriving within a single micro-batch are sorted ascending by timestamp/step inside the state function; intra-batch out-of-order events are reordered rather than rejected as `LATE_EVENT`.
2. **Spark 4.1.1 Crash Recovery Replay NPE**: Hard kills mid-batch leave an uncommitted offset file triggering a `KafkaMicroBatchStream.metrics` NPE on unassisted restart; recovery requires the documented Operator Workaround rollback.
3. **Continuous Test Suite Load on Windows**: Running all 25 streaming integration tests sequentially in rapid succession generates heavy OS socket churn on Windows, which can lead to temporary `BlockManagerMasterEndpoint` timeouts unless socket release cooldowns are observed (verified in PDF where isolated single runs with cooldown passed 6/6 and full suite passed 24/25).
4. **Member 4 Sink Status**: Integration interfaces and deduplicating downstream consumers are fully implemented and demonstrated in `member3_streaming/example_consumer_member4.py` and `HANDOFF_MEMBER4.md`. However, confirmation from the downstream modeling team regarding final production sinks remains **unanswered as of 2026-10-17**.

---

## 6. Reviewer Checklist
- [ ] No files in `member2_spark/` or `member_audit1/` were modified.
- [ ] Git history is strictly forward-only; zero amends, rebases, or force pushes.
- [ ] Real Kafka and real PySpark queries used for all streaming verifications; no mocks.
- [ ] All output columns adhere to documented schemas (42 cols PaySim, 483 cols IEEE).
- [ ] Checkpoint crash recovery produces byte-identical duplicates deduplicated downstream.
- [ ] Raw execution logs saved in `member3_streaming/reports/raw/` supporting every claim.
