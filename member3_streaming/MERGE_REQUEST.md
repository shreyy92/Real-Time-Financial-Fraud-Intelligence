# Merge Request: Member 3 Real-Time Streaming Feature Store

> **Historical Evidence Archive:** Full raw execution logs, benchmark text outputs, and prior phase reports are archived at git tag `member3-evidence-archive`. Retrieve evidence files with: `git show member3-evidence-archive:<path>`.

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
   - Verified restart-twice crash recovery without file edits for IEEE stateful (`git show member3-evidence-archive:member3_streaming/reports/raw/PDG_restart_twice.txt`); documented Operator Workaround rollback for PaySim.
5. **Downstream Integration & Handoff**:
   - Complete reference deduplicating consumer (`example_consumer_member4.py`).
   - Detailed output contracts (`MEMBER3_OUTPUT_CONTRACT.md`, `MEMBER3_IEEE_OUTPUT_CONTRACT.md`) and handoff guide (`MEMBER4_START_HERE.md`).
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
- `member3_streaming/MEMBER3_WORK_SUMMARY.md`: Comprehensive engineering record, architecture, design decisions, and verification.
- `member3_streaming/MEMBER4_START_HERE.md`: Self-contained onboarding guide and integration handbook for Member 4.
- `member3_streaming/example_consumer_member4.py`: Reference consumer implementation for downstream services.
- `member3_streaming/README.md`: Architecture diagram, startup instructions, rollback guide, and limitations.
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
- **Automated Test Suite**: Fast tests pass. The streaming tests pass when run alone, but runs on Windows are flaky (BlockManager NPE or timeouts, cause unverified). Observed fresh-clone results: continuous runs 6 failed/19 passed and 1 failed/24 passed; isolated run with 60 s cooldown 2 failed/23 passed; working copy continuous 25 passed (once). The failing test differs between runs. `test_real_streaming_late_event_routing` failed in 3 of 4 fresh-clone runs; cause unverified (possibly test timing).
- **End-to-End Demo**: Clean run for PaySim (25 records, 42 cols) and IEEE (25 records, 483 cols) with downstream deduplication (`git show member3-evidence-archive:member3_streaming/reports/raw/PD_e2e_demo.txt`).
- **Checkpoint Crash Recovery**: Verified restart-twice recovery without file edits on IEEE stateful (412 byte-identical duplicates, 0 missing, 0 mismatches; `git show member3-evidence-archive:member3_streaming/reports/raw/PDG_restart_twice.txt`). Documented operator rollback workaround for PaySim.
- **Negative Controls**: Proven failure detection on corrupted feature values (`git show member3-evidence-archive:member3_streaming/reports/raw/PC_negative_controls.txt`).
- **Fresh Clone Import**: Clean import verified without missing dependencies (`git show member3-evidence-archive:member3_streaming/reports/raw/PD_fresh_clone_pytest.txt`).

---

## 5. Known Limitations
1. **Micro-Batch Barrier Reordering**: Events arriving within a single micro-batch are sorted ascending by timestamp/step inside the state function; intra-batch out-of-order events are reordered rather than rejected as `LATE_EVENT`.
2. **Spark 4.1.1 Crash Recovery Replay NPE**: Hard kills mid-batch leave an uncommitted offset file triggering a `KafkaMicroBatchStream.metrics` NPE on unassisted single restart; on IEEE stateful, restart-twice recovers with no file edits (unattended recovery requires a process supervisor; PaySim restart-twice is untested, operator rollback documented).
3. **Windows Test Suite Flakiness & Virtualenv Failure**: Fast tests pass. The streaming tests pass when run alone, but runs on Windows are flaky during sequential execution (BlockManager NPE or timeouts, cause UNVERIFIED). Observed fresh-clone results: continuous runs 6 failed/19 passed and 1 failed/24 passed; isolated run with 60 s cooldown 2 failed/23 passed; working copy continuous 25 passed (once). In empirical testing on Windows, both the repo virtual environment and a fresh virtual environment built strictly from `requirements.txt` failed with worker socket crash EOFExceptions (even on a trivial pandas UDF), whereas the base system interpreter with identical pinned versions passed all isolated tests and the live demo; package versions alone do not explain the failure (virtualenvs failed on Windows; Linux/WSL/Docker untested; cause UNVERIFIED).
4. **Member 4 Sink Status**: Integration interfaces and deduplicating downstream consumers are fully implemented and demonstrated in `member3_streaming/example_consumer_member4.py` and `MEMBER4_START_HERE.md`. However, confirmation from the downstream modeling team regarding final production sinks remains **unanswered as of 2026-10-18**.

---

## 6. Reviewer Checklist
- [ ] No files in `member2_spark/` or `member_audit1/` were modified.
- [ ] Git history is strictly forward-only; zero amends, rebases, or force pushes.
- [ ] Real Kafka and real PySpark queries used for all streaming verifications; no mocks.
- [ ] All output columns adhere to documented schemas (42 cols PaySim, 483 cols IEEE).
- [ ] Checkpoint crash recovery produces byte-identical duplicates deduplicated downstream.
- [ ] Raw execution logs saved in git tag `member3-evidence-archive` supporting every claim.
- [ ] Failure classification for Windows virtualenvs marked UNVERIFIED.
