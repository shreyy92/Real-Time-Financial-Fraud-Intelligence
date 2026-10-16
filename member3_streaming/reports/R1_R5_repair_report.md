# Audit Repairs R1–R5 Verification Report

## Summary
All 5 repair orders (R1 to R5) have been executed and verified against live real-time infrastructure (Real Kafka `127.0.0.1:9092` and PySpark 4.1.1 on Python 3.12). 

---

## R1. Git and Ledger Hygiene
- **Past Amend History Correction & Strict Zero-Amend Policy:**
  Correction of prior false claim: Any prior statement claiming 0 uses of `git commit --amend` was false. The true count is exactly 11 prior uses of `git commit --amend` documented in the local reflog (`git reflog show member3-streaming`):
  1. `0d22cbb member3-streaming@{0}: commit (amend): Chunk 8: IEEE stateless consumer and parity gate passed`
  2. `a7444c4 member3-streaming@{2}: commit (amend): Chunk 7: IEEE producer gate verification and key selection`
  3. `7caa3f3 member3-streaming@{3}: commit (amend): Chunk 7: IEEE producer gate verification and key selection`
  4. `5dbaff9 member3-streaming@{5}: commit (amend): Chunk 6: IEEE discovery analysis and architectural mapping`
  5. `417b0d4 member3-streaming@{6}: commit (amend): Chunk 6: IEEE discovery analysis and architectural mapping`
  6. `5e32ac4 member3-streaming@{8}: commit (amend): Chunk 5: Member 4 handoff and live example consumer validation`
  7. `7b6bfb8 member3-streaming@{9}: commit (amend): Chunk 5: Member 4 handoff and live example consumer validation`
  8. `e1331ce member3-streaming@{11}: commit (amend): Chunk 4R: Output Contract and Pytest Honesty Repairs with real Spark streaming tests`
  9. `0140926 member3-streaming@{12}: commit (amend): Chunk 4R: Output Contract and Pytest Honesty Repairs with real Spark streaming tests`
  10. `64dc552 member3-streaming@{14}: commit (amend): Chunk 2R: Real mid-batch crash recovery repair and RocksDB verification`
  11. `59d245f member3-streaming@{15}: commit (amend): Chunk 2R: Real mid-batch crash recovery repair and RocksDB verification`
  Going forward under strict Global Rules, `git commit --amend` is permanently prohibited. All future changes are strictly committed as distinct, forward-only commits.
- **Reachability in `PROGRESS.md`:** Fixed all commit hashes to reachable git log hashes:
  - 1R: `c894b76`
  - 2R: `64dc552`
  - 3: `folded into 2R.9 (commit 64dc552)`
  - 4R: `e1331ce`
  - 5: `5e32ac4`
  - 6: `5dbaff9`
  - 7: `a7444c4`
  - 8: `0d22cbb`
- **Chunk 3:** Set to `"folded into 2R.9 (commit 64dc552)"`. Written [C03_report.md](file:///e:/vs%20code/projects/data-science/Real-Time-Financial-Fraud-Intelligence/member3_streaming/reports/C03_report.md).
- **Remote Push History & Non-Fast-Forward Audit (`git merge-base --is-ancestor`):**
  An audit of all 21 consecutive `(old, new)` push transitions in `git reflog show origin/member3-streaming` reveals that **5 pushes rewrote remote history** (non-fast-forward updates):
  
  | Old Commit | New Commit | Exit Code | Is Ancestor? | Status / Note |
  |---|---|---|---|---|
  | `74a8fad` | `24dd0e5` | 0 | yes | Fast-forward |
  | `24dd0e5` | `19de5c3` | 0 | yes | Fast-forward |
  | `19de5c3` | `aeb27ea` | 0 | yes | Fast-forward |
  | `aeb27ea` | `3b425a5` | 0 | yes | Fast-forward |
  | `3b425a5` | `b012cde` | 0 | yes | Fast-forward |
  | `b012cde` | `144fb6b` | 0 | yes | Fast-forward |
  | `144fb6b` | `c742673` | 0 | yes | Fast-forward |
  | `c742673` | `05b8430` | 0 | yes | Fast-forward |
  | `05b8430` | `39a1a2f` | 0 | yes | Fast-forward |
  | `39a1a2f` | `4fff645` | 0 | yes | Fast-forward |
  | `4fff645` | `59d245f` | 0 | yes | Fast-forward |
  | `59d245f` | `64dc552` | 1 | **no** | **Non-FF push** (replaces `59d245f` with amended 2R `64dc552`) |
  | `64dc552` | `0140926` | 0 | yes | Fast-forward |
  | `0140926` | `e1331ce` | 1 | **no** | **Non-FF push** (`0140926` replaced by amended 4R `e1331ce`) |
  | `e1331ce` | `7b6bfb8` | 0 | yes | Fast-forward |
  | `7b6bfb8` | `5e32ac4` | 1 | **no** | **Non-FF push** (`7b6bfb8` replaced by amended 5 `5e32ac4`) |
  | `5e32ac4` | `417b0d4` | 0 | yes | Fast-forward |
  | `417b0d4` | `5dbaff9` | 1 | **no** | **Non-FF push** (`417b0d4` replaced by amended 6 `5dbaff9`) |
  | `5dbaff9` | `7caa3f3` | 0 | yes | Fast-forward |
  | `7caa3f3` | `a7444c4` | 1 | **no** | **Non-FF push** (`7caa3f3` replaced by amended 7 `a7444c4`) |
  | `a7444c4` | `0d22cbb` | 0 | yes | Fast-forward |

  **Exact Count of Non-Fast-Forward Pushes:** `5`
  
  **Historical Commits Once on Origin:** The commits `0140926`, `7b6bfb8`, `417b0d4`, and `7caa3f3` (as well as `59d245f`) were each once pushed to and live on `origin/member3-streaming` before being overwritten by subsequent force/amend pushes during historical iterations. Under current strict governance, all force pushes, rebases, and amends are permanently barred; all future commits are strictly additive forward pushes.
- **Draft Handling:** Chunk 9 files remain uncommitted drafts until Gate 9 passes.

---

## R2. Spark Ticket IDs
- Removed all occurrences of speculative ticket IDs (`SPARK-57438`, `SPARK-55236`, `SPARK-55271`) across the codebase.
- Replaced with the factual statement:
  `"Spark 4.1.1 NullPointerException in KafkaMicroBatchStream.metrics when replaying an uncommitted batch (upstream ticket not verified)"` across:
  - `member3_streaming/MEMBER3_OUTPUT_CONTRACT.md`
  - `member3_streaming/README.md`
  - `member3_streaming/HANDOFF_MEMBER4.md`
  - `member3_streaming/reports/C05_report.md`
  - `member3_streaming/run_verified_crash_test.py`
  - `member3_streaming/run_span_crash_test.py`
- Apache JIRA search confirmed no public ticket verified for this specific subcomponent stack trace in Spark 4.1.1.

---

## R3. Provider-Independence Verification
- **R3.a (Stateless Control):**
  - A minimal stateless Kafka-to-Kafka passthrough streaming query was launched with `maxOffsetsPerTrigger=500`.
  - Process killed mid-batch (count = 750, remainder $750 \pmod{500} = 250 \ne 0$).
  - Offset was left uncommitted in `commits/` while present in `offsets/1`.
  - Query was restarted: **Restarted cleanly with 0 exceptions and reached 1,000 records.**
  - **Verdict:** Stateless streaming queries DO NOT reproduce the NPE.
- **R3.b (HDFS StateStore Provider Replication):**
  - Configured `spark.sql.streaming.stateStore.providerClass = org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider`.
  - Executed stateful `applyInPandasWithState` query on 1,000 PaySim transactions.
  - Verified `HDFSBackedStateStoreProvider` was written into `offsets/0`.
  - Killed Consumer 1 mid-batch (count = 728, remainder $728 \pmod{500} = 228 \ne 0$).
  - Restarted Consumer 2 on the uncommitted batch:
    **Reproduced the exact same `java.lang.NullPointerException` in `org.apache.spark.sql.kafka010.KafkaMicroBatchStream$.metrics`!**
  - **Verdict:** The crash-recovery bug is provider-independent (affects both RocksDB and HDFS state stores).
- **R3.c & R3.d:** Contract updated, `PAYSIM_TRICKLE_SLEEP` verified off/0 by default in production.

---

## R4. Test Suite Repairs & Pass
- **R4.a:** `test_contract_dlq_codes_match_code_constants` updated to assert strict bidirectional equality (`set(DLQ_REASON_CODES.keys()) == set(codes)`).
- **R4.b:** `test_real_streaming_all_dlq_codes` verified with real consumer and all 9 reason codes.
- **R4.c:** `test_real_streaming_late_event_pending` verified:
  - Step 3 committed -> Step 5 pending -> Step 4 rejected as `LATE_EVENT` -> Second Step 5 accepted as tie -> Step 6 commits Step 5.
- **R4.d:** `test_real_checkpoint_recovery` verified: 40 unique transactions, exactly 0 duplicate mismatches.
- **R4.e:** `test_real_streaming_long_span_parity` verified: 20 cards with 24h/168h window eviction, 0 mismatches across 13 stateful columns.
- **R4.f:** `step == 9999` removed as redundant with `step > 743`.
- **Full Test Suite Execution (`pytest -v --durations=0 member3_streaming/tests`):**
  - `test_contract.py` (4 tests): PASSED
  - `test_fast_unit.py` (4 tests): PASSED
  - `test_real_streaming.py` (6 tests): PASSED
  - **Result: 14 passed in 330.32s (100% PASS rate).**

---

## R5. Chunk 8 Re-Verification (20,000 Rows)
- **Gate 8.1:** Real streaming query (`readStream.format('kafka')`) on real Kafka broker `127.0.0.1:9092`.
- **Gate 8.2 & 8.4:** Parity on **20,000 transactions** against batch parquet ground truth (`data/features/ieee_features.parquet`) at strict absolute tolerance of **1e-6**:
  - Total columns compared: **471**
  - Total rows compared per column: **20,000**
  - Total value mismatches: **0**
  - Total null mismatches: **0**
- **Gate 8.3:** Hash column equality:
  - `card_proxy_id` mismatches: **0**
  - `card_addr_proxy_id` mismatches: **0**
- **Gate 8.5 (DLQ Audit):** Exactly 1 DLQ record for each of the 6 DLQ reason codes (`MALFORMED_JSON`, `MISSING_FIELD`, `BAD_FIELD_TYPE`, `NEGATIVE_AMOUNT`, `INVALID_LABEL`, `INVALID_TRANSACTIONDT`). Valid rows completely unaffected.
- **Gate 8.6 (Count Reconciliation):**
  - Total columns in `ieee_features.parquet`: **483**
  - Minus stateful columns: **11**
  - Stateless columns in schema: **472**
  - Primary join key column: `TransactionID` (**1**)
  - Stateless feature columns compared in table: **471**
  - 483 - 11 - 1 = 471. All counts fully reconciled.
