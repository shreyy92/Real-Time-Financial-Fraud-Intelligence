# Member 2 Final Verification Report

**Project:** Real-Time Financial Fraud Intelligence Platform  
**Component:** Member 2 — Distributed PySpark Feature Engineering & Data Processing  
**Verification Date:** 2026-09-20  
**Verification Scope:** End-to-end execution on real datasets, comprehensive test suite pass, zero-leakage enforcement, row-grain preservation, and downstream ML handoff verification for Member 4.

---

## 1. Environment

- **Operating System:** Windows 11 Home (x64)
- **Python Version:** 3.12.8 (`C:\Users\hp\venv\Scripts\python.exe`)
- **PySpark Version:** 4.2.0 (Apache Spark 4.2.0 with Hadoop 3.5.0 client libraries)
- **Java Virtual Machine:** OpenJDK 21.0.8 (64-Bit Server VM, Eclipse Adoptium / Oracle Corporation)
- **Windows Hadoop Native Layer:** WinUtils 3.3.6 and `hadoop.dll` (x64) installed at `C:\hadoop\bin` and bound via `HADOOP_HOME = C:\hadoop` and `PATH`.
- **Primary Dependencies:** `pyarrow==25.0.1`, `pytest==9.1.1`, `pyyaml==6.0.3`

---

## 2. Unit Tests

All unit tests were executed against an isolated local Spark session.

- **Total Tests:** 15
- **Passed Tests:** 15
- **Failed Tests:** 0
- **Skipped / Errored:** 0
- **Execution Time:** 48.54 seconds
- **Test Modules:**
  - `member2_spark/tests/test_spark_session.py`: 2 passed (session lifecycle, configuration overrides, alias)
  - `member2_spark/tests/test_paysim_features.py`: 5 passed (temporal features, Tier A vs B balance arithmetic, strictly historical destination windowing, shuffled input determinism, end-to-end pipeline)
  - `member2_spark/tests/test_ieee_features.py`: 5 passed (missingness preservation without row drops, card proxy derivation, cyclical timestamps, strictly historical card history, end-to-end pipeline)
  - `member2_spark/tests/test_validation.py`: 3 passed (schema validation, duplicate identifier and null detection, leakage audit and target exclusion)

---

## 3. PaySim Real Dataset

- **Input Parquet File:** `data/processed/paysim_clean.parquet`
- **Output Parquet Directory:** `data/features/paysim_features.parquet`
- **Input Rows:** 6,362,620
- **Output Rows:** 6,362,620 (Exact 1:1 row grain preserved; 0 dropped, 0 duplicated)
- **Input Columns:** 12 (`step`, `type`, `amount`, `nameOrig`, `oldbalanceOrg`, `newbalanceOrig`, `nameDest`, `oldbalanceDest`, `newbalanceDest`, `isFraud`, `isFlaggedFraud`, `step_hour`)
- **Output Columns:** 48
- **Feature Count (excluding metadata/identifiers/label):** 39 features (34 Tier A features + 5 Tier B features)
- **Tier A Feature Count:** 34 (Authorization-time features: cyclical hours, transfer/cashout one-hots, amount ratios, strictly prior destination transaction counts, first-seen flags, prior amount sums, prior amount means/stds/max, prior steps elapsed, destination Z-scores, 24h rolling velocity, 168h rolling velocity, prior cashout share)
- **Tier B Feature Count:** 5 (`newbalanceOrig`, `newbalanceDest`, `err_balance_orig`, `err_balance_dest`, `orig_new_balance_zero_flag`)
- **Null Audit:**
  - `txn_id`: 0 nulls (100% complete deterministic SHA-256 surrogate primary keys)
  - `isFraud`: 0 nulls (100% complete binary label)
  - `split`: 0 nulls (100% complete time-based split assignment)
  - Feature-level nulls strictly confined to legitimate cold-start instances (e.g., `dest_amount_zscore` when prior count < 3, `dest_steps_since_last_txn` for first-seen destinations).
- **Duplicate Identifier Audit:** 0 duplicate `txn_id` values across all 6,362,620 rows.
- **Leakage Result:** PASS. Zero target leakage. Destination behavioral windows enforce `rangeBetween(Window.unboundedPreceding, -1)` on `step`, strictly excluding current and future transactions. Simulator per-step total traffic features are completely omitted.
- **Time-Based Split Distribution:**
  - **Train (Steps 1–500):** 6,061,807 rows | 5,561 frauds (0.0917% fraud prevalence)
  - **Validation (Steps 501–600):** 197,240 rows | 1,052 frauds (0.5334% fraud prevalence)
  - **Test (Steps 601–743):** 103,573 rows | 1,600 frauds (1.5448% fraud prevalence)
  - **Total Ground-Truth Frauds:** 8,213 (exact match to Member 1 clean dataset)
- **Execution Runtime:** 47.33 seconds (end-to-end distributed transformation and Snappy Parquet write)
- **Output Storage:** 8 Snappy Parquet partitions, total size ~1.15 GB.

---

## 4. IEEE Real Dataset

- **Input Parquet File:** `data/processed/ieee_clean.parquet`
- **Output Parquet Directory:** `data/features/ieee_features.parquet`
- **Input Rows:** 590,540
- **Output Rows:** 590,540 (Exact 1:1 row grain preserved; 0 dropped, 0 duplicated)
- **Input Columns:** 434 (Cleaned combined transaction + identity parquet from Member 1)
- **Output Columns:** 483
- **Feature Count:** 479 candidate features + 4 primary/metadata columns (`TransactionID`, `TransactionDT`, `isFraud`, `split`, `feature_version`)
- **Dropped Low-Information Near-Constants:** 14 columns dropped per Member 1 Audit Part 4.2 & 9.2 (`V1`, `V14`, `V27`, `V28`, `V41`, `V65`, `V68`, `V88`, `V89`, `V107`, `V240`, `V241`, `V305`, `id_27`)
- **Null Audit:**
  - `TransactionID`: 0 nulls (100% unique primary key)
  - `isFraud`: 0 nulls (100% complete binary label)
  - `split`: 0 nulls (100% complete time-based split assignment)
  - Missingness patterns preserved across raw V, C, D, M, and id columns without destructive imputation or row drops.
- **Duplicate Identifier Audit:** 0 duplicate `TransactionID` values across all 590,540 rows.
- **Leakage Result:** PASS. Candidate model features contain 0 target leakage features, 0 prohibited target-derived statistics, and 11 review columns flagged as reference keys or high-null masked columns (`TransactionID`, `TransactionDT`, `card_proxy_id`, `card_addr_proxy_id`, `D8`, `D9`, `id_02`, `id_07`, `id_08`, `id_22`, `id_26`). Card history aggregations use strictly past transactions: `rangeBetween(Window.unboundedPreceding, -1)` on `TransactionDT`.
- **Time-Based Split Distribution:**
  - **Train (Days 1–130 | TransactionDT <= 11,232,000s):** 442,366 rows | 15,535 frauds (3.5118% fraud prevalence)
  - **Validation (Days 131–183 | TransactionDT > 11,232,000s):** 148,174 rows | 5,128 frauds (3.4608% fraud prevalence)
  - **Total Ground-Truth Frauds:** 20,663 (exact match to Member 1 clean dataset)
- **Execution Runtime:** 56.94 seconds (end-to-end distributed transformation and Snappy Parquet write)
- **Output Storage:** 4 Snappy Parquet partitions, total size ~103 MB.

---

## 5. Code Corrections

The following optimizations and corrections were applied during the verification process:

1. **Windows NativeIO Hadoop Compatibility (`member2_spark/src/spark_session.py`):**
   - Installed native Windows 64-bit Hadoop binaries (`winutils.exe` and `hadoop.dll`) into `C:\hadoop\bin`.
   - Updated `src/spark_session.py` to auto-detect `C:\hadoop` and bind `HADOOP_HOME` and `PATH`, preventing JVM `UnsatisfiedLinkError: NativeIO$Windows.access0` when reading or writing local Parquet files.
2. **Feature Quality Aggregation Optimization (`member2_spark/src/validation/feature_quality.py`):**
   - Refactored null and empty column validation from sequential Python per-column query loops into a unified, single-pass distributed aggregation query (`df.agg()`).
   - Reduced query execution jobs on IEEE-CIS (483 columns) from over 450 sequential Spark stages to 1 stage.
3. **Parquet Write and Row-Grain Verification Optimization (`src/paysim/build_features.py` & `src/ieee/build_features.py`):**
   - Updated pipeline execution to write output to Parquet and verify the row count from the persisted dataset.
   - Eliminated redundant shuffle evaluations of the window DAG across 6.36M rows and ensured subsequent validation steps operate directly on the persisted Parquet on disk.
4. **Candidate Feature Isolation in CLI Runner (`member2_spark/run_pipeline.py`):**
   - Updated `run_pipeline.py` to pass candidate model features (explicitly excluding the ground-truth supervised target `isFraud` and dataset partition metadata `split`) to `audit_feature_leakage()`, ensuring proper reporting of feature leakage versus ground-truth label presence.

---

## 6. Leakage Verification

### Target Leakage
- `isFraud` is retained in the final dataset strictly as a supervised ground-truth label for Member 4.
- Zero features are calculated from `isFraud`. Prohibited aggregations (e.g., historical fraud rate per account, fraud count per hour, target encoding) are completely absent from the codebase. The `audit_feature_leakage()` validator automatically rejects any column matching target keywords.

### Future Leakage
- No global dataset-wide aggregations (e.g., whole-dataset mean amount, global max step) are computed.
- All temporal aggregations are strictly partitioned by entity and ordered by time.
- Time-based splits are determined strictly by chronological horizons:
  - PaySim: Steps 1–500 (Train), 501–600 (Validation), 601–743 (Test).
  - IEEE-CIS: Days 1–130 (Train), Days 131–183 (Validation).
- Future data is never consulted when computing metrics for earlier transactions.

### Current-Row Self-Inclusion Leakage
- All Spark Window specifications for historical behavior enforce strict exclusion of the current record:
  - PaySim destination history: `.rangeBetween(Window.unboundedPreceding, -1)` on `step`.
  - PaySim velocity windows: `.rangeBetween(-24, -1)` and `.rangeBetween(-168, -1)` on `step`.
  - IEEE card history: `.rangeBetween(Window.unboundedPreceding, -1)` on `TransactionDT`.
- The current transaction's amount, timestamp, or type is never counted in its own prior history.

### PaySim Step-Level Leakage Control
- In compliance with Member 1 Audit P5, no global per-step transaction counts or simulator traffic volumes are engineered. In PaySim, hourly transaction volume directly reflects simulator scheduling and acts as an artificial target leak; Member 2 strictly derives velocity only on a per-destination account basis (`dest_txn_count_prev_24h`, `dest_txn_count_prev_168h`).

### Tier B Handling (Post-Transaction Balances)
- The 5 post-transaction features (`newbalanceOrig`, `newbalanceDest`, `err_balance_orig`, `err_balance_dest`, `orig_new_balance_zero_flag`) are clearly isolated.
- In `feature_config.yaml`, `include_tier_b: true` is configurable.
- Downstream Member 4 documentation explicitly advises evaluating models both with and without Tier B features to ensure realistic pre-authorization production performance.

---

## 7. Performance

Actual physical execution measurements on local workstation (Intel Core / 16GB RAM):

| Pipeline | Total Rows | Total Columns | Parquet Partitions | Disk Size | Processing & Write Time | Memory Footprint |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **PaySim Feature Pipeline** | 6,362,620 | 48 | 8 Snappy files | 1.15 GB | **47.33 s** | 4GB Driver, 16 Shuffles |
| **IEEE-CIS Feature Pipeline** | 590,540 | 483 | 4 Snappy files | 103 MB | **56.94 s** | 4GB Driver, 16 Shuffles |
| **Unit Test Suite (15 tests)**| Synthetic | Variable | In-memory | N/A | **48.54 s** | Local test harness |

Both pipelines complete in under 60 seconds each, demonstrating high scalability, zero Cartesian joins, and efficient PySpark Catalyst plan optimization.

---

## 8. Output Contract

Member 4 (Machine Learning Engineer) receives production-ready, validated feature datasets conforming to `member2_spark/docs/OUTPUT_CONTRACT.md`:

1. **PaySim Feature Dataset:**
   - **Path:** `data/features/paysim_features.parquet`
   - **Primary Key:** `txn_id` (deterministic SHA-256 string)
   - **Target Label:** `isFraud` (integer: 0 or 1)
   - **Split Column:** `split` (string: `'train'`, `'valid'`, `'test'`)
   - **Feature Matrix:** 34 Tier A features + 5 Tier B features.
2. **IEEE-CIS Feature Dataset:**
   - **Path:** `data/features/ieee_features.parquet`
   - **Primary Key:** `TransactionID` (integer)
   - **Target Label:** `isFraud` (integer: 0 or 1)
   - **Split Column:** `split` (string: `'train'`, `'valid'`)
   - **Feature Matrix:** 479 features including missingness flags, card proxy historical behaviors, temporal sinusoids, decimal fingerprints, and aggregated M/C/D blocks.
3. **Reference Keys to Exclude from ML Inputs:**
   - PaySim: `txn_id`, `nameOrig`, `nameDest`, `isFlaggedFraud`, `feature_version`
   - IEEE-CIS: `TransactionID`, `TransactionDT`, `card_proxy_id`, `card_addr_proxy_id`, `feature_version`

---

## 9. Remaining Limitations

1. **Local Disk Ingestion Mode:** Features are currently written to local filesystems (`data/features/`). In production cloud/cluster environments, paths should be pointed to HDFS (`hdfs://...`) or S3/GCS buckets (`s3a://...`, `gs://...`) in `feature_config.yaml`.
2. **High-Cardinality Proxy Entities:** In IEEE-CIS, `card_proxy_id` yields ~136,000 distinct card entities. While PySpark window partitioning handles this cleanly within 16 shuffle partitions, scaling to billions of rows would require salting or bucketing by card hash.
3. **Near-Zero Variance Pruning:** 14 near-constant columns were dropped from IEEE-CIS. If Member 4 determines specific tree-based algorithms require additional V-column reduction, further correlation-based feature selection can be performed in the ML layer.

---

## 10. Final Status

```text
READY_FOR_MEMBER_4
```
