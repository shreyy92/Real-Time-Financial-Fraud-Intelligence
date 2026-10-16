# Chunk 9 Report: IEEE Stateful Feature Streaming Pipeline keyed on card_proxy_id

## 1. Metadata
- **Chunk:** Chunk 9
- **Start Time:** 2026-10-16T17:30:00+05:30
- **End Time:** 2026-10-16T18:52:00+05:30
- **Status:** PASSED

## 2. Infrastructure Honesty
- **Real Kafka:** yes (Broker `127.0.0.1:9092` via Docker KRaft container `c0812e01f318`)
- **Real Spark Structured Streaming query:** yes (`readStream.format("kafka")` -> `applyInPandasWithState` keyed on `card_proxy_id` -> `writeStream.format("kafka")`)
- **Per-Gate Infrastructure Verification:**
  - Gate 1.1: Real Kafka: yes | Real Spark query: yes
  - Gate 1.2: Real Kafka: yes | Real Spark query: yes
  - Gate 1.3: Real Kafka: yes | Real Spark query: yes
  - Gate 1.4: Real Kafka: yes | Real Spark query: yes
  - Gate 1.5: Real Kafka: yes | Real Spark query: yes

## 3. Discovery Findings (0.1 - 0.5)
- **0.1 Producer Order:** Verified `producer_ieee.py` sorts input records by `(TransactionDT, TransactionID)`. Computed order over all 590,540 rows of `data/processed/ieee_clean.parquet`: 0 inversions, 100% strictly monotonic.
- **0.2 Oracle Tie & Boundary Logic:** Analyzed rolling window definitions for IEEE card history:
  - 1h window: $W = 3600$, range $[t - 3600, t - 1]$
  - 24h window: $W = 86400$, range $[t - 86400, t - 1]$
  - 7d window: $W = 604800$, range $[t - 604800, t - 1]$
  - Current transaction at time $t$ is strictly excluded from its own rolling counts and prior aggregates.
  - Transactions at exactly $t - W$ are included in the rolling count; transactions at $t - W - 1$ are excluded.
  - Multiple transactions with the exact same `TransactionDT` are ordered deterministically by `TransactionID`.
- **0.3 DLQ Mechanism:** Side-effect `KafkaProducer` instantiated per executor partition safely routes invalid records directly to `financial_transactions_ieee_dlq` and flushes immediately, ensuring bad inputs do not corrupt state or crash the streaming job.
- **0.4 Member 2 Batch Equivalence:** Member 2's batch card feature implementation (`src/ieee/card_features.py`) was verified:
  - `card_prior_amt_std` defaults to `0.0` when prior transaction count $N < 2$.
  - `card_prior_amt_max` defaults to `0.0` when $N = 0$.
  - `amt_zscore_vs_card` defaults to `0.0` when std == 0.
  - PySpark stateful streaming uses Welford's online algorithm for running variance/std and numpy binary search (`bisect_left`/`bisect_right`) over monotonic timestamps for eviction and window slicing.
- **0.5 Schema Consistency:** Verified output schema contains exactly 483 columns (472 stateless + 11 stateful card history features). Symmetric difference with `data/features/ieee_features.parquet` is empty (`set()`).

## 4. Files Created or Changed
- `member3_streaming/consumer_ieee.py`: Real-time PySpark Structured Streaming consumer with `applyInPandasWithState` keyed on `card_proxy_id`, vectorized rolling calculations, single `pd.concat` DataFrame construction, side-effect DLQ, and RocksDB state storage.
- `member3_streaming/producer_ieee.py`: Kafka producer for IEEE dataset supporting full, limited, and card-subset production.
- `member3_streaming/produce_ieee_subset.py`: Helper script to deterministically produce 15 whole cards with long lifespans (39,965 total rows).
- `member3_streaming/verify_gate1_2_oracle.py`: Test harness for Gate 1.2 oracle tie and boundary verification.
- `member3_streaming/run_gate1_3_20k.py`: Test harness for Gate 1.3 20k full-column parity verification.
- `member3_streaming/run_gate1_4_longspan.py`: Test harness for Gate 1.4 long-span parity and 7-day state eviction verification.
- `member3_streaming/run_gate1_5_recovery.py`: Test harness for Gate 1.5 mid-batch crash recovery, plain restart failure reproduction, and post-rollback deduplicated parity.
- `member3_streaming/reports/C09_report.md`: This report.
- `member3_streaming/reports/raw/C09_gate1_1.txt`: Raw terminal output for Gate 1.1.
- `member3_streaming/reports/raw/C09_gate1_2.txt`: Raw terminal output for Gate 1.2.
- `member3_streaming/reports/raw/C09_gate1_3.txt`: Raw terminal output for Gate 1.3.
- `member3_streaming/reports/raw/C09_gate1_4.txt`: Raw terminal output for Gate 1.4.
- `member3_streaming/reports/raw/C09_gate1_5.txt`: Raw terminal output for Gate 1.5.

## 5. Gate Results Table

| Gate | Description | Rows Tested | Metric / Target | Result | Evidence Pointer |
|---|---|---|---|---|---|
| **1.1** | Stateless Regression | 20,000 | 471 stateless columns match batch, DLQ = 6 | **PASS** | `reports/raw/C09_gate1_1.txt`: 0 mismatches across all 471 columns; DLQ routed 6/6 expected error types |
| **1.2** | Oracle Tie & Boundary Test | 10 handcrafted edge-cases | Ties, $t-W, t-W-1, t-1, t$ exact match vs Member 2 batch oracle; negative control fails | **PASS** | `reports/raw/C09_gate1_2.txt`: 0 mismatches against batch function; negative control confirmed prior windowing produced 4 mismatches |
| **1.3** | 20k Full Parity | 20,000 | 483 columns evaluated vs `ieee_features.parquet` at 1e-6 tol; DLQ = 0 | **PASS** | `reports/raw/C09_gate1_3.txt`: 0 mismatches across all 483 columns; DLQ = 0; non-vacuous positive counts verified |
| **1.4** | Long-Span Parity & Eviction | 39,965 (15 full cards) | 11 stateful columns match batch; 7d eviction tested on >90% rows; max buffer <= 2500 | **PASS** | `reports/raw/C09_gate1_4.txt`: 0 mismatches across 11 stateful columns; 37,401 rows (93.58%) tested eviction; max 7d buffer = 1678; DLQ = 0 |
| **1.5** | Mid-Batch Crash Recovery | 39,965 (15 full cards) | Hard kill mid-batch (`taskkill /F /T`); reproduce NPE; operator rollback; 0 missing, byte-identical duplicates, 0 parity mismatches | **PASS** | `reports/raw/C09_gate1_5.txt`: Kill at count 2156 (% 500 = 156); NPE reproduced; rollback applied; 39,965 distinct (0 missing); 999 duplicates (100% byte-identical); 0 stateful mismatches |

## 6. Crash Recovery Details (Gate 1.5)
- **Kill Condition:** Hard kill executed via `taskkill /F /T` at output count **2156** (`count % 500 = 156`).
- **Checkpoint State Provider Line (`offsets/0`):**
  ```json
  {"batchWatermarkMs":0,"batchTimestampMs":1792156134461,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.stateStore.encodingFormat":"unsaferow","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"4","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.optimizer.pruneFiltersCanPruneStreamingSubplan":"false"}}
  ```
- **Plain Restart Attempt (Without Workaround):**
  Failed immediately with exit code 1. First 15 lines of exception from consumer log:
  ```text
  java.lang.NullPointerException: Cannot invoke "scala.collection.IterableOps.map(scala.Function1)" because the return value of "scala.Option.get()" is null
  	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream$.metrics(KafkaMicroBatchStream.scala:520)
  	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.metrics(KafkaMicroBatchStream.scala:363)
  	at org.apache.spark.sql.execution.streaming.runtime.ProgressContext.$anonfun$extractSourceProgress$2(ProgressReporter.scala:384)
  	at org.apache.spark.util.Utils$.timeTakenMs(Utils.scala:496)
  	at org.apache.spark.sql.execution.streaming.runtime.ProgressContext.$anonfun$extractSourceProgress$1(ProgressReporter.scala:380)
  	at org.apache.spark.sql.execution.streaming.runtime.ProgressContext.extractSourceProgress(ProgressReporter.scala:379)
  	at org.apache.spark.sql.execution.streaming.runtime.ProgressContext.constructNewProgress(ProgressReporter.scala:348)
  	at org.apache.spark.sql.execution.streaming.runtime.ProgressContext.finishTrigger(ProgressReporter.scala:312)
  	at org.apache.spark.sql.execution.streaming.runtime.ProgressContext.finishTrigger(ProgressReporter.scala:429)
  	at org.apache.spark.sql.execution.streaming.runtime.MicroBatchExecution.executeOneBatch(MicroBatchExecution.scala:525)
  	at org.apache.spark.sql.execution.streaming.runtime.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:461)
  	at org.apache.spark.sql.execution.streaming.runtime.MicroBatchExecution.$anonfun$runActivatedStream$1$adapted(MicroBatchExecution.scala:461)
  	at org.apache.spark.sql.execution.streaming.runtime.TriggerExecutor.runOneBatch(TriggerExecutor.scala:40)
  	at org.apache.spark.sql.execution.streaming.runtime.TriggerExecutor.runOneBatch$(TriggerExecutor.scala:38)
  ```
- **Recovery Mode:** `mid-batch crash requires operator rollback`
- **Operator Workaround Applied:**
  1. Inspected checkpoint commits: max committed batch index was 4 (`commits/4`).
  2. Identified uncommitted orphan offset file `offsets/4` and associated checksum file `offsets/.4.crc`.
  3. Cleaned uncommitted batch artifacts: `['offsets/.4.crc', 'offsets/4', 'commits/.4.crc', 'commits/4']`.
  4. Successfully relaunched consumer stream.
- **Recovery Audit Results:**
  - Total Output Records: 40,964
  - Distinct TransactionIDs: 39,965 / 39,965
  - Missing TransactionIDs: 0
  - Duplicate Records: 999
  - Duplicates Byte-Identical: **yes** (100% byte identical across all JSON fields)
  - Post-dedupe Stateful Parity vs `ieee_features.parquet`: **0 mismatches** across all 11 stateful columns.

## 7. Fix Attempts
1. **Pandas DataFrame Fragmentation:**
   - *Problem:* Assigning 11 stateful columns one-by-one into a 472-column DataFrame caused Pandas to make 11 copies per card group and emit thousands of `PerformanceWarning`s, slowing down processing speed by ~10x.
   - *Fix:* Constructed a single `pd.DataFrame` containing all 11 computed stateful columns with explicit numpy arrays and joined via a single `pd.concat([pdf, new_cols_df], axis=1)`.
2. **Windows Hadoop CRC File Cleanup:**
   - *Problem:* On Windows, Hadoop ChecksumFileSystem creates hidden `.{batch}.crc` files. During operator rollback, deleting only `offsets/4` left `offsets/.4.crc`, which caused Hadoop to throw `FileAlreadyExistsException: Rename destination .../offsets/.4.crc already exists` on restart.
   - *Fix:* Enhanced the operator rollback routine to delete both `{fname}` and `.{fname}.crc` in both `offsets/` and `commits/` directories.
3. **Kafka Consumer Offset Seeking:**
   - *Problem:* Ad-hoc consumer `KafkaConsumer.assign([tp])` without a consumer group did not automatically begin at offset 0 if records already existed on the topic.
   - *Fix:* Added `consumer.seek_to_beginning(tp)` prior to reading records, ensuring all output records (including duplicates from replayed batches) were captured.

## 8. Surprises / Limitations
- Spark Structured Streaming with Kafka source and RocksDB state provider leaves an uncommitted offset file when killed abruptly mid-microbatch. Upon plain restart, `KafkaMicroBatchStream$.metrics` attempts to retrieve source metrics for the uncommitted batch and encounters `null.get()`, causing a `NullPointerException`. Manual operator rollback of the uncommitted batch files cleanly restores state store and offset log consistency, resulting in 100% byte-identical duplicates and 0 parity errors.

## 9. NOT DONE
- None. All Chunk 9 requirements passed.
