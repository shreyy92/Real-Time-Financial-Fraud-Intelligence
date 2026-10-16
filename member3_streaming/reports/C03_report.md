# Chunk 3 Report: PaySim Long-Span Re-Verification (Folded into Chunk 2R.9)

- **Date**: 2026-10-13
- **Status**: **FOLDED INTO CHUNK 2R.9**
- **Commit Hash**: `64dc552`
- **Real Kafka**: yes
- **Real Spark Structured Streaming query**: yes

---

## 1. Context & Disposition
The original Phase 3 Chunk 3 test verified long-span destination keys (spanning > 168 hours) to exercise time-window eviction on `dest_txn_count_prev_24h` and `dest_txn_count_prev_168h`.

During the Chunk 2R crash recovery repair, the long-span test on 300 keys (3,979 transactions) was completely re-run from scratch under mid-batch kill conditions (`maxOffsetsPerTrigger=250`, kill count `1,638`, `1638 % 250 = 138 != 0`) and RocksDB recovery.

Per roadmap instructions, Chunk 3's verification evidence is officially folded into Chunk 2R.9 to maintain full continuity without redundant re-execution.

---

## 2. Evidence Pointer (from `reports/C02R_report.md`, Section 2R.9)
- **Script Executed**: `member3_streaming/execute_span_crash_recovery_2r.py` (commit `64dc552`)
- **Key Count**: 300 distinct destination accounts spanning > 168 hours
- **Total Transactions**: 3,979
- **Window Expiry Exercised**:
  - `dest_txn_count_prev_24h != dest_prior_txn_count`: 2,891 transactions (> 0)
  - `dest_txn_count_prev_168h != dest_prior_txn_count`: 1,344 transactions (> 0)
- **Stateful Parity**: 0 mismatches across all 13 stateful columns vs `data/features/paysim_features.parquet`.
- **Crash Recovery Proof**: Mid-batch kill at 1,638 transactions, recovered with 138 byte-identical duplicates and 0 missing transactions.
