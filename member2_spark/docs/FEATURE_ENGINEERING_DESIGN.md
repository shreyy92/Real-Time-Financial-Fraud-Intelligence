# Feature Engineering System Design — Member 2

This document details the architectural design, PySpark execution strategy, and optimization principles for **Member 2 (Big Data / PySpark Feature Engineering)** in the Real-Time Financial Fraud Intelligence Platform.

---

## 1. Overall System Architecture

```text
Member 1 Clean Parquet
   (paysim_clean.parquet & ieee_clean.parquet)
                    │
                    ▼
┌────────────────────────────────────────────────────────┐
│         MEMBER 2: PYSPARK DISTRIBUTED ENGINE           │
├────────────────────────────────────────────────────────┤
│ 1. Schema Validation & Input Ingestion                 │
│ 2. Deterministic Key Minting & Ordering Setup          │
│ 3. Row-Wise Feature Generation (Narrow Stage, 0 Shuffle)│
│    - Amount log transforms, ratios, zero flags         │
│    - Cyclical temporal sin/cos & relative diurnal proxy│
│    - Structured missingness indicators & completeness  │
│ 4. Time-Based Split Assignment (Anti-Drift Cut Points) │
│ 5. Distributed Behavioral Windows (Wide Stage, 1 Exch) │
│    - PaySim: Destination historical & velocity windows │
│    - IEEE: Card Proxy historical & velocity windows    │
│ 6. Automated Quality, Grain, and Leakage Audits        │
│ 7. Output Partitioning & Parquet Serialization         │
└────────────────────────────────────────────────────────┘
                    │
                    ▼
       ML-Ready Feature Datasets (v1.0)
     (PaySim Features & IEEE-CIS Features)
                    │
                    ▼
            Member 4 (Machine Learning)
            Member 3 (Kafka + Streaming)
```

---

## 2. PaySim Feature Pipeline Design

### 2.1 Domain Insights & Constraints (Member 1 Audit Findings)
- **Origin Account Disposability (Audit P1):** 99.85% of origin accounts appear exactly once. Sender-side histories or velocities are degenerate.
- **Destination Inflow Signal (Audit P2):** 2.72M recipient accounts receive up to 113 transactions. Recipient fan-in and first-seen novelty strongly correlate with mule accounts and cashing-out hubs.
- **Simulator Balance Arithmetic Artifact (Audit P4):** 97.6% of fraud rows exhibit `amount == oldbalanceOrg > 0` (drain the account), compared to 0.0% of legitimate rows. This simulator artifact causes artificial class separability and is strictly isolated into **Tier B**.
- **Constant Fraud Volume per Step (Audit P5):** Fraud counts stay roughly constant per step (216–320/day) regardless of volume. Per-step volume features act as direct label leakage and are strictly prohibited.
- **Surrogate Key & Tie-Breaker (Audit P11):** PaySim lacks a transaction ID and has 1-hour step resolution. A deterministic SHA-256 hash across the 5-tuple `(step, type, amount, nameOrig, nameDest)` provides a surrogate key.

### 2.2 PaySim Processing Stages
1. **Load & Validate:** Ingest `paysim_clean.parquet` and verify the 10 required columns.
2. **Temporal & Key Prep:** Mint `txn_id`, derive `step_hour = step % 24`, cyclical `hour_sin/cos`, `sim_day = (step - 1) // 24`, and binary indicators for `type`.
3. **Time Split:** Assign `split` using step thresholds (Steps 1–500: Train, 501–600: Valid, 601–743: Test).
4. **Amount & Balance Features:**
   - **Tier A:** `amount_log1p`, `amount_zero_flag`, `amt_to_oldbalOrg`, `amt_to_oldbalDest`, `orig_drain_flag`, `orig_balance_zero_flag`, `dest_balance_zero_flag`.
   - **Tier B:** `err_balance_orig`, `err_balance_dest`, `orig_new_balance_zero_flag`, raw post-balances.
5. **Destination Historical Windows:** Partition by `nameDest`, order by `step`, frame `rangeBetween(Window.unboundedPreceding, -1)`. Computes prior transaction count, first-seen flag, cumulative sum, mean, std, z-score, and dormancy.
6. **Destination Velocity Windows:** Partition by `nameDest`, order by `step`, frames `rangeBetween(-24, -1)` and `rangeBetween(-168, -1)`. Computes 24h and 168h counts and sums.
7. **Quality & Grain Check:** Assert `final_count == initial_count` and `txn_id` distinctness.

---

## 3. IEEE-CIS Feature Pipeline Design

### 3.1 Domain Insights & Constraints (Member 1 Audit Findings)
- **Structured Missingness (Audit Part 4.4):** 70 identical-null-mask groups exist. Dropping nulls (`dropna`) eliminates 100% of data. Block-level indicators and identity completeness capture the missingness signal without feature explosion.
- **Identity Record Signal:** 24.42% of transactions contain identity records; these rows exhibit a 7.85% fraud rate vs. 2.09% for rows without identity.
- **Lack of Customer ID & Card Proxy (Audit B3, B6):** IEEE-CIS lacks direct customer IDs. A card proxy (`card_proxy_id`) is formed by hashing `card1` through `card6` with missing values coalesced to 'NA'. A second proxy (`card_addr_proxy_id`) incorporates `addr1`.
- **Relative Timedelta (Audit B2):** `TransactionDT` represents seconds from an arbitrary origin. Diurnal features (`hour_of_day = (DT // 3600) % 24`) and relative 7-day cyclical weekdays are extracted.

### 3.2 IEEE-CIS Processing Stages
1. **Load & Validate:** Ingest `ieee_clean.parquet` and verify presence of key identifiers and transaction attributes.
2. **Missingness Preservation:** Derive `has_identity_flag`, `identity_nonnull_count`, `identity_completeness`, and mask block indicators (`ind_addr`, `ind_dist1`, `ind_dist2`, `ind_Pemail`, `ind_Remail`, `ind_D*`, `ind_V*`).
3. **Card Proxy Entity:** Compute deterministic `card_proxy_id` and `card_addr_proxy_id`.
4. **Relative Temporal Features:** Compute `hour_of_day`, `hour_sin`, `hour_cos`, `day_index`, `day_of_week_rel`, and 7-day warmup indicator.
5. **Time Split:** Assign `split` at Day 130.0 (`TransactionDT <= 130 * 86400 -> train`, otherwise `valid`), preserving stable ~3.5% fraud prevalence across splits.
6. **Amount & Masked Aggregates:** Compute `amt_log1p`, `amt_cents`, `amt_decimals_gt2_flag`, `amt_is_whole_flag`, and summary aggregates over M, C, and D families.
7. **Card Proxy Historical Windows:** Partition by `card_proxy_id`, order by `(TransactionDT, TransactionID)`, frame `rowsBetween(Window.unboundedPreceding, -1)`. Computes prior transaction count, first-seen flag, spending mean/std/z-score, and velocity windows (1h, 24h, 7d).
8. **Pruning & Grain Check:** Drop the 14 uninformative near-constant columns (`V1`, `V14`, etc.), assert row-grain preservation, and write output.

---

## 4. PySpark Distributed Optimization Considerations

### 4.1 Zero Python UDFs
All row-wise transformations, cyclical trigonometry, bitwise flags, and string parsing use native PySpark SQL functions (`pyspark.sql.functions`). Built-in expressions execute entirely inside the JVM via Catalyst optimization and avoid costly Python-JVM serialization roundtrips.

### 4.2 Shuffle Minimization & Window Coalescing
- All destination window functions for PaySim share the exact same partition key (`nameDest`) and order key (`step`). Spark Catalyst plans a **single shuffle exchange** for all destination historical and velocity features.
- All card proxy window functions for IEEE share the exact same partition key (`card_proxy_id`). Spark executes all expanding and rolling calculations in one shuffle sort.

### 4.3 Partitioning Strategy
- **Shuffle Partitions:** `spark.sql.shuffle.partitions` is set to 16 for local laptop execution (preventing hundreds of tiny tasks on 6.36M rows) and scales to 64–128 in cluster deployments.
- **Output Files:**
  - PaySim: Re-partitioned to 8 files (target file size ~30–50 MB Snappy Parquet).
  - IEEE-CIS: Coalesced to 4 files (target file size ~20–30 MB Snappy Parquet).
- **Anti-Pattern Prohibited:** Never partition Parquet output by high-cardinality keys (`nameDest`, `card_proxy_id`) or fine-grained time steps (`step`), which would generate thousands of tiny, fragmented directories.

### 4.4 Memory and Caching Rules
- DataFrames are not cached blindly. Transformations are evaluated lazily.
- In multi-action workflows, caching is restricted to post-feature DataFrames immediately prior to validation action and Parquet write, and unpersisted promptly.
