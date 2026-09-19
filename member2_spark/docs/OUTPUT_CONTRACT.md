# Output Contract & Downstream Handoff Specification — Member 2

This document defines the formal data contract for the ML-ready feature datasets produced by **Member 2 (Big Data / PySpark Feature Engineering)**. It specifies exact schemas, grain guarantees, column semantics, and handoff protocols for **Member 4 (Machine Learning)** and **Member 3 (Kafka + Spark Structured Streaming)**.

---

## 1. Grain & Format Guarantees

| Property | PaySim Contract | IEEE-CIS Contract |
|---|---|---|
| **Data Format** | Apache Parquet (Snappy compression) | Apache Parquet (Snappy compression) |
| **Row Grain** | Exact 1 transaction = 1 feature row | Exact 1 transaction = 1 feature row |
| **Primary Key** | `txn_id` (deterministic SHA-256 hash) | `TransactionID` (integer) |
| **Target Label** | `isFraud` (integer, values ∈ {0, 1}) | `isFraud` (integer, values ∈ {0, 1}) |
| **Time Split** | `split` (values ∈ {'train', 'valid', 'test'}) | `split` (values ∈ {'train', 'valid'}) |
| **Version** | `feature_version` ('v1.0') | `feature_version` ('v1.0') |
| **Row Multiplier Check** | `count(txn_id) == countDistinct(txn_id)` | `count(TransactionID) == countDistinct(TransactionID)` |

---

## 2. PaySim Output Schema Contract

### 2.1 Keys, Target & Bookkeeping (Never Feed Directly as Features to ML)
- `txn_id` (string): Deterministic surrogate key (`sha2(concat_ws('|', step, type, amount, nameOrig, nameDest), 256)`).
- `step` (integer): Simulation hour (1–743).
- `nameOrig` (string): Sender account identifier (99.85% single-use).
- `nameDest` (string): Recipient account identifier.
- `isFraud` (integer): Ground truth binary label.
- `split` (string): Dataset split assignment (`train`: steps 1–500, `valid`: 501–600, `test`: 601–743).
- `sim_day` (integer): Simulation day index (`(step - 1) div 24`).
- `history_window_complete_flag` (tinyint): Warmup indicator (`step >= 169`).
- `feature_version` (string): Contract version identifier (`v1.0`).

### 2.2 Tier A Features (Standard ML Input Matrix)
- `amount` (double): Raw transaction amount.
- `amount_log1p` (double): Log-transformed amount.
- `amount_zero_flag` (tinyint): Binary flag for `amount == 0.0`.
- `oldbalanceOrg` (double): Sender pre-balance.
- `oldbalanceDest` (double): Recipient pre-balance (structurally 0 for merchants).
- `amt_to_oldbalOrg` (double): Amount to sender balance ratio (`amount / (oldbalanceOrg + 1)`).
- `amt_to_oldbalDest` (double): Amount to recipient balance ratio (`amount / (oldbalanceDest + 1)`).
- `orig_drain_flag` (tinyint): Flag indicating `amount == oldbalanceOrg > 0` (Simulator artifact).
- `amount_exceeds_orig_balance_flag` (tinyint): Flag indicating `amount > oldbalanceOrg`.
- `orig_balance_zero_flag` (tinyint): Flag for `oldbalanceOrg == 0.0`.
- `dest_balance_zero_flag` (tinyint): Flag for `oldbalanceDest == 0.0` for non-PAYMENT transactions.
- `step_hour` (integer): Diurnal hour (0–23).
- `hour_sin` (double), `hour_cos` (double): Cyclical hour encodings.
- `type_transfer` (tinyint): One-hot flag for `TRANSFER`.
- `type_cash_out` (tinyint): One-hot flag for `CASH_OUT`.
- `type_payment` (tinyint): One-hot flag for `PAYMENT`.
- `type_cash_in` (tinyint): One-hot flag for `CASH_IN`.
- `type_debit` (tinyint): One-hot flag for `DEBIT`.
- `dest_prior_txn_count` (integer): Total prior transactions received by `nameDest`.
- `dest_first_seen_flag` (tinyint): Novelty flag (`dest_prior_txn_count == 0`).
- `dest_prior_amount_sum` (double): Cumulative prior inflow amount.
- `dest_prior_amount_mean` (double): Historical mean amount received (null if count == 0).
- `dest_prior_amount_std` (double): Historical amount standard deviation (null if count < 2).
- `dest_prior_amount_max` (double): Historical peak transaction amount.
- `dest_amount_zscore` (double): Z-score of amount vs. destination history (null if count < 3).
- `dest_steps_since_last_txn` (integer): Hours since previous transaction received.
- `dest_txn_count_prev_24h` (integer): Rolling 24-hour transaction count on destination.
- `dest_txn_count_prev_168h` (integer): Rolling 7-day transaction count on destination.
- `dest_amount_sum_prev_24h` (double): Rolling 24-hour amount sum on destination.
- `dest_amount_sum_prev_168h` (double): Rolling 7-day amount sum on destination.
- `dest_prior_cashout_share` (double): Historical CASH_OUT proportion received.

### 2.3 Tier B Features (Post-Transaction State — For Ablation Only)
- `newbalanceOrig` (double): Sender post-balance.
- `newbalanceDest` (double): Recipient post-balance.
- `err_balance_orig` (double): Accounting error delta: `newbalanceOrig + amount - oldbalanceOrg`.
- `err_balance_dest` (double): Accounting error delta: `oldbalanceDest + amount - newbalanceDest`.
- `orig_new_balance_zero_flag` (tinyint): Indicator that sender balance was emptied.

---

## 3. IEEE-CIS Output Schema Contract

### 3.1 Keys, Target & Bookkeeping
- `TransactionID` (integer): Primary key.
- `isFraud` (integer): Binary target label.
- `TransactionDT` (integer): Timedelta in seconds.
- `split` (string): Split assignment (`train`: DT <= 130 days, `valid`: DT > 130 days).
- `card_proxy_id` (string): SHA-256 payment card instrument proxy.
- `card_addr_proxy_id` (string): SHA-256 payment card + billing region proxy.
- `day_index` (integer): Elapsed day index (`floor(DT / 86400)`).
- `history_window_complete_flag` (tinyint): Warmup indicator (`day_index >= 7`).
- `feature_version` (string): Contract version identifier (`v1.0`).

### 3.2 Raw Retained Features (Imputation Left to Member 4)
- Core: `TransactionAmt`, `ProductCD`, `card1`–`card6`, `addr1`, `addr2`, `dist1`, `dist2`, `P_emaildomain`, `R_emaildomain`.
- Masked Families: `C1`–`C14`, `D1`–`D7`, `D10`–`D15`, `M1`–`M9`.
- Vesta Masked: `V*` (excluding the 14 uninformative near-constants: `V1, V14, V27, V28, V41, V65, V68, V88, V89, V107, V240, V241, V305, id_27`).
- Identity: `id_01`, `id_03`–`id_06`, `id_09`–`id_11`, `id_12`–`id_21`, `id_23`–`id_25`, `id_28`–`id_38`, `DeviceType`, `DeviceInfo`.

### 3.3 Engineered Features
- Amount transforms: `amt_log1p`, `amt_cents`, `amt_decimals_gt2_flag`, `amt_is_whole_flag`.
- Relative temporal: `hour_of_day`, `hour_sin`, `hour_cos`, `day_of_week_rel`.
- Missingness & Identity: `has_identity_flag`, `identity_nonnull_count`, `identity_completeness`, `ind_addr`, `ind_dist1`, `ind_dist2`, `ind_Pemail`, `ind_Remail`, `ind_D2`..`ind_D15`, `ind_V*`.
- Masked family aggregates: `m_true_cnt`, `m_false_cnt`, `m_null_cnt`, `c_sum_log1p`, `c_nonzero_cnt`, `c_max`, `d_nonnull_cnt`, `d_min`, `d_max`.
- Strictly past card behavioral: `card_prior_txn_count`, `card_first_seen_flag`, `card_prior_amt_sum`, `card_prior_amt_mean`, `card_prior_amt_std`, `card_prior_amt_max`, `amt_zscore_vs_card`, `card_secs_since_last_txn`, `card_txn_count_1h`, `card_txn_count_24h`, `card_txn_count_7d`.

---

## 4. Downstream Responsibilities

### Member 4 (Machine Learning)
- Read features directly from Parquet paths using the `split` column.
- Select model features by filtering out Reference/Key columns and Target.
- Train baseline models on **Tier A features only**. Run a separate ablation with Tier A + Tier B to document simulator separability.
- Perform tree-based or imputation-based handling of retained sparse columns (nulls were deliberately preserved by Member 2 per Member 1 handoff contract).

### Member 3 (Kafka + Structured Streaming)
- Ingest raw transactions and compute Tier A features statefully using identical window specifications:
  - PaySim: Key on `nameDest`, state window over `step`.
  - IEEE-CIS: Key on `card_proxy_id`, state window over `TransactionDT`.
- Maintain bounded rolling windows (24h, 168h) in streaming state stores (RocksDB).
