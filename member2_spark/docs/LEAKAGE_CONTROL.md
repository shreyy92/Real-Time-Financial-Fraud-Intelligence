# Leakage Control & Safety Manual — Member 2

This document establishes the anti-leakage policies, temporal ordering guarantees, and simulator artifact isolation rules enforced across Member 2's PySpark feature engineering layer.

---

## 1. Executive Rule: The Real-Time Evaluation Principle

> **Every feature value must be computable using ONLY information available at the exact instant the transaction arrived at the authorization engine.**
>
> If a feature uses information that occurs after transaction authorization, or relies on ground truth fraud labels that take weeks or months to arrive via chargebacks, it is **leakage** and will cause catastrophic train-serve skew in production.

---

## 2. Prohibited Features

### 2.1 Target Leakage (Completely Banned)
Under NO circumstances is `isFraud` or any derivative of `isFraud` permitted as an input feature during model training:
- `isFraud` (The target label itself).
- Target encodings across high-cardinality IDs (`nameDest_fraud_rate`, `card1_fraud_rate`).
- Aggregates over the target label (`step_fraud_count`, `hourly_fraud_rate`, `dest_prior_fraud_count`).
- Simulator rule outputs that directly leak fraud labels: `isFlaggedFraud` (fires on only 16 rows in PaySim, all TRANSFER and all fraud).

### 2.2 Global Volume & Prevalence Leakage (PaySim Specific)
In the PaySim simulator, the daily count of fraudulent transactions is approximately constant (216–320 frauds per day), whereas legitimate traffic volume varies wildly (from 272 to 574,255 transactions per day).
- **Consequence:** Quiet steps (<100 transactions) exhibit a **79.2% fraud rate**, whereas busy steps exhibit a **0.05% fraud rate**.
- **Prohibition:** Any feature measuring total traffic volume per step or global transaction rate (`step_count`, `txns_per_hour`, `global_velocity`) acts as a surrogate for the simulator's fraud prevalence. These features are strictly prohibited.

---

## 3. Tier A vs. Tier B Architecture

To maintain academic rigor and interview defensibility, features are strictly partitioned into two tiers:

### 3.1 Tier A: Authorization-Time Features (Default ML Input)
Features knowable at the moment the transaction request hits the payment gateway:
- Transaction amount and log transforms (`amount_log1p`).
- Pre-transaction account balances (`oldbalanceOrg`, `oldbalanceDest`).
- Cyclical and diurnal time indicators (`step_hour`, `hour_sin`, `hour_cos`).
- Strictly prior behavioral history (`dest_prior_txn_count`, `dest_first_seen_flag`).
- Strictly prior rolling velocity bursts (`dest_txn_count_prev_24h`, `dest_txn_count_prev_168h`).
- Structured missingness indicators (`has_identity_flag`, `ind_addr`).

### 3.2 Tier B: Post-Transaction Outcome State (Simulator Artifacts)
Features dependent on post-transaction execution state or simulator accounting quirks:
- Post-transaction balances (`newbalanceOrig`, `newbalanceDest`).
- Accounting balance delta errors:
  - `err_balance_orig = newbalanceOrig + amount - oldbalanceOrg`
  - `err_balance_dest = oldbalanceDest + amount - newbalanceDest`
- Account emptied indicator: `orig_new_balance_zero_flag`.

> [!CAUTION]
> **Simulator Arithmetic Artifact:**
> In PaySim, `newbalanceOrig == oldbalanceOrg - amount` holds for 99.45% of fraud vs. only 9.49% of legitimate rows, because 90.10% of legitimate simulator transactions violate accounting (`amount > oldbalanceOrg`).
> Feeding Tier B balance deltas directly to an ML classifier yields near-perfect ROC-AUC (>0.999). Member 4 must train models **with Tier A only**, and separately report Tier A+B as an ablation.

---

## 4. Historical Feature Rules: Preventing Future Contamination

### 4.1 Frame Definitions
When computing historical features (e.g. cumulative sums, means, transaction counts):
1. **The current transaction must NEVER be included in its own historical aggregate:**
   - In PaySim: `rangeBetween(Window.unboundedPreceding, -1)` on `step`.
   - In IEEE-CIS: `rowsBetween(Window.unboundedPreceding, -1)` ordered by `(TransactionDT, TransactionID)`.
2. **Future transactions must NEVER be included:**
   - Window upper bound is strictly `-1`.
3. **Same-step ties must be handled deterministically:**
   - In PaySim, 290,449 rows share `(nameDest, step)`. Using `rangeBetween(..., -1)` over `step` ensures that any row with the same `step` is excluded, eliminating intraday leakage.
   - In IEEE-CIS, 33,932 rows share `TransactionDT`. Adding `TransactionID` as a tie-breaker establishes a strict total ordering.

### 4.2 Proof of Non-Leakage via Automated Tests
Unit tests in `member2_spark/tests/` verify:
1. Destination's very first transaction always evaluates to `dest_prior_txn_count == 0` and `dest_first_seen_flag == 1`.
2. Reversing or randomly shuffling the input row order results in **100% identical** feature values.
3. No historical aggregate correlates perfectly with the target label.

---

## 5. Time-Based Split Protocol

Random k-fold or row-wise train/test splitting leaks future patterns into the training set and violates temporal causality.
Both datasets are partitioned using strict time boundaries:

| Dataset | Time Column | Train Split | Validation Split | Test Split |
|---|---|---|---|---|
| **PaySim** | `step` | Steps 1–500 (~6.06M rows) | Steps 501–600 (~197K rows) | Steps 601–743 (~103K rows) |
| **IEEE-CIS** | `TransactionDT` | Day 1 to 130 (~443K rows) | Day 131 to 183 (~148K rows) | N/A (Train table only) |

The time split assignment is persisted directly in the `split` column of the output feature Parquet files. Downstream consumers (Member 4) must use this split column without modification.
