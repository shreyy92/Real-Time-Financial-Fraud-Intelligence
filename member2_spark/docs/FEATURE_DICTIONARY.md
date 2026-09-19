# Feature Dictionary — Real-Time Financial Fraud Intelligence Platform

This document details every feature engineered by **Member 2 (PySpark Feature Engineering & Distributed Processing)**. Every feature is categorized by tier, leakage status, and real-time availability.

---

## 1. PaySim Features

| Feature Name | Dataset | Data Type | Source Column(s) | Calculation | Meaning | Tier | Leakage Status | Available At Prediction Time |
|---|---|---|---|---|---|---|---|---|
| `txn_id` | PaySim | string | `step, type, amount, nameOrig, nameDest` | `sha2(concat_ws('\|', ...), 256)` | Unique deterministic surrogate transaction key | Reference | SAFE | YES |
| `step` | PaySim | integer | `step` | Pass-through | Simulation hour (1–743); temporal ordering key | Reference | SAFE | YES |
| `step_hour` | PaySim | integer | `step` | `step % 24` | Diurnal cycle hour (0–23) | Tier A | SAFE | YES |
| `hour_sin` | PaySim | double | `step_hour` | `sin(2π · step_hour / 24)` | Trigonometric cyclical representation of hour | Tier A | SAFE | YES |
| `hour_cos` | PaySim | double | `step_hour` | `cos(2π · step_hour / 24)` | Trigonometric cyclical representation of hour | Tier A | SAFE | YES |
| `sim_day` | PaySim | integer | `step` | `(step - 1) div 24` | Simulated day index (0–30) for split tracking | Reference | SAFE | YES |
| `history_window_complete_flag` | PaySim | tinyint | `step` | `step >= 169` | Indicator that 7-day (168h) history warmup is complete | Reference | SAFE | YES |
| `type_transfer` | PaySim | tinyint | `type` | `type == 'TRANSFER'` | Indicator of TRANSFER type (fraud-prone) | Tier A | SAFE | YES |
| `type_cash_out` | PaySim | tinyint | `type` | `type == 'CASH_OUT'` | Indicator of CASH_OUT type (fraud-prone) | Tier A | SAFE | YES |
| `type_payment` | PaySim | tinyint | `type` | `type == 'PAYMENT'` | Indicator of PAYMENT type (0% fraud) | Tier A | SAFE | YES |
| `type_cash_in` | PaySim | tinyint | `type` | `type == 'CASH_IN'` | Indicator of CASH_IN type (0% fraud) | Tier A | SAFE | YES |
| `type_debit` | PaySim | tinyint | `type` | `type == 'DEBIT'` | Indicator of DEBIT type (0% fraud) | Tier A | SAFE | YES |
| `amount` | PaySim | double | `amount` | Pass-through | Transaction amount in local currency | Tier A | SAFE | YES |
| `amount_log1p` | PaySim | double | `amount` | `log1p(greatest(0, amount))` | Tames heavy right-skewed amount distribution | Tier A | SAFE | YES |
| `amount_zero_flag` | PaySim | tinyint | `amount` | `amount == 0.0` | Rare-event flag (16 rows in PaySim, all fraud) | Tier A | SAFE | YES |
| `amt_to_oldbalOrg` | PaySim | double | `amount, oldbalanceOrg` | `amount / (oldbalanceOrg + 1.0)` | Ratio of transfer magnitude to origin account balance | Tier A | SAFE | YES |
| `amt_to_oldbalDest` | PaySim | double | `amount, oldbalanceDest` | `amount / (oldbalanceDest + 1.0)` | Ratio of transfer magnitude to recipient pre-balance | Tier A | SAFE | YES |
| `orig_drain_flag` | PaySim | tinyint | `amount, oldbalanceOrg` | `oldbalanceOrg > 0 AND abs(amount - oldbalanceOrg) < 0.005` | Simulator artifact: perfect account drainage (97.6% fraud) | Tier A | SAFE (Artifact) | YES |
| `amount_exceeds_orig_balance_flag` | PaySim | tinyint | `amount, oldbalanceOrg` | `amount > oldbalanceOrg` | Simulator anomaly: 90.1% legit transfers exceed balance | Tier A | SAFE | YES |
| `orig_balance_zero_flag` | PaySim | tinyint | `oldbalanceOrg` | `oldbalanceOrg == 0.0` | Initial sender balance is zero | Tier A | SAFE | YES |
| `dest_balance_zero_flag` | PaySim | tinyint | `oldbalanceDest, type` | `oldbalanceDest == 0.0 AND type != 'PAYMENT'` | Non-merchant recipient with zero pre-balance | Tier A | SAFE | YES |
| `dest_prior_txn_count` | PaySim | integer | `nameDest, step` | `count() OVER (dest, step < t)` | Number of prior transactions received by destination | Tier A | SAFE | YES (Stateful) |
| `dest_first_seen_flag` | PaySim | tinyint | `dest_prior_txn_count` | `dest_prior_txn_count == 0` | Recipient has no earlier history in dataset (novelty marker) | Tier A | SAFE | YES (Stateful) |
| `dest_prior_amount_sum` | PaySim | double | `nameDest, step, amount` | `sum(amount) OVER (dest, step < t)` | Cumulative inflow amount received by destination in past | Tier A | SAFE | YES (Stateful) |
| `dest_prior_amount_mean` | PaySim | double | `nameDest, step, amount` | `avg(amount) OVER (dest, step < t)` | Historical average transaction size received by destination | Tier A | SAFE | YES (Stateful) |
| `dest_prior_amount_std` | PaySim | double | `nameDest, step, amount` | `stddev(amount) OVER (dest, step < t)` | Historical amount volatility received by destination | Tier A | SAFE | YES (Stateful) |
| `dest_prior_amount_max` | PaySim | double | `nameDest, step, amount` | `max(amount) OVER (dest, step < t)` | Largest prior transaction size received by destination | Tier A | SAFE | YES (Stateful) |
| `dest_amount_zscore` | PaySim | double | `amount, dest_prior_*` | `(amount - mean) / std (if count >= 3)` | Current amount deviation from destination normal inflow | Tier A | SAFE | YES (Stateful) |
| `dest_steps_since_last_txn` | PaySim | integer | `nameDest, step` | `step - max(prior_step)` | Recency/dormancy: hours since last transaction received | Tier A | SAFE | YES (Stateful) |
| `dest_txn_count_prev_24h` | PaySim | integer | `nameDest, step` | `count() OVER (dest, [t-24, t-1])` | Destination 24-hour transaction velocity burst | Tier A | SAFE | YES (Stateful) |
| `dest_txn_count_prev_168h` | PaySim | integer | `nameDest, step` | `count() OVER (dest, [t-168, t-1])` | Destination 7-day transaction velocity | Tier A | SAFE | YES (Stateful) |
| `dest_amount_sum_prev_24h` | PaySim | double | `nameDest, step, amount` | `sum(amount) OVER (dest, [t-24, t-1])`| Inflow amount velocity in preceding 24 hours | Tier A | SAFE | YES (Stateful) |
| `dest_amount_sum_prev_168h` | PaySim | double | `nameDest, step, amount` | `sum(amount) OVER (dest, [t-168, t-1])`| Inflow amount velocity in preceding 7 days | Tier A | SAFE | YES (Stateful) |
| `dest_prior_cashout_share` | PaySim | double | `nameDest, step, type` | `prior_cashout_count / dest_prior_count` | Historical fraction of CASH_OUT transactions received | Tier A | SAFE | YES (Stateful) |
| `newbalanceOrig` | PaySim | double | `newbalanceOrig` | Pass-through | Sender balance after transaction | Tier B | LEAKAGE_RISK | NO (Post-Txn) |
| `newbalanceDest` | PaySim | double | `newbalanceDest` | Pass-through | Recipient balance after transaction | Tier B | LEAKAGE_RISK | NO (Post-Txn) |
| `err_balance_orig` | PaySim | double | `newbalanceOrig, amount, oldbalanceOrg` | `newbalanceOrig + amount - oldbalanceOrg` | Accounting error delta (0 for 99.45% of fraud vs 9.5% legit) | Tier B | LEAKAGE_RISK | NO (Post-Txn) |
| `err_balance_dest` | PaySim | double | `oldbalanceDest, amount, newbalanceDest` | `oldbalanceDest + amount - newbalanceDest` | Recipient accounting error delta | Tier B | LEAKAGE_RISK | NO (Post-Txn) |
| `orig_new_balance_zero_flag` | PaySim | tinyint | `newbalanceOrig` | `newbalanceOrig == 0.0` | Sender account balance emptied following transaction | Tier B | LEAKAGE_RISK | NO (Post-Txn) |

---

## 2. IEEE-CIS Features

| Feature Name | Dataset | Data Type | Source Column(s) | Calculation | Meaning | Tier | Leakage Status | Available At Prediction Time |
|---|---|---|---|---|---|---|---|---|
| `TransactionID` | IEEE | integer | `TransactionID` | Pass-through | Unique transaction key (join key, not a feature) | Reference | SAFE | YES |
| `TransactionDT` | IEEE | integer | `TransactionDT` | Pass-through | Relative timedelta in seconds from undisclosed origin | Reference | SAFE | YES |
| `card_proxy_id` | IEEE | string | `card1`–`card6` | `sha2(concat_ws('_', card1..card6), 256)` | Deterministic proxy entity representing payment card | Reference | SAFE | YES |
| `card_addr_proxy_id` | IEEE | string | `card_proxy_id, addr1` | `sha2(concat_ws('_', card_proxy_id, addr1), 256)` | Card proxy bound to billing address region | Reference | SAFE | YES |
| `hour_of_day` | IEEE | integer | `TransactionDT` | `floor(TransactionDT / 3600) % 24` | Relative hour of transaction (2.4% to 10.1% fraud spread) | Tier A | SAFE | YES |
| `hour_sin` | IEEE | double | `hour_of_day` | `sin(2π · hour_of_day / 24)` | Trigonometric cyclical representation of relative hour | Tier A | SAFE | YES |
| `hour_cos` | IEEE | double | `hour_of_day` | `cos(2π · hour_of_day / 24)` | Trigonometric cyclical representation of relative hour | Tier A | SAFE | YES |
| `day_index` | IEEE | integer | `TransactionDT` | `floor(TransactionDT / 86400)` | Relative day number (day 1 to 183) | Reference | SAFE | YES |
| `day_of_week_rel` | IEEE | integer | `day_index` | `day_index % 7` | Relative 7-day cyclical weekday proxy | Tier A | SAFE | YES |
| `history_window_complete_flag` | IEEE | tinyint | `day_index` | `day_index >= 7` | Indicator that 7-day card velocity warmup is complete | Reference | SAFE | YES |
| `amt_log1p` | IEEE | double | `TransactionAmt` | `log1p(greatest(0, TransactionAmt))` | Tames heavy right-skewed amount distribution | Tier A | SAFE | YES |
| `amt_cents` | IEEE | integer | `TransactionAmt` | `round((amt - floor(amt)) * 100)` | Fractional cents component (.00, .95, .99 patterns) | Tier A | SAFE | YES |
| `amt_decimals_gt2_flag` | IEEE | tinyint | `TransactionAmt` | `abs(amt*100 - round(amt*100)) > 1e-4` | Currency conversion fingerprint (90.4% true for ProductCD C) | Tier A | SAFE | YES |
| `amt_is_whole_flag` | IEEE | tinyint | `TransactionAmt` | `amt == floor(amt)` | Round dollar transaction indicator (card testing marker) | Tier A | SAFE | YES |
| `has_identity_flag` | IEEE | tinyint | `id_01`–`id_38, Device*`| Any non-null in identity table (7.85% vs 2.09% fraud) | Tier A | SAFE | YES |
| `identity_nonnull_count` | IEEE | integer | `id_01`–`id_38, Device*`| Count of populated fields across 40 identity columns | Tier A | SAFE | YES |
| `identity_completeness` | IEEE | double | `identity_nonnull_count`| `identity_nonnull_count / 40.0` | Completeness gradient of device and identity data | Tier A | SAFE | YES |
| `ind_addr` | IEEE | tinyint | `addr1` | `addr1 IS NOT NULL` | Address presence (null has 11.78% fraud vs 2.46% present) | Tier A | SAFE | YES |
| `ind_dist1` | IEEE | tinyint | `dist1` | `dist1 IS NOT NULL` | Distance 1 presence indicator (MG28) | Tier A | SAFE | YES |
| `ind_dist2` | IEEE | tinyint | `dist2` | `dist2 IS NOT NULL` | Distance 2 presence indicator (MG63: 9.9% fraud if present) | Tier A | SAFE | YES |
| `ind_Pemail` | IEEE | tinyint | `P_emaildomain` | `P_emaildomain IS NOT NULL` | Purchaser email domain presence indicator | Tier A | SAFE | YES |
| `ind_Remail` | IEEE | tinyint | `R_emaildomain` | `R_emaildomain IS NOT NULL` | Recipient email domain presence indicator | Tier A | SAFE | YES |
| `ind_D2`..`ind_D15` | IEEE | tinyint | `D2`..`D15` | `col IS NOT NULL` | Timedelta block presence indicators | Tier A | SAFE | YES |
| `ind_V*` blocks | IEEE | tinyint | `V1, V12, V35, ...` | Representative block `col IS NOT NULL` | Vesta engineered cluster presence flags | Tier A | SAFE | YES |
| `m_true_cnt` | IEEE | integer | `M1`–`M9` | Count of 'T' match flags | Match attribute agreement summary | Tier A | SAFE | YES |
| `m_false_cnt` | IEEE | integer | `M1`–`M9` | Count of 'F' match flags | Match attribute mismatch summary | Tier A | SAFE | YES |
| `m_null_cnt` | IEEE | integer | `M1`–`M9` | Count of null match flags | Match attribute missingness summary | Tier A | SAFE | YES |
| `c_sum_log1p` | IEEE | double | `C1`–`C14` | `log1p(sum(C1..C14))` | Aggregate mass of counting features linked to card | Tier A | SAFE | YES |
| `c_nonzero_cnt` | IEEE | integer | `C1`–`C14` | Count of C columns with value > 0 | Active count indicator | Tier A | SAFE | YES |
| `c_max` | IEEE | double | `C1`–`C14` | `greatest(C1..C14)` | Maximum counting feature value | Tier A | SAFE | YES |
| `d_nonnull_cnt` | IEEE | integer | `D1..D7, D10..D15` | Count of non-null timedelta features | Populated timedelta depth | Tier A | SAFE | YES |
| `d_min` | IEEE | double | `D1..D7, D10..D15` | Minimum timedelta across non-null Ds | Most recent transaction interval proxy | Tier A | SAFE | YES |
| `d_max` | IEEE | double | `D1..D7, D10..D15` | Maximum timedelta across non-null Ds | Oldest transaction interval proxy | Tier A | SAFE | YES |
| `card_prior_txn_count` | IEEE | integer | `card_proxy_id, DT` | `count() OVER (card, prior)` | Prior transaction count on card proxy | Tier A | SAFE | YES (Stateful) |
| `card_first_seen_flag` | IEEE | tinyint | `card_prior_txn_count` | `card_prior_txn_count == 0` | First observed use of this payment card in dataset | Tier A | SAFE | YES (Stateful) |
| `card_prior_amt_sum` | IEEE | double | `card_proxy_id, amt` | `sum(amt) OVER (card, prior)` | Cumulative prior spend on this card proxy | Tier A | SAFE | YES (Stateful) |
| `card_prior_amt_mean` | IEEE | double | `card_proxy_id, amt` | `avg(amt) OVER (card, prior)` | Historical average transaction spend on card proxy | Tier A | SAFE | YES (Stateful) |
| `card_prior_amt_std` | IEEE | double | `card_proxy_id, amt` | `stddev(amt) OVER (card, prior)` | Spending volatility on card proxy | Tier A | SAFE | YES (Stateful) |
| `card_prior_amt_max` | IEEE | double | `card_proxy_id, amt` | `max(amt) OVER (card, prior)` | Peak historical transaction amount on card proxy | Tier A | SAFE | YES (Stateful) |
| `amt_zscore_vs_card` | IEEE | double | `amt, card_prior_*` | `(amt - mean) / std (if count >= 3)` | Current transaction deviation from normal card spend | Tier A | SAFE | YES (Stateful) |
| `card_secs_since_last_txn` | IEEE | integer | `card_proxy_id, DT` | `DT - previous_DT` | Seconds elapsed since previous transaction on card | Tier A | SAFE | YES (Stateful) |
| `card_txn_count_1h` | IEEE | integer | `card_proxy_id, DT` | `count() OVER (card, [t-3600, t-1])` | 1-hour transaction velocity burst on card | Tier A | SAFE | YES (Stateful) |
| `card_txn_count_24h` | IEEE | integer | `card_proxy_id, DT` | `count() OVER (card, [t-86400, t-1])` | 24-hour transaction velocity on card | Tier A | SAFE | YES (Stateful) |
| `card_txn_count_7d` | IEEE | integer | `card_proxy_id, DT` | `count() OVER (card, [t-604800, t-1])`| 7-day transaction velocity on card | Tier A | SAFE | YES (Stateful) |
