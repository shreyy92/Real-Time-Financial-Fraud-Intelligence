# Chunk 6 Report: IEEE-CIS Discovery (Read-Only)

**Execution Mode**: Real Kafka: **N/A** (Read-Only Discovery) | Real Spark Structured Streaming query: **N/A**  
**Date**: 2026-10-13  
**Status**: **PASSED**

---

## 1. Discovery Findings (Items a through j)

### a) `card_proxy_id` and `card_addr_proxy_id` Generation
Quoted verbatim from `member2_spark/src/ieee/card_features.py` (lines 38–71):

```python
    existing_cols = set(df.columns)

    def _col_or_na(c: str):
        if c in existing_cols:
            return F.coalesce(F.col(c).cast("string"), F.lit("NA"))
        return F.lit("NA")

    # 1. Card Proxy Key (Instrument level)
    df = df.withColumn(
        "card_proxy_id",
        F.sha2(
            F.concat_ws(
                "_",
                _col_or_na("card1"),
                _col_or_na("card2"),
                _col_or_na("card3"),
                _col_or_na("card4"),
                _col_or_na("card5"),
                _col_or_na("card6"),
            ),
            256,
        ),
    )

    # 2. Card + Address Proxy Key (Instrument + billing address region)
    df = df.withColumn(
        "card_addr_proxy_id",
        F.sha2(
            F.concat_ws(
                "_",
                F.col("card_proxy_id"),
                _col_or_na("addr1"),
            ),
            256,
        ),
    )
```

- **Exact Column List**:
  - `card_proxy_id`: `card1`, `card2`, `card3`, `card4`, `card5`, `card6`
  - `card_addr_proxy_id`: `card_proxy_id`, `addr1`
- **Separator**: Underscore (`"_"`)
- **Null Handling**: Missing or null attributes are coalesced to string `"NA"` using `F.coalesce(F.col(c).cast("string"), F.lit("NA"))` (neither empty string `""` nor SQL null drop).
- **Casts**: Explicit string cast (`.cast("string")`).
- **Hash Function**: SHA-256 (`F.sha2(..., 256)`).

---

### b) Input File and Schema Parity
- **Input Path**: `data/processed/ieee_clean.parquet` (configured in `member2_spark/config/feature_config.yaml`, line 37).
- **Parquet vs CSV Raw Schema Differences**:
  - Total columns in `data/processed/ieee_clean.parquet`: **434 columns**
  - Total columns in `member_audit1/schemas/ieee_schema.csv`: **434 columns**
  - Column difference: `clean_cols ^ csv_cols = set()` (**0 differences, 100% schema match**).

---

### c) Ordering Column, Units, and Tie-Break Rule
- **Ordering Column**: `TransactionDT`
- **Units**: Relative timedelta in **seconds** (elapsed seconds from undisclosed reference origin; minimum: 86,400 s, maximum: 15,811,131 s).
- **Tie-Break Rule**: `TransactionID` (ascending integer primary key).
- **Quoted Evidence**:
  - From `member2_spark/src/ieee/aggregate_features.py` (lines 124–128):
    ```python
    w_card_prior = (
        Window.partitionBy("card_proxy_id")
        .orderBy("TransactionDT", "TransactionID")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    ```
  - From `member2_spark/docs/LEAKAGE_CONTROL.md` (line 70):
    > "In IEEE-CIS, 33,932 rows share TransactionDT. Adding TransactionID as a tie-breaker establishes a strict total ordering."

---

### d) Tier B (Leakage) Columns in IEEE
- **Finding**: **Zero columns in IEEE are classified as Tier B.**
- **Quoted Evidence**:
  - From `member2_spark/docs/LEAKAGE_CONTROL.md` (Section 3.2):
    Tier B applies exclusively to PaySim post-transaction balances (`newbalanceOrig`, `newbalanceDest`, `err_balance_orig`, `err_balance_dest`, `orig_new_balance_zero_flag`).
  - From `member2_spark/docs/OUTPUT_CONTRACT.md` (Section 3.3):
    All engineered features for IEEE are Tier A (`amount`, `temporal`, `missingness`, `masked aggregates`, `historical card behavioral`).
- **Prohibited Attributes**:
  - `isFraud` (ground truth label, retained solely as target).
  - Global volume / rate proxies (`txns_per_hour`, `global_velocity`).
  - The 14 uninformative near-constant columns pruned from output: `V1, V14, V27, V28, V41, V65, V68, V88, V89, V107, V240, V241, V305, id_27`.

---

### e) Full IEEE Output Column List and Grouping (483 Total Columns)

`data/features/ieee_features.parquet` contains **483 columns** partitioned as:

| Category | Count | Column Names / Subsets |
|---|---|---|
| **Label** | 1 | `isFraud` |
| **Primary Key** | 1 | `TransactionID` |
| **Proxy Keys** | 2 | `card_proxy_id`, `card_addr_proxy_id` |
| **Split & Version** | 2 | `split`, `feature_version` |
| **Temporal (Stateless)** | 6 | `hour_of_day`, `hour_sin`, `hour_cos`, `day_index`, `day_of_week_rel`, `history_window_complete_flag` |
| **Amount (Stateless)** | 4 | `amt_log1p`, `amt_cents`, `amt_decimals_gt2_flag`, `amt_is_whole_flag` |
| **Missingness & Identity (Stateless)** | 29 | `has_identity_flag`, `identity_nonnull_count`, `identity_completeness`, `ind_addr`, `ind_dist1`, `ind_dist2`, `ind_Pemail`, `ind_Remail`, `ind_D2`, `ind_D3`, `ind_D4`, `ind_D5`, `ind_D6`, `ind_D7`, `ind_D10`, `ind_D11`, `ind_D12`, `ind_D13`, `ind_D14`, `ind_D15`, `ind_M1_3`, `ind_V1_11`, `ind_V12_34`, `ind_V35_52`, `ind_V53_74`, `ind_V75_94`, `ind_V138_166`, `ind_V167_278`, `ind_V322_339` |
| **Masked Aggregates (Stateless)** | 9 | `m_true_cnt`, `m_false_cnt`, `m_null_cnt`, `c_sum_log1p`, `c_nonzero_cnt`, `c_max`, `d_nonnull_cnt`, `d_min`, `d_max` |
| **Card Behavioral (Stateful)** | 11 | `card_prior_txn_count`, `card_first_seen_flag`, `card_prior_amt_sum`, `card_prior_amt_mean`, `card_prior_amt_std`, `card_prior_amt_max`, `amt_zscore_vs_card`, `card_secs_since_last_txn`, `card_txn_count_1h`, `card_txn_count_24h`, `card_txn_count_7d` |
| **Raw Retained Attributes** | 418 | `TransactionDT`, `TransactionAmt`, `ProductCD`, `card1`–`card6`, `addr1`, `addr2`, `dist1`, `dist2`, `P_emaildomain`, `R_emaildomain`, `C1`–`C14`, `D1`–`D7`, `D10`–`D15`, `M1`–`M9`, non-pruned `V*` columns, `id_01`–`id_26`, `id_28`–`id_38`, `DeviceType`, `DeviceInfo` |
| **Total** | **483** | **Exact match with Parquet output metadata** |

---

### f) Split Derivation and Boundaries
Quoted from `member2_spark/src/ieee/build_features.py` (lines 76–80):

```python
    train_max_seconds = train_max_day * 86400.0
    df_features = df_features.withColumn(
        "split",
        F.when(F.col("TransactionDT") <= train_max_seconds, "train").otherwise("valid"),
    )
```

- **Configuration**: `train_max_day: 130.0` (`member2_spark/config/feature_config.yaml`, line 46).
- **Threshold**: `130.0 * 86400 = 11,232,000` seconds.
- **Boundaries**:
  - `train`: `TransactionDT <= 11232000` (Days 1.0 to 130.0, 443,021 transactions, ~3.51% fraud).
  - `valid`: `TransactionDT > 11232000` (Days 130.0+ to 183.0, 147,519 transactions, ~3.45% fraud).
- **Test Split**: IEEE contains no `test` split rows in `ieee_features.parquet`.

---

### g) Raw Data Types and Column Counts
- **Total Raw Columns**: **434 columns** in `data/processed/ieee_clean.parquet` and `ieee_schema.csv`.
- **Type Distribution**:
  - `int32` (4 columns): `TransactionID`, `isFraud`, `TransactionDT`, `card1`.
  - `string` (31 columns): `ProductCD`, `card4`, `card6`, `P_emaildomain`, `R_emaildomain`, `M1`..`M9`, `id_12`, `id_15`, `id_16`, `id_23`, `id_27`, `id_28`, `id_29`, `id_30`, `id_31`, `id_33`, `id_34`, `id_35`, `id_36`, `id_37`, `id_38`, `DeviceType`, `DeviceInfo`.
  - `double` (399 columns): `TransactionAmt`, `card2`, `card3`, `card5`, `addr1`, `addr2`, `dist1`, `dist2`, `C1`..`C14`, `D1`..`D15`, `V1`..`V339`, `id_01`..`id_11`, `id_13`, `id_14`, `id_17`..`id_22`, `id_24`..`id_26`, `id_32`.

---

### h) Exact Stateless Feature Definitions
Quoted from Member 2 source files:

1. **Amount Transforms** (`aggregate_features.py`, lines 34–58):
   - `amt_log1p`: `F.log1p(F.greatest(F.lit(0.0), F.col("TransactionAmt")))`
   - `amt_cents`: `F.round((F.col("TransactionAmt") - F.floor(F.col("TransactionAmt"))) * 100).cast("integer")`
   - `amt_decimals_gt2_flag`: `F.when(F.abs(F.col("TransactionAmt") * 100 - F.round(F.col("TransactionAmt") * 100)) > 1e-4, 1).otherwise(0).cast("tinyint")`
   - `amt_is_whole_flag`: `F.when(F.col("TransactionAmt") == F.floor(F.col("TransactionAmt")), 1).otherwise(0).cast("tinyint")`

2. **Temporal Features** (`temporal_features.py`, lines 39–62):
   - `hour_of_day`: `(F.floor(F.col("TransactionDT") / 3600) % 24).cast("integer")`
   - `hour_sin`: `F.sin(F.col("hour_of_day") * (2.0 * math.pi / 24.0))`
   - `hour_cos`: `F.cos(F.col("hour_of_day") * (2.0 * math.pi / 24.0))`
   - `day_index`: `F.floor(F.col("TransactionDT") / 86400).cast("integer")`
   - `day_of_week_rel`: `(F.col("day_index") % 7).cast("integer")`
   - `history_window_complete_flag`: `F.when(F.col("day_index") >= 7, 1).otherwise(0).cast("tinyint")`

3. **Identity & Missingness Indicators** (`missingness_features.py`, lines 47–106):
   - `identity_nonnull_count`: `sum([F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in present_id_cols]).cast("integer")` across 40 identity columns (`id_01`–`id_38`, `DeviceType`, `DeviceInfo`).
   - `has_identity_flag`: `F.when(F.col("identity_nonnull_count") > 0, 1).otherwise(0).cast("tinyint")`
   - `identity_completeness`: `(F.col("identity_nonnull_count") / 40.0).cast("double")`
   - Block indicators: `ind_addr` (`addr1`), `ind_dist1`, `ind_dist2`, `ind_Pemail`, `ind_Remail`, `ind_D2`–`ind_D15`, `ind_M1_3` (`M1`), `ind_V1_11` (`V1`), `ind_V12_34` (`V12`), `ind_V35_52` (`V35`), `ind_V53_74` (`V53`), `ind_V75_94` (`V75`), `ind_V138_166` (`V138`), `ind_V167_278` (`V167`), `ind_V322_339` (`V322`), derived as `F.when(F.col(src).isNotNull(), 1).otherwise(0).cast("tinyint")`.

4. **Masked Column Family Aggregates** (`aggregate_features.py`, lines 72–110):
   - `m_true_cnt`: `sum([F.when(F.col(c) == "T", 1).otherwise(0) for c in M1..M9]).cast("integer")`
   - `m_false_cnt`: `sum([F.when(F.col(c) == "F", 1).otherwise(0) for c in M1..M9]).cast("integer")`
   - `m_null_cnt`: `sum([F.when(F.col(c).isNull(), 1).otherwise(0) for c in M1..M9]).cast("integer")`
   - `c_sum_log1p`: `F.log1p(F.greatest(F.lit(0.0), sum([F.coalesce(F.col(c), F.lit(0.0)) for c in C1..C14])))`
   - `c_nonzero_cnt`: `sum([F.when(F.col(c) > 0, 1).otherwise(0) for c in C1..C14]).cast("integer")`
   - `c_max`: `F.greatest(*[F.coalesce(F.col(c), F.lit(0.0)) for c in C1..C14])`
   - `d_nonnull_cnt`: `sum([F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in D1..D7, D10..D15]).cast("integer")`
   - `d_min`: `F.least(*[F.coalesce(F.col(c), F.lit(999999.0)) for c in d_cols])`
   - `d_max`: `F.greatest(*[F.coalesce(F.col(c), F.lit(-999999.0)) for c in d_cols])`

---

### i) Exact Stateful Feature Definitions
Quoted from `member2_spark/src/ieee/aggregate_features.py` (lines 124–188):

```python
    w_card_prior = (
        Window.partitionBy("card_proxy_id")
        .orderBy("TransactionDT", "TransactionID")
        .rowsBetween(Window.unboundedPreceding, -1)
    )

    w_1h = Window.partitionBy("card_proxy_id").orderBy("TransactionDT").rangeBetween(-3600, -1)
    w_24h = Window.partitionBy("card_proxy_id").orderBy("TransactionDT").rangeBetween(-86400, -1)
    w_7d = Window.partitionBy("card_proxy_id").orderBy("TransactionDT").rangeBetween(-604800, -1)

    df = (
        df.withColumn("card_prior_txn_count", F.coalesce(F.count(F.lit(1)).over(w_card_prior), F.lit(0)).cast("integer"))
        .withColumn("card_first_seen_flag", F.when(F.col("card_prior_txn_count") == 0, 1).otherwise(0).cast("tinyint"))
        .withColumn("card_prior_amt_sum", F.coalesce(F.sum("TransactionAmt").over(w_card_prior), F.lit(0.0)))
        .withColumn("card_prior_amt_mean", F.avg("TransactionAmt").over(w_card_prior))
        .withColumn("card_prior_amt_std", F.stddev("TransactionAmt").over(w_card_prior))
        .withColumn("card_prior_amt_max", F.max("TransactionAmt").over(w_card_prior))
        .withColumn(
            "amt_zscore_vs_card",
            F.when(
                (F.col("card_prior_txn_count") >= 3) & (F.col("card_prior_amt_std") > 1e-5),
                (F.col("TransactionAmt") - F.col("card_prior_amt_mean")) / F.col("card_prior_amt_std"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "card_secs_since_last_txn",
            F.when(
                F.col("card_prior_txn_count") > 0,
                F.col("TransactionDT") - F.lag("TransactionDT", 1).over(
                    Window.partitionBy("card_proxy_id").orderBy("TransactionDT", "TransactionID")
                ),
            ).otherwise(F.lit(None).cast("integer")),
        )
        .withColumn("card_txn_count_1h", F.coalesce(F.count(F.lit(1)).over(w_1h), F.lit(0)).cast("integer"))
        .withColumn("card_txn_count_24h", F.coalesce(F.count(F.lit(1)).over(w_24h), F.lit(0)).cast("integer"))
        .withColumn("card_txn_count_7d", F.coalesce(F.count(F.lit(1)).over(w_7d), F.lit(0)).cast("integer"))
    )
```

- **Window Frames**:
  - Historical cumulative: `rowsBetween(Window.unboundedPreceding, -1)` ordered by `(TransactionDT, TransactionID)`.
  - Velocity ranges: `rangeBetween(-W, -1)` on `TransactionDT` in seconds:
    - 1 Hour: `[-3600, -1]`
    - 24 Hours: `[-86400, -1]`
    - 7 Days: `[-604800, -1]`
- **Tie Semantics**:
  - In cumulative aggregates, `TransactionID` provides total tie-break ordering for transactions occurring at the identical second.
  - In velocity ranges, `rangeBetween(-W, -1)` strictly excludes current second `t` (upper bound `-1`). Transactions occurring at the identical second `t` do not count in each other's velocity window.
- **Small-Sample & Null Rules**:
  - `card_prior_amt_mean`: `null` when `card_prior_txn_count == 0`.
  - `card_prior_amt_std`: `null` when `card_prior_txn_count < 2`.
  - `card_prior_amt_max`: `null` when `card_prior_txn_count == 0`.
  - `card_secs_since_last_txn`: `null` when `card_prior_txn_count == 0`.
  - `amt_zscore_vs_card`: strictly `null` unless `card_prior_txn_count >= 3` AND `card_prior_amt_std > 1e-5`.

---

### j) Dataset Facts (Computed from Ground Truth Data)
- **Distinct `card_proxy_id` count**: **14,893** unique cards across 590,540 transactions.
- **Maximum events per card in any 7-day window (`card_txn_count_7d`)**: **1,678** events.
- **`TransactionDT` span**:
  - Min: `86,400` seconds (Day 1.0)
  - Max: `15,811,131` seconds (Day 183.0)
  - Span: `15,724,731` seconds = **182.00 days**.

---

## 2. Documentation vs. Code Disagreements (Check 6.2)

1. **`card_addr_proxy_id` Null Coalesce**:
   - `FEATURE_DICTIONARY.md` (row 60) states calculation as `sha2(concat_ws('_', card_proxy_id, addr1), 256)`.
   - In code (`card_features.py`, line 67), `addr1` is wrapped in `_col_or_na("addr1")`, which explicitly coalesces nulls to `'NA'`. Standard SQL `concat_ws` would drop null arguments. The streaming consumer must replicate the code's `_col_or_na` coalesce behavior.
2. **`D8` and `D9` Exclusion from Timedelta Family Aggregates**:
   - `OUTPUT_CONTRACT.md` (Section 3.2) lists `D1`–`D7`, `D10`–`D15`. `FEATURE_DICTIONARY.md` mentions `D1..D7, D10..D15`.
   - In code (`aggregate_features.py`, line 98), `D8` and `D9` are deliberately excluded from `d_nonnull_cnt`, `d_min`, and `d_max` because of unaligned missingness coupling to the identity table (`MG54`).
3. **`test` Split Non-existence**:
   - `feature_config.yaml` lines 46–47 comment that Days 131–183 is `valid`. `OUTPUT_CONTRACT.md` Section 1 lists `split in {'train', 'valid'}`.
   - Confirming: IEEE-CIS contains **no test split partition** in `data/features/ieee_features.parquet`.

---

## 3. Code Files Creation Audit (Check 6.3)

`git status` confirms that **no code files were created or modified** during Chunk 6. Only this report file `member3_streaming/reports/C06_report.md` was generated.
