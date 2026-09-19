"""
IEEE-CIS Aggregate and Strictly Historical Behavioral Feature Engineering.

Computes:
1. Amount transforms and currency conversion fingerprints.
2. Masked column family summaries (C-counts, M-matches, D-deltas).
3. Card proxy historical metrics and velocity windows (1h, 24h, 7d).

CRITICAL NON-LEAKAGE ENFORCEMENT:
All card proxy aggregations are strictly historical:
- Window ordered deterministically by (TransactionDT, TransactionID).
- Frame: rowsBetween(Window.unboundedPreceding, -1) and rangeBetween(-W, -1).
- Current transaction and future transactions are strictly excluded.
"""

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_ieee_amount_features(df: DataFrame) -> DataFrame:
    """
    Compute continuous amount transforms and currency-conversion fingerprints.

    Features derived:
        - amt_log1p: log1p(TransactionAmt).
        - amt_cents: Fractional cents component round((amt - floor(amt)) * 100).
        - amt_decimals_gt2_flag: Flag indicating >2 decimal places (90.4% true for ProductCD 'C', 0% for others).
        - amt_is_whole_flag: Indicator whether TransactionAmt is an integer amount.
    """
    return (
        df.withColumn(
            "amt_log1p",
            F.log1p(F.greatest(F.lit(0.0), F.col("TransactionAmt"))),
        )
        .withColumn(
            "amt_cents",
            F.round((F.col("TransactionAmt") - F.floor(F.col("TransactionAmt"))) * 100).cast("integer"),
        )
        .withColumn(
            "amt_decimals_gt2_flag",
            F.when(
                F.abs(F.col("TransactionAmt") * 100 - F.round(F.col("TransactionAmt") * 100)) > 1e-4,
                1,
            )
            .otherwise(0)
            .cast("tinyint"),
        )
        .withColumn(
            "amt_is_whole_flag",
            F.when(
                F.col("TransactionAmt") == F.floor(F.col("TransactionAmt")),
                1,
            )
            .otherwise(0)
            .cast("tinyint"),
        )
    )


def add_ieee_masked_aggregates(df: DataFrame) -> DataFrame:
    """
    Compute row-wise summary statistics across masked column families (M, C, D).
    Safely skips columns that are not present in the input DataFrame (e.g. in test fixtures).
    """
    existing_cols = set(df.columns)

    # 1. Match flags (M1 to M9)
    m_cols = [f"M{i}" for i in range(1, 10) if f"M{i}" in existing_cols]
    if m_cols:
        true_expr = sum([F.when(F.col(c) == "T", 1).otherwise(0) for c in m_cols])
        false_expr = sum([F.when(F.col(c) == "F", 1).otherwise(0) for c in m_cols])
        null_expr = sum([F.when(F.col(c).isNull(), 1).otherwise(0) for c in m_cols])

        df = (
            df.withColumn("m_true_cnt", true_expr.cast("integer"))
            .withColumn("m_false_cnt", false_expr.cast("integer"))
            .withColumn("m_null_cnt", null_expr.cast("integer"))
        )

    # 2. Count columns (C1 to C14)
    c_cols = [f"C{i}" for i in range(1, 15) if f"C{i}" in existing_cols]
    if c_cols:
        c_sum_expr = sum([F.coalesce(F.col(c), F.lit(0.0)) for c in c_cols])
        c_nonzero_expr = sum([F.when(F.col(c) > 0, 1).otherwise(0) for c in c_cols])

        df = (
            df.withColumn("c_sum_log1p", F.log1p(F.greatest(F.lit(0.0), c_sum_expr)))
            .withColumn("c_nonzero_cnt", c_nonzero_expr.cast("integer"))
        )
        if len(c_cols) >= 2:
            df = df.withColumn("c_max", F.greatest(*[F.coalesce(F.col(c), F.lit(0.0)) for c in c_cols]))
        else:
            df = df.withColumn("c_max", F.coalesce(F.col(c_cols[0]), F.lit(0.0)))

    # 3. Timedelta columns (D1 to D15, excluding experimental D8/D9)
    d_cols = [f"D{i}" for i in [1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15] if f"D{i}" in existing_cols]
    if d_cols:
        d_nonnull_expr = sum([F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in d_cols])
        df = df.withColumn("d_nonnull_cnt", d_nonnull_expr.cast("integer"))
        if len(d_cols) >= 2:
            df = (
                df.withColumn("d_min", F.least(*[F.coalesce(F.col(c), F.lit(999999.0)) for c in d_cols]))
                .withColumn("d_max", F.greatest(*[F.coalesce(F.col(c), F.lit(-999999.0)) for c in d_cols]))
            )
        else:
            df = (
                df.withColumn("d_min", F.col(d_cols[0]))
                .withColumn("d_max", F.col(d_cols[0]))
            )

    return df


def add_ieee_card_history_features(df: DataFrame) -> DataFrame:
    """
    Compute strictly historical behavioral statistics and rolling velocity per card proxy.

    Ordering key: (TransactionDT, TransactionID) — guarantees deterministic total ordering.
    Frame: rowsBetween(Window.unboundedPreceding, -1) and rangeBetween(-W, -1).
    """
    # Deterministic strictly prior window for card_proxy_id
    w_card_prior = (
        Window.partitionBy("card_proxy_id")
        .orderBy("TransactionDT", "TransactionID")
        .rowsBetween(Window.unboundedPreceding, -1)
    )

    # Velocity range windows over TransactionDT (seconds)
    w_1h = Window.partitionBy("card_proxy_id").orderBy("TransactionDT").rangeBetween(-3600, -1)
    w_24h = Window.partitionBy("card_proxy_id").orderBy("TransactionDT").rangeBetween(-86400, -1)
    w_7d = Window.partitionBy("card_proxy_id").orderBy("TransactionDT").rangeBetween(-604800, -1)

    df = (
        df.withColumn(
            "card_prior_txn_count",
            F.coalesce(F.count(F.lit(1)).over(w_card_prior), F.lit(0)).cast("integer"),
        )
        .withColumn(
            "card_first_seen_flag",
            F.when(F.col("card_prior_txn_count") == 0, 1).otherwise(0).cast("tinyint"),
        )
        .withColumn(
            "card_prior_amt_sum",
            F.coalesce(F.sum("TransactionAmt").over(w_card_prior), F.lit(0.0)),
        )
        .withColumn(
            "card_prior_amt_mean",
            F.avg("TransactionAmt").over(w_card_prior),
        )
        .withColumn(
            "card_prior_amt_std",
            F.stddev("TransactionAmt").over(w_card_prior),
        )
        .withColumn(
            "card_prior_amt_max",
            F.max("TransactionAmt").over(w_card_prior),
        )
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
        .withColumn(
            "card_txn_count_1h",
            F.coalesce(F.count(F.lit(1)).over(w_1h), F.lit(0)).cast("integer"),
        )
        .withColumn(
            "card_txn_count_24h",
            F.coalesce(F.count(F.lit(1)).over(w_24h), F.lit(0)).cast("integer"),
        )
        .withColumn(
            "card_txn_count_7d",
            F.coalesce(F.count(F.lit(1)).over(w_7d), F.lit(0)).cast("integer"),
        )
    )

    return df
