"""
PaySim Velocity and Rolling Window Feature Engineering.

Computes rolling destination transaction velocity over bounded temporal horizons (24h and 168h).
Strictly prevents global volume leakage (per Member 1 Audit P5).
All rolling frames exclude the current transaction (rangeBetween -W to -1 on step).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_paysim_velocity_features(df: DataFrame) -> DataFrame:
    """
    Compute destination transaction velocity and burst features.

    Windows:
        - 24h Window: rangeBetween(-24, -1) partitioned on nameDest, ordered by step.
        - 168h Window: rangeBetween(-168, -1) partitioned on nameDest, ordered by step.

    Features derived:
        - dest_txn_count_prev_24h: Count of transactions received by nameDest in [step-24, step-1].
        - dest_txn_count_prev_168h: Count of transactions received by nameDest in [step-168, step-1].
        - dest_amount_sum_prev_24h: Total amount received by nameDest in [step-24, step-1].
        - dest_amount_sum_prev_168h: Total amount received by nameDest in [step-168, step-1].
        - dest_prior_cashout_share: Historical share of CASH_OUT transactions among all prior received.

    Args:
        df: Input PaySim DataFrame containing 'nameDest', 'step', 'type', and 'amount'.

    Returns:
        DataFrame: Augmented DataFrame with velocity features.
    """
    # 24-hour strictly prior rolling window
    w_24h = (
        Window.partitionBy("nameDest")
        .orderBy("step")
        .rangeBetween(-24, -1)
    )

    # 168-hour (7-day) strictly prior rolling window
    w_168h = (
        Window.partitionBy("nameDest")
        .orderBy("step")
        .rangeBetween(-168, -1)
    )

    # Unbounded strictly prior window for cashout share
    w_unbounded = (
        Window.partitionBy("nameDest")
        .orderBy("step")
        .rangeBetween(Window.unboundedPreceding, -1)
    )

    df = (
        df.withColumn(
            "dest_txn_count_prev_24h",
            F.coalesce(F.count(F.lit(1)).over(w_24h), F.lit(0)).cast("integer"),
        )
        .withColumn(
            "dest_txn_count_prev_168h",
            F.coalesce(F.count(F.lit(1)).over(w_168h), F.lit(0)).cast("integer"),
        )
        .withColumn(
            "dest_amount_sum_prev_24h",
            F.coalesce(F.sum("amount").over(w_24h), F.lit(0.0)),
        )
        .withColumn(
            "dest_amount_sum_prev_168h",
            F.coalesce(F.sum("amount").over(w_168h), F.lit(0.0)),
        )
        .withColumn(
            "_is_cashout",
            F.when(F.col("type") == "CASH_OUT", 1).otherwise(0),
        )
        .withColumn(
            "_dest_prior_cashout_count",
            F.coalesce(F.sum("_is_cashout").over(w_unbounded), F.lit(0)),
        )
        .withColumn(
            "dest_prior_cashout_share",
            F.when(
                F.col("dest_prior_txn_count") > 0,
                F.col("_dest_prior_cashout_count") / F.col("dest_prior_txn_count"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .drop("_is_cashout", "_dest_prior_cashout_count")
    )

    return df
