"""
PaySim Destination Behavioral Feature Engineering.

Builds strictly historical behavioral metrics partitioned on destination accounts ('nameDest').
Follows the Member 1 audit finding that origin accounts are 99.85% single-use,
while destination accounts exhibit rich multi-transaction history.

Enforces zero leakage:
- Current transaction is strictly excluded (rangeBetween unboundedPreceding to -1).
- Future transactions are strictly excluded.
- Same-step ties are excluded from prior aggregates to prevent intraday leakage.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_paysim_destination_features(df: DataFrame) -> DataFrame:
    """
    Compute strictly historical behavioral features for PaySim destination accounts.

    Window definition:
        Partition: nameDest
        Order: step
        Frame: rangeBetween(Window.unboundedPreceding, -1)
        Ensures only rows where step < current_step are aggregated.

    Features derived:
        - dest_prior_txn_count: Number of transactions received by nameDest in earlier steps.
        - dest_first_seen_flag: 1 if dest_prior_txn_count == 0, else 0 (strong novelty signal).
        - dest_prior_amount_sum: Cumulative sum of amounts received in prior steps.
        - dest_prior_amount_mean: Mean amount received across prior steps.
        - dest_prior_amount_std: Sample standard deviation of prior received amounts.
        - dest_prior_amount_max: Maximum amount received across prior steps.
        - dest_amount_zscore: Z-score of current amount relative to prior history (null if count < 3).
        - dest_steps_since_last_txn: Number of steps elapsed since most recent prior transaction.

    Args:
        df: Input PaySim DataFrame containing 'nameDest', 'step', and 'amount'.

    Returns:
        DataFrame: Augmented DataFrame with historical destination features.
    """
    # Strictly prior window over steps: step <= current_step - 1
    w_dest_prior = (
        Window.partitionBy("nameDest")
        .orderBy("step")
        .rangeBetween(Window.unboundedPreceding, -1)
    )

    df = (
        df.withColumn(
            "dest_prior_txn_count",
            F.coalesce(F.count(F.lit(1)).over(w_dest_prior), F.lit(0)).cast("integer"),
        )
        .withColumn(
            "dest_first_seen_flag",
            F.when(F.col("dest_prior_txn_count") == 0, 1).otherwise(0).cast("tinyint"),
        )
        .withColumn(
            "dest_prior_amount_sum",
            F.coalesce(F.sum("amount").over(w_dest_prior), F.lit(0.0)),
        )
        .withColumn(
            "dest_prior_amount_mean",
            F.avg("amount").over(w_dest_prior),
        )
        .withColumn(
            "dest_prior_amount_std",
            F.stddev("amount").over(w_dest_prior),
        )
        .withColumn(
            "dest_prior_amount_max",
            F.max("amount").over(w_dest_prior),
        )
        .withColumn(
            "dest_steps_since_last_txn",
            F.when(
                F.col("dest_prior_txn_count") > 0,
                F.col("step") - F.max("step").over(w_dest_prior),
            ).otherwise(F.lit(None).cast("integer")),
        )
        .withColumn(
            "dest_amount_zscore",
            F.when(
                (F.col("dest_prior_txn_count") >= 3) & (F.col("dest_prior_amount_std") > 1e-5),
                (F.col("amount") - F.col("dest_prior_amount_mean")) / F.col("dest_prior_amount_std"),
            ).otherwise(F.lit(None).cast("double")),
        )
    )

    return df
