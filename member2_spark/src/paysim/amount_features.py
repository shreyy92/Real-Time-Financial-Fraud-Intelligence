"""
PaySim Amount and Balance Feature Engineering with Strict Tier Separation.

Separates Tier A (authorization-time safe features) from Tier B (post-transaction
state and simulator arithmetic artifacts) to prevent unintended leakage during model training.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_paysim_amount_features(df: DataFrame, include_tier_b: bool = True) -> DataFrame:
    """
    Compute amount and balance relationship features for PaySim.

    Tier A Features (Knowable at authorization time):
        - amount_log1p: log1p transformed transaction amount.
        - amount_zero_flag: Rare-event flag for amount == 0 (16 rows in PaySim, all fraud).
        - amt_to_oldbalOrg: Ratio of transaction amount to origin pre-balance (+1).
        - amt_to_oldbalDest: Ratio of transaction amount to destination pre-balance (+1).
        - orig_drain_flag: Simulator artifact indicator where amount equals oldbalanceOrg (>0).
          Captures 97.6% of fraud vs 0.0% of legitimate transactions.
        - amount_exceeds_orig_balance_flag: Indicator where amount > oldbalanceOrg.
        - orig_balance_zero_flag: Indicator where origin account had zero initial balance.
        - dest_balance_zero_flag: Indicator where destination balance is 0 for non-PAYMENT types
          (Merchants structurally have balance 0).

    Tier B Features (Post-transaction outcome state — requires explicit ablation):
        - err_balance_orig: Origin balance delta error: newbalanceOrig + amount - oldbalanceOrg.
        - err_balance_dest: Destination balance delta error: oldbalanceDest + amount - newbalanceDest.
        - orig_new_balance_zero_flag: Indicator where origin account is emptied (newbalanceOrig == 0).

    Args:
        df: Input PaySim DataFrame.
        include_tier_b: If True, computes and includes Tier B post-transaction features. Default is True.

    Returns:
        DataFrame: Augmented DataFrame with Tier A (and optionally Tier B) features.
    """
    # -------------------------------------------------------------------------
    # Tier A: Authorization-Time Features
    # -------------------------------------------------------------------------
    df = (
        df.withColumn(
            "amount_log1p",
            F.log1p(F.greatest(F.lit(0.0), F.col("amount"))),
        )
        .withColumn(
            "amount_zero_flag",
            F.when(F.col("amount") == 0.0, 1).otherwise(0).cast("tinyint"),
        )
        .withColumn(
            "amt_to_oldbalOrg",
            F.col("amount") / (F.col("oldbalanceOrg") + 1.0),
        )
        .withColumn(
            "amt_to_oldbalDest",
            F.col("amount") / (F.col("oldbalanceDest") + 1.0),
        )
        .withColumn(
            "orig_drain_flag",
            F.when(
                (F.col("oldbalanceOrg") > 0.0)
                & (F.abs(F.col("amount") - F.col("oldbalanceOrg")) < 0.005),
                1,
            )
            .otherwise(0)
            .cast("tinyint"),
        )
        .withColumn(
            "amount_exceeds_orig_balance_flag",
            F.when(F.col("amount") > F.col("oldbalanceOrg"), 1).otherwise(0).cast("tinyint"),
        )
        .withColumn(
            "orig_balance_zero_flag",
            F.when(F.col("oldbalanceOrg") == 0.0, 1).otherwise(0).cast("tinyint"),
        )
        .withColumn(
            "dest_balance_zero_flag",
            F.when(
                (F.col("oldbalanceDest") == 0.0) & (F.col("type") != "PAYMENT"),
                1,
            )
            .otherwise(0)
            .cast("tinyint"),
        )
    )

    # -------------------------------------------------------------------------
    # Tier B: Post-Transaction Features (Outcome State)
    # -------------------------------------------------------------------------
    if include_tier_b:
        df = (
            df.withColumn(
                "err_balance_orig",
                F.col("newbalanceOrig") + F.col("amount") - F.col("oldbalanceOrg"),
            )
            .withColumn(
                "err_balance_dest",
                F.col("oldbalanceDest") + F.col("amount") - F.col("newbalanceDest"),
            )
            .withColumn(
                "orig_new_balance_zero_flag",
                F.when(F.col("newbalanceOrig") == 0.0, 1).otherwise(0).cast("tinyint"),
            )
        )

    return df
