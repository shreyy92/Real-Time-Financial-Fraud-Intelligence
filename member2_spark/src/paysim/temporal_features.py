"""
PaySim Temporal and Transaction Type Feature Engineering.

Derives deterministic identifiers, cyclical temporal features, simulation day,
and transaction-type indicators grounded in the Member 1 audit report.
"""

import math
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_paysim_temporal_features(df: DataFrame) -> DataFrame:
    """
    Compute temporal representations, transaction-type encodings, and deterministic surrogate keys.

    Features derived:
        - txn_id: SHA-256 hash over (step, type, amount, nameOrig, nameDest).
        - step_hour: Hour within a 24-hour cycle (0-23). Uses existing column if present,
          else derives step % 24.
        - hour_sin / hour_cos: Trigonometric cyclical transformation of step_hour.
        - sim_day: Simulation day index ((step - 1) // 24), useful for bookkeeping and drift analysis.
        - history_window_complete_flag: Indicator whether step >= 169 (168 hours = 7-day warmup window).
        - type_transfer, type_cash_out, type_payment, type_cash_in, type_debit: Binary one-hot indicators.

    Args:
        df: Input PaySim DataFrame with 'step', 'type', 'amount', 'nameOrig', 'nameDest'.

    Returns:
        DataFrame: Augmented DataFrame with temporal and type features.
    """
    # 1. Deterministic surrogate key (5-tuple has zero duplicates in PaySim)
    if "txn_id" not in df.columns:
        df = df.withColumn(
            "txn_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("step"),
                    F.col("type"),
                    F.col("amount"),
                    F.col("nameOrig"),
                    F.col("nameDest"),
                ),
                256,
            ),
        )

    # 2. Step hour (0-23)
    if "step_hour" not in df.columns:
        df = df.withColumn("step_hour", (F.col("step") % 24).cast("integer"))
    else:
        df = df.withColumn("step_hour", F.col("step_hour").cast("integer"))

    # 3. Cyclical hour encodings
    two_pi = 2.0 * math.pi
    df = df.withColumn(
        "hour_sin",
        F.sin(F.col("step_hour") * (two_pi / 24.0)),
    ).withColumn(
        "hour_cos",
        F.cos(F.col("step_hour") * (two_pi / 24.0)),
    )

    # 4. Simulation day index (0 to 30)
    df = df.withColumn(
        "sim_day",
        F.floor((F.col("step") - 1) / 24).cast("integer"),
    )

    # 5. Warmup indicator (longest destination velocity window is 168 hours = 7 days)
    df = df.withColumn(
        "history_window_complete_flag",
        F.when(F.col("step") >= 169, 1).otherwise(0).cast("tinyint"),
    )

    # 6. Transaction type one-hot indicators (Fraud occurs strictly in TRANSFER & CASH_OUT)
    df = (
        df.withColumn("type_transfer", F.when(F.col("type") == "TRANSFER", 1).otherwise(0).cast("tinyint"))
        .withColumn("type_cash_out", F.when(F.col("type") == "CASH_OUT", 1).otherwise(0).cast("tinyint"))
        .withColumn("type_payment", F.when(F.col("type") == "PAYMENT", 1).otherwise(0).cast("tinyint"))
        .withColumn("type_cash_in", F.when(F.col("type") == "CASH_IN", 1).otherwise(0).cast("tinyint"))
        .withColumn("type_debit", F.when(F.col("type") == "DEBIT", 1).otherwise(0).cast("tinyint"))
    )

    return df
