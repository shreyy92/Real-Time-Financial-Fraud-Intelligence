"""
IEEE-CIS Temporal Feature Engineering.

Extracts relative diurnal, cyclical, and day-of-week patterns from 'TransactionDT'.

IMPORTANT ARCHITECTURAL RULE:
'TransactionDT' measures seconds from an undisclosed reference instant. Features
are strictly relative (e.g., relative hour, relative weekday) and must not be
interpreted as calendar-anchored UTC timestamps.
Global transaction interval features across all rows are prohibited to prevent volume leakage.
"""

import math
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_ieee_temporal_features(df: DataFrame) -> DataFrame:
    """
    Compute relative diurnal, cyclical, and multi-day temporal indicators from TransactionDT.

    Features derived:
        - hour_of_day: Relative hour within a 24-hour cycle (floor(TransactionDT / 3600) % 24).
          (Audit findings: fraud rates vary significantly by relative hour, e.g., 2.4% to 10.1%).
        - hour_sin / hour_cos: Trigonometric cyclical transformation of hour_of_day.
        - day_index: Relative day number (floor(TransactionDT / 86400)).
        - day_of_week_rel: Relative 7-day cyclical weekday proxy (day_index % 7).
        - history_window_complete_flag: 1 if day_index >= 7 (completes 7-day velocity warmup).

    Args:
        df: Input IEEE-CIS DataFrame containing 'TransactionDT'.

    Returns:
        DataFrame: Augmented DataFrame with relative temporal features.
    """
    two_pi = 2.0 * math.pi

    df = (
        df.withColumn(
            "hour_of_day",
            (F.floor(F.col("TransactionDT") / 3600) % 24).cast("integer"),
        )
        .withColumn(
            "hour_sin",
            F.sin(F.col("hour_of_day") * (two_pi / 24.0)),
        )
        .withColumn(
            "hour_cos",
            F.cos(F.col("hour_of_day") * (two_pi / 24.0)),
        )
        .withColumn(
            "day_index",
            F.floor(F.col("TransactionDT") / 86400).cast("integer"),
        )
        .withColumn(
            "day_of_week_rel",
            (F.col("day_index") % 7).cast("integer"),
        )
        .withColumn(
            "history_window_complete_flag",
            F.when(F.col("day_index") >= 7, 1).otherwise(0).cast("tinyint"),
        )
    )

    return df
