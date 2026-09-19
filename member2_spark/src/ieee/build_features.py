"""
IEEE-CIS Complete Feature Engineering Pipeline Runner.

Orchestrates data loading, time-based split assignment, missingness preservation,
card proxy entity generation, temporal encoding, masked feature aggregation,
historical card behavioral features, row-grain protection, and Parquet persistence.
"""

from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.ieee.load import load_ieee_data
from src.ieee.missingness_features import add_ieee_missingness_features
from src.ieee.card_features import add_ieee_card_features
from src.ieee.temporal_features import add_ieee_temporal_features
from src.ieee.aggregate_features import (
    add_ieee_amount_features,
    add_ieee_masked_aggregates,
    add_ieee_card_history_features,
)

# 14 Low-information near-constant columns marked for deletion in Member 1 Audit Part 4.2 & 9.2
UNINFORMATIVE_NEAR_CONSTANTS: List[str] = [
    "V1", "V14", "V27", "V28", "V41", "V65", "V68",
    "V88", "V89", "V107", "V240", "V241", "V305", "id_27",
]


def build_ieee_features(
    spark: SparkSession,
    input_path: Optional[str] = None,
    df: Optional[DataFrame] = None,
    output_path: Optional[str] = None,
    train_max_day: float = 130.0,
    drop_uninformative: bool = True,
    feature_version: str = "v1.0",
    num_output_partitions: int = 4,
) -> DataFrame:
    """
    Execute the end-to-end feature pipeline for IEEE-CIS.

    Args:
        spark: Active SparkSession.
        input_path: Path to cleaned IEEE Parquet dataset directory. Required if df is None.
        df: Optional existing DataFrame (e.g., for unit testing with synthetic data).
        output_path: Optional destination directory to persist features as Parquet.
        train_max_day: Day cutoff for time-based split (Day 1-130 train, Day 131-183 valid).
        drop_uninformative: Whether to prune the 14 uninformative near-constant columns.
        feature_version: Version identifier for feature contract. Default 'v1.0'.
        num_output_partitions: Partition count for output Parquet. Default 4.

    Returns:
        DataFrame: ML-ready IEEE feature DataFrame at exact 1-transaction-to-1-row grain.

    Raises:
        ValueError: If neither input_path nor df is supplied, or if row grain is violated.
    """
    if df is None:
        if input_path is None:
            raise ValueError("Either 'input_path' or 'df' must be provided.")
        df = load_ieee_data(spark, input_path)

    initial_row_count = df.count()

    # 1. Missingness & identity completeness features (never drops rows)
    df_features = add_ieee_missingness_features(df)

    # 2. Card proxy entity generation (card_proxy_id, card_addr_proxy_id)
    df_features = add_ieee_card_features(df_features)

    # 3. Relative temporal features (hour, sin/cos, day index, weekday)
    df_features = add_ieee_temporal_features(df_features)

    # 4. Time-based split assignment (Day 130 cutoff maintains ~3.5% prevalence in both splits)
    train_max_seconds = train_max_day * 86400.0
    df_features = df_features.withColumn(
        "split",
        F.when(F.col("TransactionDT") <= train_max_seconds, "train").otherwise("valid"),
    )

    # 5. Amount features and currency decimal fingerprints
    df_features = add_ieee_amount_features(df_features)

    # 6. Masked column aggregates (M, C, D)
    df_features = add_ieee_masked_aggregates(df_features)

    # 7. Strictly historical card proxy metrics and velocity windows
    df_features = add_ieee_card_history_features(df_features)

    # 8. Prune the 14 low-information near-constant columns if requested
    if drop_uninformative:
        cols_to_drop = [c for c in UNINFORMATIVE_NEAR_CONSTANTS if c in df_features.columns]
        if cols_to_drop:
            df_features = df_features.drop(*cols_to_drop)

    # 9. Tag feature version
    df_features = df_features.withColumn("feature_version", F.lit(feature_version))

    # 10. Write Parquet if destination path is specified, then verify row-grain integrity
    if output_path:
        (
            df_features.coalesce(num_output_partitions)
            .write.mode("overwrite")
            .parquet(output_path)
        )
        df_persisted = spark.read.parquet(output_path)
        final_row_count = df_persisted.count()
        if initial_row_count != final_row_count:
            raise ValueError(
                f"Row grain violated in IEEE pipeline! "
                f"Input rows: {initial_row_count}, Output rows: {final_row_count}"
            )
        return df_persisted

    final_row_count = df_features.count()
    if initial_row_count != final_row_count:
        raise ValueError(
            f"Row grain violated in IEEE pipeline! "
            f"Input rows: {initial_row_count}, Output rows: {final_row_count}"
        )

    return df_features
