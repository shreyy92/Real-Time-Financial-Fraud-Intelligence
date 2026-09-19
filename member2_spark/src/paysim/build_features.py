"""
PaySim Complete Feature Engineering Pipeline Runner.

Orchestrates data loading, time-based split assignment, temporal encoding,
Tier A and Tier B feature extraction, destination behavior and velocity calculation,
row-grain validation, and Parquet persistence.
"""

from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.paysim.load import load_paysim_data
from src.paysim.temporal_features import add_paysim_temporal_features
from src.paysim.amount_features import add_paysim_amount_features
from src.paysim.destination_features import add_paysim_destination_features
from src.paysim.velocity_features import add_paysim_velocity_features


def build_paysim_features(
    spark: SparkSession,
    input_path: Optional[str] = None,
    df: Optional[DataFrame] = None,
    output_path: Optional[str] = None,
    include_tier_b: bool = True,
    train_max_step: int = 500,
    valid_max_step: int = 600,
    feature_version: str = "v1.0",
    num_output_partitions: int = 8,
) -> DataFrame:
    """
    Execute the end-to-end feature pipeline for PaySim.

    Args:
        spark: Active SparkSession.
        input_path: Path to cleaned PaySim Parquet file/directory. Required if df is None.
        df: Optional existing DataFrame (e.g., for unit testing with synthetic data).
        output_path: Optional destination directory to persist features as Parquet.
        include_tier_b: Whether to include post-transaction Tier B features. Default True.
        train_max_step: Maximum step value for training split. Default 500.
        valid_max_step: Maximum step value for validation split. Default 600.
        feature_version: Version identifier for feature contract. Default 'v1.0'.
        num_output_partitions: Partition count for output Parquet. Default 8.

    Returns:
        DataFrame: ML-ready PaySim feature DataFrame at exact 1-transaction-to-1-row grain.

    Raises:
        ValueError: If neither input_path nor df is supplied, or if row grain is violated.
    """
    if df is None:
        if input_path is None:
            raise ValueError("Either 'input_path' or 'df' must be provided.")
        df = load_paysim_data(spark, input_path)

    initial_row_count = df.count()

    # 1. Temporal, cyclical, surrogate keys, and type one-hots
    df_features = add_paysim_temporal_features(df)

    # 2. Time-based split assignment (prevents future leakage)
    df_features = df_features.withColumn(
        "split",
        F.when(F.col("step") <= train_max_step, "train")
        .when(F.col("step") <= valid_max_step, "valid")
        .otherwise("test"),
    )

    # 3. Amount and balance relationships (Tier A and Tier B)
    df_features = add_paysim_amount_features(df_features, include_tier_b=include_tier_b)

    # 4. Strictly historical destination metrics
    df_features = add_paysim_destination_features(df_features)

    # 5. Destination velocity rolling windows (24h and 168h)
    df_features = add_paysim_velocity_features(df_features)

    # 6. Tag feature version
    df_features = df_features.withColumn("feature_version", F.lit(feature_version))

    # 7. Write Parquet if destination path is specified, then verify row-grain integrity
    if output_path:
        (
            df_features.repartition(num_output_partitions)
            .write.mode("overwrite")
            .parquet(output_path)
        )
        df_persisted = spark.read.parquet(output_path)
        final_row_count = df_persisted.count()
        if initial_row_count != final_row_count:
            raise ValueError(
                f"Row grain violated in PaySim pipeline! "
                f"Input rows: {initial_row_count}, Output rows: {final_row_count}"
            )
        return df_persisted

    final_row_count = df_features.count()
    if initial_row_count != final_row_count:
        raise ValueError(
            f"Row grain violated in PaySim pipeline! "
            f"Input rows: {initial_row_count}, Output rows: {final_row_count}"
        )

    return df_features
