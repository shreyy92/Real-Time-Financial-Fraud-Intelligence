"""
IEEE-CIS Dataset Loader for Member 2.

Loads Member 1's cleaned IEEE-CIS Parquet dataset and validates required core columns.
"""

from typing import Optional, List
import os
from pyspark.sql import SparkSession, DataFrame


IEEE_REQUIRED_COLUMNS: List[str] = [
    "TransactionID",
    "isFraud",
    "TransactionDT",
    "TransactionAmt",
    "ProductCD",
    "card1",
]


def load_ieee_data(
    spark: SparkSession,
    path: str,
    required_columns: Optional[List[str]] = None,
) -> DataFrame:
    """
    Load cleaned IEEE-CIS Parquet dataset and validate expected schema fields.

    Args:
        spark: Active SparkSession.
        path: Path to the cleaned IEEE Parquet dataset directory or file.
        required_columns: Optional custom list of required columns.

    Returns:
        DataFrame: Spark DataFrame containing IEEE-CIS transactions.

    Raises:
        FileNotFoundError: If the input path does not exist.
        ValueError: If any required columns are missing from the dataset.
    """
    if not os.path.exists(path) and not path.startswith("hdfs://") and not path.startswith("s3://"):
        raise FileNotFoundError(f"IEEE input path does not exist: {path}")

    df = spark.read.parquet(path)
    cols_to_check = required_columns or IEEE_REQUIRED_COLUMNS

    missing_cols = [col for col in cols_to_check if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"IEEE dataset at '{path}' is missing required columns: {missing_cols}. "
            f"Available columns: {df.columns[:20]}... (total {len(df.columns)})"
        )

    return df
