"""
PaySim Dataset Loader for Member 2.

Loads Member 1's cleaned PaySim Parquet dataset and performs initial schema validation.
"""

from typing import Optional, List
import os
from pyspark.sql import SparkSession, DataFrame


PAYSIM_REQUIRED_COLUMNS: List[str] = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
]


def load_paysim_data(
    spark: SparkSession,
    path: str,
    required_columns: Optional[List[str]] = None,
) -> DataFrame:
    """
    Load cleaned PaySim Parquet dataset and validate expected columns.

    Args:
        spark: Active SparkSession.
        path: Path to the cleaned PaySim Parquet dataset directory or file.
        required_columns: Optional custom list of required columns.

    Returns:
        DataFrame: Spark DataFrame containing PaySim transactions.

    Raises:
        FileNotFoundError: If the input path does not exist.
        ValueError: If any required columns are missing from the dataset.
    """
    if not os.path.exists(path) and not path.startswith("hdfs://") and not path.startswith("s3://"):
        raise FileNotFoundError(f"PaySim input path does not exist: {path}")

    df = spark.read.parquet(path)
    cols_to_check = required_columns or PAYSIM_REQUIRED_COLUMNS

    missing_cols = [col for col in cols_to_check if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"PaySim dataset at '{path}' is missing required columns: {missing_cols}. "
            f"Available columns: {df.columns}"
        )

    return df
