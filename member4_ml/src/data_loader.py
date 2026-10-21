"""
Contract-Enforcing Data Loader for Member 4 Machine Learning.

Handles:
1. Loading batch Parquet feature datasets produced by Member 2.
2. Enforcing precomputed temporal split partitions ('train', 'valid', 'test').
3. Raising clear, actionable errors when files are missing (no silent fallback).
4. Validating row schemas against contract expectations.
"""

from typing import Tuple, Optional, Dict, Any
import os
import yaml
import pandas as pd

from member4_ml.src.feature_selection import load_config, select_features


def validate_raw_schema(df: pd.DataFrame, dataset: str, config: Optional[Dict[str, Any]] = None) -> None:
    """
    Validate that an input DataFrame contains all required contract columns.

    Parameters
    ----------
    df : pd.DataFrame
        Input data.
    dataset : str
        'paysim' or 'ieee'.
    config : Optional[Dict[str, Any]]
        Optional loaded configuration.
    """
    if config is None:
        config = load_config()

    dataset = dataset.lower()
    if dataset not in ("paysim", "ieee"):
        raise ValueError(f"Unknown dataset '{dataset}'. Must be 'paysim' or 'ieee'.")

    ds_config = config[dataset]
    target_col = ds_config["target_col"]
    primary_key = ds_config["primary_key"]
    split_col = ds_config["split_col"]

    # Required core columns
    required_core = [primary_key, target_col, split_col]
    missing_core = [c for c in required_core if c not in df.columns]
    if missing_core:
        raise ValueError(f"{dataset.upper()} input is missing core contract columns: {missing_core}")

    # Check split values
    valid_splits = set(ds_config["valid_splits"])
    actual_splits = set(df[split_col].dropna().unique())
    invalid_splits = actual_splits - valid_splits
    if invalid_splits:
        raise ValueError(
            f"Invalid split value(s) in '{split_col}': {invalid_splits}. "
            f"Allowed for {dataset.upper()}: {valid_splits}"
        )


def load_dataset_split(
    dataset: str,
    split: str,
    data_path: Optional[str] = None,
    include_tier_b: bool = False,
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Load a precomputed temporal split for a dataset and return (X, y).

    Parameters
    ----------
    dataset : str
        'paysim' or 'ieee'.
    split : str
        Target split partition ('train', 'valid', or 'test' for PaySim; 'train', 'valid' for IEEE).
    data_path : Optional[str]
        Optional path to Parquet dataset. If None, uses path configured in model_config.yaml.
    include_tier_b : bool
        If True, loads Tier B simulator post-balances for PaySim ablation.
    config : Optional[Dict[str, Any]]
        Optional loaded config dict.

    Returns
    -------
    X : pd.DataFrame
        Clean feature matrix with zero target/temporal leakage.
    y : pd.Series
        Binary target ground-truth label series ('isFraud').

    Raises
    -------
    FileNotFoundError
        If the Parquet file does not exist at data_path.
    ValueError
        If requested split is not valid for the dataset or schema check fails.
    """
    if config is None:
        config = load_config()

    dataset = dataset.lower()
    if dataset not in ("paysim", "ieee"):
        raise ValueError(f"Unknown dataset '{dataset}'. Must be 'paysim' or 'ieee'.")

    ds_config = config[dataset]
    split = split.lower()
    valid_splits = ds_config["valid_splits"]
    if split not in valid_splits:
        raise ValueError(
            f"Requested split '{split}' is invalid for {dataset.upper()}. "
            f"Allowed splits are: {valid_splits}"
        )

    if data_path is None:
        data_path = ds_config["batch_path"]

    if not os.path.exists(data_path):
        raise FileNotFoundError(
            f"Batch feature Parquet dataset not found at '{data_path}'.\n"
            f"Member 2 must provide '{data_path}' or link the data directory using:\n"
            f"  cmd /c mklink /J data <path_to_data>"
        )

    # Read using PyArrow engine
    split_col = ds_config["split_col"]
    df = pd.read_parquet(data_path, engine="pyarrow")

    validate_raw_schema(df, dataset, config)

    # Filter strictly by precomputed split
    split_mask = df[split_col] == split
    df_split = df[split_mask].copy()

    if len(df_split) == 0:
        raise ValueError(f"No records found for split '{split}' in dataset '{data_path}'.")

    X, y = select_features(df_split, dataset=dataset, include_tier_b=include_tier_b, config=config)

    if y is None:
        raise ValueError(f"Target column '{ds_config['target_col']}' is missing from {split} split!")

    return X, y
