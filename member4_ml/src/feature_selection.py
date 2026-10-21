"""
Authoritative Feature Selection and Column Alignment for Member 4 ML Models.

Ensures:
1. Zero target leakage: 'isFraud' and prohibited statistics are NEVER included in X.
2. Zero temporal leakage: split assignments, timestamps, and keys are filtered out.
3. Strict Tier A isolation: post-transaction balance deltas (Tier B) are kept separate
   and used only for ablation studies.
4. Stable, deterministic feature ordering between training and real-time streaming inference.
"""

from typing import List, Tuple, Dict, Any, Optional
import os
import yaml
import pandas as pd


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load model_config.yaml from default or specified path."""
    if config_path is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base_dir, "config", "model_config.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_tier_a_features(config: Optional[Dict[str, Any]] = None) -> List[str]:
    """Return the authoritative list of 32 PaySim Tier A (authorization-time) features."""
    if config is None:
        config = load_config()
    return list(config["paysim"]["tier_a_features"])


def get_tier_ab_features(config: Optional[Dict[str, Any]] = None) -> List[str]:
    """Return PaySim Tier A + Tier B (post-transaction simulator state) features for ablation."""
    if config is None:
        config = load_config()
    tier_a = config["paysim"]["tier_a_features"]
    tier_b = config["paysim"]["tier_b_features"]
    return list(tier_a) + list(tier_b)


def get_ieee_features(config: Optional[Dict[str, Any]] = None) -> List[str]:
    """Return the authoritative list of 474 IEEE-CIS model features."""
    if config is None:
        config = load_config()
    return list(config["ieee"]["features"])


def select_features(
    df: pd.DataFrame,
    dataset: str,
    include_tier_b: bool = False,
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, Optional[pd.Series]]:
    """
    Extract the feature matrix X and ground-truth target y from a dataset DataFrame.

    Enforces:
    - Feature columns match the exact authoritative contract.
    - Column ordering is preserved deterministically.
    - Identification columns ('txn_id', 'TransactionID', etc.) and split columns are removed.
    - Target label ('isFraud') is separated and NEVER included in X.
    - If 'isFraud' is not present (e.g. at live streaming inference time), y is returned as None.
    - Natural missing values (NaN/None) are strictly preserved without arbitrary imputation.

    Parameters
    ----------
    df : pd.DataFrame
        Input transaction or feature records.
    dataset : str
        Either 'paysim' or 'ieee'.
    include_tier_b : bool
        If True (PaySim only), includes Tier B simulator balance deltas for ablation.
    config : Optional[Dict[str, Any]]
        Optional loaded configuration dict.

    Returns
    -------
    X : pd.DataFrame
        Feature matrix aligned to the authoritative feature list.
    y : Optional[pd.Series]
        Target series 'isFraud', or None if absent.
    """
    if config is None:
        config = load_config()

    dataset = dataset.lower()
    if dataset not in ("paysim", "ieee"):
        raise ValueError(f"Unknown dataset '{dataset}'. Must be 'paysim' or 'ieee'.")

    ds_config = config[dataset]
    target_col = ds_config["target_col"]

    # 1. Extract target if available
    y = None
    if target_col in df.columns:
        y = df[target_col].copy()
        # Validate binary target labels
        non_null_y = y.dropna()
        if not non_null_y.isin([0, 1]).all():
            invalid_vals = set(non_null_y.unique()) - {0, 1}
            raise ValueError(f"Invalid target values found in '{target_col}': {invalid_vals}. Must be in {{0, 1}}.")

    # 2. Determine target feature column list
    if dataset == "paysim":
        if include_tier_b:
            expected_features = get_tier_ab_features(config)
        else:
            expected_features = get_tier_a_features(config)
    else:
        expected_features = get_ieee_features(config)

    # 3. Check for missing feature columns in input DataFrame
    missing_cols = [c for c in expected_features if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Input DataFrame is missing {len(missing_cols)} required feature columns for {dataset.upper()}: "
            f"{missing_cols[:5]}..."
        )

    # 4. Strict Column Ordering and Leakage Defense
    # Construct X strictly using the ordered expected_features list
    X = df[expected_features].copy()

    # Assert target is absent from X
    assert target_col not in X.columns, f"CRITICAL: Target column '{target_col}' leaked into feature matrix X!"

    # Assert primary key is absent from X
    pk = ds_config["primary_key"]
    assert pk not in X.columns, f"CRITICAL: Primary key '{pk}' leaked into feature matrix X!"

    return X, y
