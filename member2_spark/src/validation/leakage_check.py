"""
Leakage Audit and Validation Module for Member 2.

Enforces strict anti-leakage policies across PaySim and IEEE-CIS feature pipelines:
1. Complete prohibition of target-derived features (e.g., fraud rates, step fraud counts).
2. Elimination of global volume and per-step traffic features that leak simulator state.
3. Strict classification of features into:
   - SAFE: Tier A authorization-time features and strictly past aggregates.
   - REVIEW: Columns requiring explicit ablation or domain verification.
   - LEAKAGE_RISK: Direct target dependencies or Tier B post-transaction balance arithmetic.
"""

from typing import Dict, Any, List, Set
from pyspark.sql import DataFrame

# Prohibited patterns in feature column names (direct or indirect target derived)
PROHIBITED_TARGET_PATTERNS = [
    "fraud_rate",
    "fraud_count",
    "is_fraud",
    "target_enc",
    "label_mean",
    "step_fraud",
]

# Prohibited global volume features in PaySim (per Member 1 Audit P5)
PAYSIM_PROHIBITED_VOLUME_PATTERNS = [
    "step_count",
    "step_volume",
    "global_velocity",
    "txns_per_hour",
]

# PaySim Tier B features (Post-transaction state and balance arithmetic)
PAYSIM_TIER_B_COLUMNS: Set[str] = {
    "newbalanceOrig",
    "newbalanceDest",
    "err_balance_orig",
    "err_balance_dest",
    "orig_new_balance_zero_flag",
}

# PaySim reference-only columns that must NEVER be passed as model features
PAYSIM_REFERENCE_COLUMNS: Set[str] = {
    "isFlaggedFraud",
    "nameOrig",
    "nameDest",
    "txn_id",
}

# IEEE-CIS reference/key columns that must NEVER be passed as model features
IEEE_REFERENCE_COLUMNS: Set[str] = {
    "TransactionID",
    "TransactionDT",
    "card_proxy_id",
    "card_addr_proxy_id",
}


def audit_feature_leakage(
    feature_columns: List[str],
    dataset_type: str = "paysim",
) -> Dict[str, Any]:
    """
    Audit a list of proposed feature column names for target leakage, volume leakage,
    and post-transaction state contamination.

    Args:
        feature_columns: Proposed feature columns for model training.
        dataset_type: 'paysim' or 'ieee'.

    Returns:
        Dict[str, Any]: Audit report classifying columns into SAFE, REVIEW, and LEAKAGE_RISK.
    """
    dataset_lower = dataset_type.lower()
    safe_cols: List[str] = []
    review_cols: List[str] = []
    risk_cols: List[str] = []
    reasons: Dict[str, str] = {}

    for col in feature_columns:
        col_lower = col.lower()

        # 1. Check for target leakage
        if col == "isFraud" or any(p in col_lower for p in PROHIBITED_TARGET_PATTERNS):
            risk_cols.append(col)
            reasons[col] = "FATAL: Direct or indirect target label dependency."
            continue

        # 2. Check for PaySim-specific volume leakage
        if dataset_lower == "paysim":
            if any(p in col_lower for p in PAYSIM_PROHIBITED_VOLUME_PATTERNS):
                risk_cols.append(col)
                reasons[col] = "HIGH RISK: Global per-step volume acts as simulator label leak."
                continue
            if col in PAYSIM_TIER_B_COLUMNS:
                risk_cols.append(col)
                reasons[col] = "TIER B: Uses post-transaction balance outcome state. Requires isolated ablation."
                continue
            if col in PAYSIM_REFERENCE_COLUMNS:
                review_cols.append(col)
                reasons[col] = "REVIEW: Raw identifier or rule flag. Recommended for exclusion from ML inputs."
                continue

        # 3. Check for IEEE-specific reference keys
        if dataset_lower == "ieee":
            if col in IEEE_REFERENCE_COLUMNS:
                review_cols.append(col)
                reasons[col] = "REVIEW: Entity key or monotone timestamp. Recommended for exclusion from ML inputs."
                continue
            if col in ["D8", "D9", "id_02", "id_07", "id_08", "id_22", "id_26"]:
                review_cols.append(col)
                reasons[col] = "REVIEW: Masked column with unexplained identity-coupling or near-100% null rate."
                continue

        safe_cols.append(col)

    status = "FAIL" if any("FATAL" in r for r in reasons.values()) else "PASS"

    return {
        "dataset": dataset_type,
        "status": status,
        "safe_count": len(safe_cols),
        "review_count": len(review_cols),
        "leakage_risk_count": len(risk_cols),
        "safe_features": safe_cols,
        "review_features": review_cols,
        "leakage_risk_features": risk_cols,
        "reasons": reasons,
    }


def validate_no_target_leakage(
    df: DataFrame,
    feature_columns: List[str],
    target_col: str = "isFraud",
    raise_on_error: bool = True,
) -> bool:
    """
    Validate that the target column is not present among ML feature inputs.

    Args:
        df: Input DataFrame.
        feature_columns: List of columns intended for feature matrix.
        target_col: Name of target column. Default 'isFraud'.
        raise_on_error: If True, raises ValueError if target is found in feature_columns.

    Returns:
        bool: True if safe, False if leakage detected.
    """
    if target_col in feature_columns:
        msg = f"CRITICAL LEAKAGE: Target column '{target_col}' found in feature columns list!"
        if raise_on_error:
            raise ValueError(msg)
        return False
    return True
