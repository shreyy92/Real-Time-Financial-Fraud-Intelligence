"""
Schema Validation Module for Member 2 Feature Engineering.

Validates input contracts from Member 1 and output contracts for downstream consumers (Member 3 & Member 4).
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame

PAYSIM_INPUT_REQUIRED = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg",
    "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud"
]

PAYSIM_OUTPUT_REQUIRED = [
    "txn_id", "step", "nameOrig", "nameDest", "isFraud", "split",
    "amount", "amount_log1p", "step_hour", "hour_sin", "hour_cos",
    "dest_prior_txn_count", "dest_first_seen_flag", "feature_version"
]

IEEE_INPUT_REQUIRED = [
    "TransactionID", "isFraud", "TransactionDT", "TransactionAmt", "ProductCD", "card1"
]

IEEE_OUTPUT_REQUIRED = [
    "TransactionID", "isFraud", "TransactionDT", "split", "card_proxy_id",
    "hour_of_day", "hour_sin", "hour_cos", "amt_log1p",
    "card_prior_txn_count", "card_first_seen_flag", "feature_version"
]


def validate_input_schema(
    df: DataFrame,
    dataset_type: str,
    raise_on_error: bool = False,
) -> Dict[str, Any]:
    """
    Validate input DataFrame against Member 1 contract schema.

    Args:
        df: Input Spark DataFrame.
        dataset_type: 'paysim' or 'ieee'.
        raise_on_error: If True, raises ValueError upon failure. Default is False.

    Returns:
        Dict[str, Any]: Validation summary report with status, missing columns, errors, and warnings.
    """
    dataset_lower = dataset_type.lower()
    if dataset_lower == "paysim":
        expected_cols = PAYSIM_INPUT_REQUIRED
    elif dataset_lower == "ieee":
        expected_cols = IEEE_INPUT_REQUIRED
    else:
        raise ValueError(f"Unknown dataset_type: {dataset_type}. Must be 'paysim' or 'ieee'.")

    missing = [col for col in expected_cols if col not in df.columns]
    errors = []
    warnings = []

    if missing:
        errors.append(f"Missing required input columns: {missing}")

    # Check for target column
    if "isFraud" not in df.columns:
        errors.append("Target column 'isFraud' is missing.")

    status = "FAIL" if errors else "PASS"
    result = {
        "dataset": dataset_type,
        "validation_type": "input_schema",
        "status": status,
        "column_count": len(df.columns),
        "missing_columns": missing,
        "errors": errors,
        "warnings": warnings,
    }

    if status == "FAIL" and raise_on_error:
        raise ValueError(f"Input schema validation failed for {dataset_type}: {errors}")

    return result


def validate_output_schema(
    df: DataFrame,
    dataset_type: str,
    raise_on_error: bool = False,
) -> Dict[str, Any]:
    """
    Validate output DataFrame against Member 2 ML-ready contract.

    Args:
        df: Output Spark DataFrame.
        dataset_type: 'paysim' or 'ieee'.
        raise_on_error: If True, raises ValueError upon failure. Default is False.

    Returns:
        Dict[str, Any]: Validation summary report with status, missing columns, errors, and warnings.
    """
    dataset_lower = dataset_type.lower()
    if dataset_lower == "paysim":
        expected_cols = PAYSIM_OUTPUT_REQUIRED
    elif dataset_lower == "ieee":
        expected_cols = IEEE_OUTPUT_REQUIRED
    else:
        raise ValueError(f"Unknown dataset_type: {dataset_type}. Must be 'paysim' or 'ieee'.")

    missing = [col for col in expected_cols if col not in df.columns]
    errors = []
    warnings = []

    if missing:
        errors.append(f"Missing required output contract columns: {missing}")

    if "split" not in df.columns:
        errors.append("Split assignment column 'split' is missing.")
    if "feature_version" not in df.columns:
        errors.append("Version indicator 'feature_version' is missing.")

    status = "FAIL" if errors else "PASS"
    result = {
        "dataset": dataset_type,
        "validation_type": "output_schema",
        "status": status,
        "column_count": len(df.columns),
        "missing_columns": missing,
        "errors": errors,
        "warnings": warnings,
    }

    if status == "FAIL" and raise_on_error:
        raise ValueError(f"Output schema validation failed for {dataset_type}: {errors}")

    return result
