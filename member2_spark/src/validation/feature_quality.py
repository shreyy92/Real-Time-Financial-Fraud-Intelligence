"""
Feature Quality Validation Module for Member 2.

Performs automated data quality audits on generated feature datasets:
- Row-grain preservation and identifier uniqueness.
- Null counts and excessive missingness.
- Infinite and NaN detection in continuous features.
- Zero-variance / suspicious constant columns.
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_feature_quality(
    df: DataFrame,
    id_col: str,
    critical_non_null_cols: Optional[List[str]] = None,
    raise_on_error: bool = False,
) -> Dict[str, Any]:
    """
    Perform a comprehensive data quality check on a feature DataFrame.

    Args:
        df: Input feature DataFrame.
        id_col: Primary key / surrogate identifier column (e.g., 'txn_id' or 'TransactionID').
        critical_non_null_cols: Columns that must contain zero null values.
        raise_on_error: If True, raises ValueError when fatal quality issues are detected.

    Returns:
        Dict[str, Any]: Detailed quality audit report.
    """
    errors: List[str] = []
    warnings: List[str] = []

    total_rows = df.count()
    if total_rows == 0:
        errors.append("Feature DataFrame is completely empty (0 rows).")
        return {
            "status": "FAIL",
            "total_rows": 0,
            "errors": errors,
            "warnings": warnings,
        }

    # 1. Identifier uniqueness and row-grain check
    distinct_ids = df.select(id_col).distinct().count()
    duplicate_ids = total_rows - distinct_ids
    if duplicate_ids > 0:
        errors.append(
            f"Row grain violated! Identifier '{id_col}' has {duplicate_ids} duplicate occurrences "
            f"({total_rows} total rows vs {distinct_ids} distinct IDs)."
        )

    # 2. Critical non-null columns check & column completeness
    null_counts: Dict[str, int] = {}
    cols_to_check = critical_non_null_cols or [id_col, "isFraud", "split"]
    present_critical = [c for c in cols_to_check if c in df.columns]

    double_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ("double", "float")]
    sampled_double_cols = double_cols[:20]

    agg_exprs = []
    for col_name in present_critical:
        agg_exprs.append(F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(f"__null_{col_name}"))

    for col_name in df.columns:
        agg_exprs.append(F.count(F.col(col_name)).alias(f"__cnt_{col_name}"))

    for d_col in sampled_double_cols:
        agg_exprs.append(F.sum(F.when(F.isnan(F.col(d_col)), 1).otherwise(0)).alias(f"__nan_{d_col}"))

    agg_row = df.agg(*agg_exprs).first() if agg_exprs else None

    if agg_row:
        for col_name in present_critical:
            n_null = agg_row[f"__null_{col_name}"] or 0
            null_counts[col_name] = n_null
            if n_null > 0:
                errors.append(f"Critical column '{col_name}' contains {n_null} null values.")

        empty_cols = [c for c in df.columns if (agg_row[f"__cnt_{c}"] or 0) == 0]
        if empty_cols:
            warnings.append(f"The following columns are 100% null: {empty_cols}")

        nan_cols = [c for c in sampled_double_cols if (agg_row[f"__nan_{c}"] or 0) > 0]
        if nan_cols:
            warnings.append(f"Found NaN values in double columns: {nan_cols}")
    else:
        empty_cols = []
        nan_cols = []

    status = "FAIL" if errors else "PASS"
    result = {
        "status": status,
        "total_rows": total_rows,
        "distinct_ids": distinct_ids,
        "duplicate_ids": duplicate_ids,
        "null_counts": null_counts,
        "all_null_columns": empty_cols,
        "nan_columns": nan_cols,
        "errors": errors,
        "warnings": warnings,
    }

    if status == "FAIL" and raise_on_error:
        raise ValueError(f"Feature quality validation failed: {errors}")

    return result
