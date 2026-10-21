"""
Unit tests for Member 4 Data Loader and Split Validation.
"""

import os
import tempfile
import pytest
import pandas as pd

from member4_ml.src.data_loader import load_dataset_split, validate_raw_schema


def test_validate_raw_schema_success(synthetic_paysim_df, synthetic_ieee_df):
    """Verify raw schema validation passes on valid schema fixtures."""
    validate_raw_schema(synthetic_paysim_df, dataset="paysim")
    validate_raw_schema(synthetic_ieee_df, dataset="ieee")


def test_validate_raw_schema_missing_primary_key(synthetic_paysim_df):
    """Verify validation fails if primary key is missing."""
    bad_df = synthetic_paysim_df.drop(columns=["txn_id"])
    with pytest.raises(ValueError, match="missing core contract columns"):
        validate_raw_schema(bad_df, dataset="paysim")


def test_validate_raw_schema_invalid_split_values(synthetic_paysim_df):
    """Verify validation fails if split contains random or unrecognized values."""
    bad_df = synthetic_paysim_df.copy()
    bad_df.loc[0, "split"] = "holdout_random"
    with pytest.raises(ValueError, match="Invalid split value"):
        validate_raw_schema(bad_df, dataset="paysim")


def test_load_dataset_split_nonexistent_file_raises_error():
    """Verify that a missing Parquet file raises a clear, actionable FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="Batch feature Parquet dataset not found"):
        load_dataset_split(dataset="paysim", split="train", data_path="non_existent/path.parquet")


def test_load_dataset_split_invalid_split_name():
    """Verify requesting an unsupported split name fails immediately."""
    with pytest.raises(ValueError, match="Requested split 'test' is invalid for IEEE"):
        load_dataset_split(dataset="ieee", split="test")


def test_load_dataset_split_from_parquet(synthetic_paysim_df):
    """Verify end-to-end split extraction from a persisted Parquet file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_parquet = os.path.join(tmpdir, "paysim_test.parquet")
        synthetic_paysim_df.to_parquet(tmp_parquet, engine="pyarrow")

        # Load 'train' split (10 rows)
        X_train, y_train = load_dataset_split(
            dataset="paysim",
            split="train",
            data_path=tmp_parquet,
            include_tier_b=False,
        )
        assert len(X_train) == 10
        assert len(y_train) == 10
        assert X_train.shape[1] == 32
        assert "isFraud" not in X_train.columns
        assert "txn_id" not in X_train.columns

        # Load 'test' split (10 rows)
        X_test, y_test = load_dataset_split(
            dataset="paysim",
            split="test",
            data_path=tmp_parquet,
            include_tier_b=False,
        )
        assert len(X_test) == 10
        assert len(y_test) == 10
