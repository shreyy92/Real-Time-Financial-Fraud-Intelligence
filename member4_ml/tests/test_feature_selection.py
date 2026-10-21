"""
Unit tests for Member 4 Feature Selection and Anti-Leakage Controls.
"""

import pytest
import pandas as pd
import numpy as np

from member4_ml.src.feature_selection import (
    select_features,
    get_tier_a_features,
    get_tier_ab_features,
    get_ieee_features,
)


def test_paysim_tier_a_counts_and_leakage_guards(synthetic_paysim_df):
    """Verify that PaySim Tier A extracts exactly 32 features and strictly excludes keys/labels/Tier B."""
    tier_a_expected = get_tier_a_features()
    assert len(tier_a_expected) == 32

    X, y = select_features(synthetic_paysim_df, dataset="paysim", include_tier_b=False)

    # 1. Feature count and shape
    assert X.shape[1] == 32
    assert list(X.columns) == tier_a_expected
    assert len(y) == len(synthetic_paysim_df)

    # 2. Strict exclusion of identifiers and target
    assert "isFraud" not in X.columns
    assert "txn_id" not in X.columns
    assert "nameOrig" not in X.columns
    assert "nameDest" not in X.columns
    assert "step" not in X.columns
    assert "split" not in X.columns
    assert "sim_day" not in X.columns
    assert "feature_version" not in X.columns

    # 3. Strict exclusion of Tier B post-transaction balance deltas
    assert "newbalanceOrig" not in X.columns
    assert "newbalanceDest" not in X.columns
    assert "err_balance_orig" not in X.columns
    assert "err_balance_dest" not in X.columns
    assert "orig_new_balance_zero_flag" not in X.columns


def test_paysim_tier_ab_ablation_selection(synthetic_paysim_df):
    """Verify that PaySim Tier A+B extracts 37 columns (32 Tier A + 5 Tier B) for ablation."""
    tier_ab_expected = get_tier_ab_features()
    assert len(tier_ab_expected) == 37

    X, y = select_features(synthetic_paysim_df, dataset="paysim", include_tier_b=True)
    assert X.shape[1] == 37
    assert list(X.columns) == tier_ab_expected

    # Tier B features must now be present
    assert "newbalanceOrig" in X.columns
    assert "err_balance_orig" in X.columns

    # Target and identifier must still NEVER leak
    assert "isFraud" not in X.columns
    assert "txn_id" not in X.columns


def test_ieee_feature_counts_and_leakage_guards(synthetic_ieee_df):
    """Verify IEEE feature extraction matches the authoritative 474 feature contract."""
    ieee_expected = get_ieee_features()
    assert len(ieee_expected) == 474

    X, y = select_features(synthetic_ieee_df, dataset="ieee")

    assert X.shape[1] == 474
    assert list(X.columns) == ieee_expected

    # Non-leakage assertions
    assert "isFraud" not in X.columns
    assert "TransactionID" not in X.columns
    assert "TransactionDT" not in X.columns
    assert "split" not in X.columns
    assert "card_proxy_id" not in X.columns
    assert "card_addr_proxy_id" not in X.columns


def test_feature_ordering_consistency(synthetic_paysim_df):
    """Verify feature ordering is deterministic even if input DataFrame columns are permuted."""
    shuffled_cols = list(synthetic_paysim_df.columns)
    np.random.seed(123)
    np.random.shuffle(shuffled_cols)
    shuffled_df = synthetic_paysim_df[shuffled_cols]

    X1, _ = select_features(synthetic_paysim_df, dataset="paysim")
    X2, _ = select_features(shuffled_df, dataset="paysim")

    # Columns must be in exact identical order
    assert list(X1.columns) == list(X2.columns)
    assert (X1.values == X2.values).all() or (np.isnan(X1.values) == np.isnan(X2.values)).all()


def test_missing_feature_column_raises_error(synthetic_paysim_df):
    """Verify that missing a required feature column raises a clear ValueError."""
    bad_df = synthetic_paysim_df.drop(columns=["amount_log1p"])
    with pytest.raises(ValueError, match="missing 1 required feature columns"):
        select_features(bad_df, dataset="paysim")


def test_invalid_target_values_raises_error(synthetic_paysim_df):
    """Verify that non-binary target labels raise ValueError."""
    bad_df = synthetic_paysim_df.copy()
    bad_df.loc[0, "isFraud"] = 2
    with pytest.raises(ValueError, match="Invalid target values found in 'isFraud'"):
        select_features(bad_df, dataset="paysim")


def test_null_value_preservation(synthetic_paysim_df):
    """Verify that natural cold-start nulls are preserved as NaN and not arbitrarily filled."""
    assert synthetic_paysim_df["dest_prior_amount_mean"].isna().any()
    X, _ = select_features(synthetic_paysim_df, dataset="paysim")
    assert X["dest_prior_amount_mean"].isna().any()
