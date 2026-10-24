"""
Unit Test Fixtures for Member 4 Contract Verification.

NOTE: These fixtures generate minimal SYNTHETIC test-only records matching
Member 2 and Member 3's authoritative schemas, column names, data types,
and null semantics. They are intended EXCLUSIVELY for automated unit tests
and MUST NEVER be used to report model performance metrics or claim real
dataset validation.
"""

from typing import Dict, Any
import pytest
import pandas as pd
import numpy as np

from member4_ml.src.feature_selection import (
    load_config,
    get_tier_a_features,
    get_tier_ab_features,
    get_ieee_features,
)


@pytest.fixture
def model_config() -> Dict[str, Any]:
    """Provide the loaded model configuration."""
    return load_config()


@pytest.fixture
def synthetic_paysim_df(model_config) -> pd.DataFrame:
    """
    Generate synthetic PaySim records adhering 1:1 to PAYSIM_OUTPUT_SCHEMA
    plus Tier B ablation columns.
    """
    n_rows = 30
    np.random.seed(42)

    # Precomputed splits: 10 train, 10 valid, 10 test
    splits = ["train"] * 10 + ["valid"] * 10 + ["test"] * 10
    steps = [100] * 10 + [550] * 10 + [650] * 10

    tier_a_cols = model_config["paysim"]["tier_a_features"]
    tier_b_cols = model_config["paysim"]["tier_b_features"]

    data = {
        "step": steps,
        "type": ["TRANSFER", "CASH_OUT", "PAYMENT"] * 10,
        "nameOrig": [f"C{1000 + i}" for i in range(n_rows)],
        "nameDest": [f"M{2000 + (i % 5)}" for i in range(n_rows)],
        "isFraud": [0, 1, 0] * 10,
        "split": splits,
        "sim_day": [s // 24 for s in steps],
        "history_window_complete_flag": [1] * n_rows,
        "txn_id": [f"hash_{i:04d}" for i in range(n_rows)],
        "feature_version": ["v1.0"] * n_rows,
    }

    # Populate Tier A features with realistic dummy numbers and natural nulls
    for col in tier_a_cols:
        if "flag" in col or "type_" in col:
            data[col] = np.random.choice([0, 1], size=n_rows).astype(np.int8)
        elif "count" in col or "step" in col:
            data[col] = np.random.randint(0, 50, size=n_rows).astype(np.int32)
        else:
            vals = np.random.uniform(10.0, 5000.0, size=n_rows)
            # Inject cold-start natural nulls into aggregate columns
            if "dest_prior_amount_mean" in col or "dest_amount_zscore" in col:
                vals[0] = np.nan
                vals[5] = np.nan
            data[col] = vals

    # Populate Tier B features (for ablation tests)
    for col in tier_b_cols:
        if "flag" in col:
            data[col] = np.random.choice([0, 1], size=n_rows).astype(np.int8)
        else:
            data[col] = np.random.uniform(0.0, 5000.0, size=n_rows)

    return pd.DataFrame(data)


@pytest.fixture
def synthetic_ieee_df(model_config) -> pd.DataFrame:
    """
    Generate synthetic IEEE records adhering 1:1 to IEEE_OUTPUT_SCHEMA.
    """
    n_rows = 20
    np.random.seed(42)

    splits = ["train"] * 10 + ["valid"] * 10
    dts = [100000] * 10 + [12000000] * 10

    features = model_config["ieee"]["features"]

    data = {
        "TransactionID": [3000000 + i for i in range(n_rows)],
        "TransactionDT": dts,
        "isFraud": [0, 1] * 10,
        "split": splits,
        "card_proxy_id": [f"card_{i % 4}" for i in range(n_rows)],
        "card_addr_proxy_id": [f"card_addr_{i % 4}" for i in range(n_rows)],
        "day_index": [dt // 86400 for dt in dts],
        "history_window_complete_flag": [1] * n_rows,
        "feature_version": ["v1.0"] * n_rows,
    }

    for col in features:
        if col.startswith("ProductCD"):
            data[col] = np.random.choice(["W", "C", "R"], size=n_rows)
        elif col.startswith("card4"):
            data[col] = np.random.choice(["visa", "mastercard"], size=n_rows)
        elif col.startswith("card6"):
            data[col] = np.random.choice(["debit", "credit"], size=n_rows)
        elif col.startswith("P_emaildomain") or col.startswith("R_emaildomain"):
            data[col] = np.random.choice(["gmail.com", "yahoo.com", "missing"], size=n_rows)
        elif col.startswith("M") and len(col) <= 3:
            data[col] = np.random.choice(["T", "F", "missing"], size=n_rows)
        elif "flag" in col or "ind_" in col:
            data[col] = np.random.choice([0, 1], size=n_rows).astype(np.int8)
        else:
            vals = np.random.uniform(0.0, 1000.0, size=n_rows)
            # Inject native IEEE sparse nulls (~50% nulls)
            mask = np.random.rand(n_rows) > 0.5
            vals[mask] = np.nan
            data[col] = vals

    return pd.DataFrame(data)
