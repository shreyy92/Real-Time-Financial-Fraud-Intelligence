"""
Unit tests for PaySim feature engineering and anti-leakage controls.
Uses small, deterministic in-memory Spark DataFrames (no external files required).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

from src.spark_session import create_spark_session
from src.paysim.temporal_features import add_paysim_temporal_features
from src.paysim.amount_features import add_paysim_amount_features
from src.paysim.destination_features import add_paysim_destination_features
from src.paysim.velocity_features import add_paysim_velocity_features
from src.paysim.build_features import build_paysim_features


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session(
        app_name="Test-PaySimFeatures",
        master="local[1]",
        config_overrides={"spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"},
    )
    yield session
    session.stop()


@pytest.fixture
def sample_paysim_df(spark: SparkSession):
    schema = StructType([
        StructField("step", IntegerType(), False),
        StructField("type", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("nameOrig", StringType(), False),
        StructField("oldbalanceOrg", DoubleType(), False),
        StructField("newbalanceOrig", DoubleType(), False),
        StructField("nameDest", StringType(), False),
        StructField("oldbalanceDest", DoubleType(), False),
        StructField("newbalanceDest", DoubleType(), False),
        StructField("isFraud", IntegerType(), False),
    ])

    # 8 deterministic test transactions
    data = [
        # Dest_A: step 1, 5, 10, 10, 20
        (1, "TRANSFER", 1000.0, "Orig_1", 1000.0, 0.0, "Dest_A", 0.0, 1000.0, 1),      # Perfect account drain
        (5, "CASH_OUT", 2000.0, "Orig_2", 5000.0, 3000.0, "Dest_A", 1000.0, 3000.0, 0),
        (10, "TRANSFER", 3000.0, "Orig_3", 3000.0, 0.0, "Dest_A", 3000.0, 6000.0, 1),
        (10, "CASH_OUT", 4000.0, "Orig_4", 10000.0, 6000.0, "Dest_A", 6000.0, 10000.0, 0), # Same-step peer
        (20, "TRANSFER", 5000.0, "Orig_5", 5000.0, 0.0, "Dest_A", 10000.0, 15000.0, 0),
        # Dest_B: step 2, 25
        (2, "PAYMENT", 50.0, "Orig_6", 200.0, 150.0, "M_Dest_B", 0.0, 0.0, 0),         # Merchant, 0 balances
        (25, "PAYMENT", 75.0, "Orig_7", 300.0, 225.0, "M_Dest_B", 0.0, 0.0, 0),
        # Rare zero amount
        (30, "CASH_OUT", 0.0, "Orig_8", 0.0, 0.0, "Dest_C", 0.0, 0.0, 1),
    ]

    return spark.createDataFrame(data, schema)


def test_paysim_temporal_features(sample_paysim_df):
    """Test temporal features, txn_id derivation, and cyclical encodings."""
    df_temp = add_paysim_temporal_features(sample_paysim_df)

    assert "txn_id" in df_temp.columns
    assert "step_hour" in df_temp.columns
    assert "hour_sin" in df_temp.columns
    assert "hour_cos" in df_temp.columns
    assert "sim_day" in df_temp.columns
    assert "type_transfer" in df_temp.columns
    assert "type_cash_out" in df_temp.columns

    row1 = df_temp.filter(df_temp.nameOrig == "Orig_1").first()
    assert row1["step_hour"] == 1  # 1 % 24 = 1
    assert row1["sim_day"] == 0   # (1 - 1) // 24 = 0
    assert row1["type_transfer"] == 1
    assert row1["type_cash_out"] == 0
    assert len(row1["txn_id"]) == 64  # SHA-256 length


def test_paysim_amount_features(sample_paysim_df):
    """Test amount ratios, simulator drain artifacts, and Tier B balance error."""
    df_amt = add_paysim_amount_features(sample_paysim_df, include_tier_b=True)

    # Check Tier A features
    row_drain = df_amt.filter(df_amt.nameOrig == "Orig_1").first()
    assert row_drain["orig_drain_flag"] == 1  # amount == oldbalanceOrg == 1000.0
    assert row_drain["amount_zero_flag"] == 0

    row_zero = df_amt.filter(df_amt.nameOrig == "Orig_8").first()
    assert row_zero["amount_zero_flag"] == 1

    # Check Tier B features
    assert "err_balance_orig" in df_amt.columns
    assert "err_balance_dest" in df_amt.columns
    # Orig_1: newbalanceOrig(0) + amount(1000) - oldbalanceOrg(1000) = 0.0
    assert abs(row_drain["err_balance_orig"]) < 1e-4


def test_paysim_destination_features_strict_non_leakage(sample_paysim_df):
    """
    CRITICAL ANTI-LEAKAGE TEST:
    Verify that destination history uses strictly past information:
    - First transaction has count 0 and first_seen_flag = 1.
    - Transaction's own amount is not included in prior aggregates.
    - Future transactions do not bleed into current row.
    - Same-step peers do not see each other.
    """
    df_dest = add_paysim_destination_features(sample_paysim_df)

    rows = {r["nameOrig"]: r for r in df_dest.collect()}

    # Row 1 (step 1, Dest_A): First transaction for Dest_A
    r1 = rows["Orig_1"]
    assert r1["dest_prior_txn_count"] == 0
    assert r1["dest_first_seen_flag"] == 1
    assert r1["dest_prior_amount_sum"] == 0.0

    # Row 2 (step 5, Dest_A): Second transaction
    r2 = rows["Orig_2"]
    assert r2["dest_prior_txn_count"] == 1
    assert r2["dest_first_seen_flag"] == 0
    assert r2["dest_prior_amount_sum"] == 1000.0
    assert r2["dest_steps_since_last_txn"] == 4  # 5 - 1

    # Row 3 and Row 4 (step 10, Dest_A): Both occur at step 10
    # Prior sum should only include step 1 (1000) and step 5 (2000) = 3000.0
    # Neither should see its own amount or the other step 10 amount!
    r3 = rows["Orig_3"]
    r4 = rows["Orig_4"]
    assert r3["dest_prior_txn_count"] == 2
    assert r3["dest_prior_amount_sum"] == 3000.0
    assert r4["dest_prior_txn_count"] == 2
    assert r4["dest_prior_amount_sum"] == 3000.0

    # Row 5 (step 20, Dest_A): Should see all 4 prior rows (1000+2000+3000+4000 = 10000.0)
    r5 = rows["Orig_5"]
    assert r5["dest_prior_txn_count"] == 4
    assert r5["dest_prior_amount_sum"] == 10000.0


def test_paysim_shuffled_input_determinism(sample_paysim_df):
    """Verify that physical input row order does not affect window feature outputs."""
    # Reverse input rows
    df_reversed = sample_paysim_df.sort("step", ascending=False)

    res_normal = add_paysim_destination_features(sample_paysim_df)
    res_shuffled = add_paysim_destination_features(df_reversed)

    map_normal = {r["nameOrig"]: r["dest_prior_amount_sum"] for r in res_normal.collect()}
    map_shuffled = {r["nameOrig"]: r["dest_prior_amount_sum"] for r in res_shuffled.collect()}

    assert map_normal == map_shuffled


def test_paysim_build_features_pipeline(spark, sample_paysim_df):
    """Test full build_paysim_features pipeline and row grain preservation."""
    initial_count = sample_paysim_df.count()

    df_out = build_paysim_features(
        spark=spark,
        df=sample_paysim_df,
        train_max_step=15,
        valid_max_step=25,
        feature_version="v1.0",
    )

    # Assert exact row count preservation (1 transaction = 1 feature row)
    assert df_out.count() == initial_count

    # Assert split column assignment
    assert "split" in df_out.columns
    assert "feature_version" in df_out.columns

    splits = {r["nameOrig"]: r["split"] for r in df_out.collect()}
    assert splits["Orig_1"] == "train"   # step 1 <= 15
    assert splits["Orig_5"] == "valid"   # step 20 <= 25
    assert splits["Orig_8"] == "test"    # step 30 > 25
