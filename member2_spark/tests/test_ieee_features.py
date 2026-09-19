"""
Unit tests for IEEE-CIS feature engineering, missingness preservation, and card proxy windowing.
Uses small, deterministic in-memory Spark DataFrames.
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
from src.ieee.missingness_features import add_ieee_missingness_features
from src.ieee.card_features import add_ieee_card_features
from src.ieee.temporal_features import add_ieee_temporal_features
from src.ieee.aggregate_features import (
    add_ieee_amount_features,
    add_ieee_masked_aggregates,
    add_ieee_card_history_features,
)
from src.ieee.build_features import build_ieee_features


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session(
        app_name="Test-IEEEFeatures",
        master="local[1]",
        config_overrides={"spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"},
    )
    yield session
    session.stop()


@pytest.fixture
def sample_ieee_df(spark: SparkSession):
    schema = StructType([
        StructField("TransactionID", IntegerType(), False),
        StructField("isFraud", IntegerType(), False),
        StructField("TransactionDT", IntegerType(), False),
        StructField("TransactionAmt", DoubleType(), False),
        StructField("ProductCD", StringType(), False),
        StructField("card1", IntegerType(), False),
        StructField("card2", DoubleType(), True),
        StructField("card3", DoubleType(), True),
        StructField("card4", StringType(), True),
        StructField("card5", DoubleType(), True),
        StructField("card6", StringType(), True),
        StructField("addr1", DoubleType(), True),
        StructField("dist1", DoubleType(), True),
        StructField("P_emaildomain", StringType(), True),
        StructField("M1", StringType(), True),
        StructField("M2", StringType(), True),
        StructField("C1", DoubleType(), True),
        StructField("C2", DoubleType(), True),
        StructField("D1", DoubleType(), True),
        StructField("DeviceType", StringType(), True),
        StructField("DeviceInfo", StringType(), True),
        StructField("id_01", DoubleType(), True),
    ])

    # 6 deterministic test transactions
    # Cards:
    # Card_X (card1=1000, card2=100.0, card4='visa', card6='debit') -> Txns 1, 2, 3
    # Card_Y (card1=2000, card2=None, card4='mastercard', card6='credit') -> Txns 4, 5
    # Card_Z -> Txn 6
    data = [
        (101, 0, 3600, 50.00, "W", 1000, 100.0, 150.0, "visa", 226.0, "debit", 300.0, None, "gmail.com", "T", "T", 1.0, 1.0, 0.0, None, None, None),
        (102, 1, 7200, 125.251, "C", 1000, 100.0, 150.0, "visa", 226.0, "debit", 300.0, 5.0, "gmail.com", "T", "F", 2.0, 1.0, 1.0, "desktop", "Windows", -5.0),
        (103, 0, 90000, 200.00, "W", 1000, 100.0, 150.0, "visa", 226.0, "debit", 300.0, None, "yahoo.com", "T", "T", 3.0, 2.0, 2.0, None, None, None),
        (104, 0, 10000, 15.00, "W", 2000, None, 150.0, "mastercard", None, "credit", None, None, None, None, None, 1.0, 1.0, 0.0, None, None, None),
        (105, 0, 20000, 30.00, "W", 2000, None, 150.0, "mastercard", None, "credit", None, None, None, None, None, 2.0, 1.0, 1.0, "mobile", "iOS", -10.0),
        (106, 1, 500000, 99.99, "R", 3000, 200.0, 150.0, "discover", 100.0, "credit", 150.0, 12.0, "anonymous.com", "F", "F", 1.0, 1.0, 0.0, "desktop", "MacOS", 0.0),
    ]

    return spark.createDataFrame(data, schema)


def test_ieee_missingness_no_dropna(sample_ieee_df):
    """
    CRITICAL RULE:
    Verify that IEEE missingness features do NOT drop rows with nulls,
    and correctly derive identity flags and completeness.
    """
    initial_count = sample_ieee_df.count()
    df_missing = add_ieee_missingness_features(sample_ieee_df)

    assert df_missing.count() == initial_count  # Zero rows dropped
    assert "has_identity_flag" in df_missing.columns
    assert "identity_nonnull_count" in df_missing.columns
    assert "ind_addr" in df_missing.columns
    assert "ind_dist1" in df_missing.columns

    rows = {r["TransactionID"]: r for r in df_missing.collect()}

    # Txn 101: No identity info populated
    assert rows[101]["has_identity_flag"] == 0
    assert rows[101]["identity_nonnull_count"] == 0
    assert rows[101]["ind_addr"] == 1
    assert rows[101]["ind_dist1"] == 0  # dist1 was null

    # Txn 102: DeviceType, DeviceInfo, id_01 populated
    assert rows[102]["has_identity_flag"] == 1
    assert rows[102]["identity_nonnull_count"] == 3
    assert rows[102]["ind_dist1"] == 1  # dist1 was 5.0


def test_ieee_card_proxy_derivation(sample_ieee_df):
    """Verify card_proxy_id handles missing values and groups identical cards."""
    df_card = add_ieee_card_features(sample_ieee_df)

    assert "card_proxy_id" in df_card.columns
    assert "card_addr_proxy_id" in df_card.columns

    rows = {r["TransactionID"]: r for r in df_card.collect()}

    # Txns 101, 102, 103 share the exact same card1-card6
    assert rows[101]["card_proxy_id"] == rows[102]["card_proxy_id"]
    assert rows[102]["card_proxy_id"] == rows[103]["card_proxy_id"]

    # Txns 104, 105 share Card_Y (with null card2 and card5)
    assert rows[104]["card_proxy_id"] == rows[105]["card_proxy_id"]

    # Card_X and Card_Y must have different proxy IDs
    assert rows[101]["card_proxy_id"] != rows[104]["card_proxy_id"]


def test_ieee_temporal_and_amount_fingerprints(sample_ieee_df):
    """Test relative hour derivation and currency decimal fingerprints."""
    df_temp = add_ieee_temporal_features(sample_ieee_df)
    df_amt = add_ieee_amount_features(df_temp)

    rows = {r["TransactionID"]: r for r in df_amt.collect()}

    # Txn 101: DT = 3600s -> hour 1, whole dollar (50.00)
    assert rows[101]["hour_of_day"] == 1
    assert rows[101]["amt_decimals_gt2_flag"] == 0
    assert rows[101]["amt_is_whole_flag"] == 1

    # Txn 102: DT = 7200s -> hour 2, amount = 125.251 (>2 decimals)
    assert rows[102]["hour_of_day"] == 2
    assert rows[102]["amt_decimals_gt2_flag"] == 1


def test_ieee_card_history_strict_non_leakage(sample_ieee_df):
    """
    CRITICAL ANTI-LEAKAGE TEST:
    Verify card proxy history uses strictly past information:
    - First transaction has count 0 and first_seen_flag = 1.
    - Transaction's own amount is excluded from prior sum.
    - Future transactions do not bleed into current row.
    """
    df_prep = add_ieee_card_features(sample_ieee_df)
    df_hist = add_ieee_card_history_features(df_prep)

    rows = {r["TransactionID"]: r for r in df_hist.collect()}

    # Txn 101: First transaction of Card_X (DT = 3600, Amt = 50.0)
    assert rows[101]["card_prior_txn_count"] == 0
    assert rows[101]["card_first_seen_flag"] == 1
    assert rows[101]["card_prior_amt_sum"] == 0.0

    # Txn 102: Second transaction of Card_X (DT = 7200, Amt = 125.251)
    assert rows[102]["card_prior_txn_count"] == 1
    assert rows[102]["card_first_seen_flag"] == 0
    assert rows[102]["card_prior_amt_sum"] == 50.0

    # Txn 103: Third transaction of Card_X (DT = 90000, Amt = 200.0)
    assert rows[103]["card_prior_txn_count"] == 2
    assert rows[103]["card_first_seen_flag"] == 0
    assert rows[103]["card_prior_amt_sum"] == 50.0 + 125.251


def test_ieee_build_features_pipeline(spark, sample_ieee_df):
    """Test full build_ieee_features pipeline and row grain preservation."""
    initial_count = sample_ieee_df.count()

    df_out = build_ieee_features(
        spark=spark,
        df=sample_ieee_df,
        train_max_day=2.0,  # 2 days = 172,800s
        feature_version="v1.0",
    )

    # Assert exact row count preservation
    assert df_out.count() == initial_count
    assert "split" in df_out.columns
    assert "feature_version" in df_out.columns

    rows = {r["TransactionID"]: r["split"] for r in df_out.collect()}
    assert rows[101] == "train"  # DT 3600 <= 172800
    assert rows[106] == "valid"  # DT 500000 > 172800
