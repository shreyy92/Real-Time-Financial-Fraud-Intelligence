"""
Unit tests for Member 2 validation framework (schema, feature quality, leakage audit).
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
from src.validation.schema_check import validate_input_schema, validate_output_schema
from src.validation.feature_quality import validate_feature_quality
from src.validation.leakage_check import audit_feature_leakage, validate_no_target_leakage


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session(
        app_name="Test-Validation",
        master="local[1]",
        config_overrides={"spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"},
    )
    yield session
    session.stop()


def test_schema_validation_success_and_failure(spark):
    """Test that input and output schema checks correctly flag missing columns."""
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
    df = spark.createDataFrame([(1, "TRANSFER", 100.0, "C1", 100.0, 0.0, "C2", 0.0, 100.0, 0)], schema)

    # Valid PaySim input
    res_input = validate_input_schema(df, "paysim")
    assert res_input["status"] == "PASS"

    # Missing column PaySim input
    df_missing = df.drop("newbalanceDest")
    res_missing = validate_input_schema(df_missing, "paysim")
    assert res_missing["status"] == "FAIL"
    assert "newbalanceDest" in str(res_missing["missing_columns"])

    with pytest.raises(ValueError):
        validate_input_schema(df_missing, "paysim", raise_on_error=True)


def test_feature_quality_duplicate_and_null_detection(spark):
    """Test detection of duplicate identifiers and nulls in critical columns."""
    schema = StructType([
        StructField("txn_id", StringType(), False),
        StructField("isFraud", IntegerType(), True),
        StructField("split", StringType(), True),
    ])
    # Duplicate txn_id: "ID_1" appears twice
    data = [
        ("ID_1", 0, "train"),
        ("ID_1", 1, "train"),
        ("ID_2", 0, None),  # Null in critical column 'split'
    ]
    df = spark.createDataFrame(data, schema)

    res = validate_feature_quality(df, id_col="txn_id", critical_non_null_cols=["txn_id", "isFraud", "split"])

    assert res["status"] == "FAIL"
    assert res["duplicate_ids"] == 1
    assert res["null_counts"]["split"] == 1


def test_leakage_audit_and_target_prevention(spark):
    """Test that target-derived features and Tier B post-transaction features are properly flagged."""
    proposed_features = [
        "amount_log1p",
        "hour_sin",
        "dest_prior_txn_count",
        "isFraud",                     # Direct target leak
        "dest_fraud_rate",              # Prohibited target-derived
        "newbalanceOrig",               # Tier B post-transaction balance
        "nameOrig",                     # Raw identifier
    ]

    report = audit_feature_leakage(proposed_features, dataset_type="paysim")

    assert report["status"] == "FAIL"
    assert "isFraud" in report["leakage_risk_features"]
    assert "dest_fraud_rate" in report["leakage_risk_features"]
    assert "newbalanceOrig" in report["leakage_risk_features"]
    assert "nameOrig" in report["review_features"]
    assert "amount_log1p" in report["safe_features"]
    assert "hour_sin" in report["safe_features"]

    # Target exclusion function
    df = spark.createDataFrame([(1, 0)], ["id", "isFraud"])
    with pytest.raises(ValueError):
        validate_no_target_leakage(df, feature_columns=["id", "isFraud"])

    assert validate_no_target_leakage(df, feature_columns=["id", "feature1"], raise_on_error=False) is True
