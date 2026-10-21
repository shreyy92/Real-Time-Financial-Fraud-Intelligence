"""
Unit and Integration Tests for Member 4 Real-Time Scoring Consumer and Deduplication.
"""

import tempfile
import os
import json
import pytest
import pandas as pd
import numpy as np

from member4_ml.src.train import ModelTrainer
from member4_ml.src.feature_selection import select_features
from member4_ml.src.scoring_consumer import (
    BoundedDeduplicationCache,
    FraudScoringEngine,
    KafkaScoringConsumer,
)


@pytest.fixture
def trained_paysim_artifact(synthetic_paysim_df):
    """Create a temporary fitted PaySim model artifact for testing."""
    train_mask = synthetic_paysim_df["split"] == "train"
    X_train, y_train = select_features(synthetic_paysim_df[train_mask], dataset="paysim")

    trainer = ModelTrainer(dataset="paysim", model_type="lightgbm")
    trainer.fit(X_train, y_train, random_state=42)

    tmpdir = tempfile.mkdtemp()
    model_path = trainer.save_artifact(model_dir=tmpdir, filename_prefix="paysim_test")
    return model_path


@pytest.fixture
def trained_ieee_artifact(synthetic_ieee_df):
    """Create a temporary fitted IEEE model artifact for testing."""
    train_mask = synthetic_ieee_df["split"] == "train"
    X_train, y_train = select_features(synthetic_ieee_df[train_mask], dataset="ieee")

    trainer = ModelTrainer(dataset="ieee", model_type="lightgbm")
    trainer.fit(X_train, y_train, random_state=42)

    tmpdir = tempfile.mkdtemp()
    model_path = trainer.save_artifact(model_dir=tmpdir, filename_prefix="ieee_test")
    return model_path


# -----------------------------------------------------------------------------
# 1. Deduplication Cache Tests
# -----------------------------------------------------------------------------

def test_bounded_deduplication_cache_hit_and_eviction():
    """Verify duplicate suppression and FIFO/LRU eviction when capacity is exceeded."""
    cache = BoundedDeduplicationCache(capacity=3)

    # Add key 1, 2, 3
    assert cache.check_and_add("key_1") is False  # New
    assert cache.check_and_add("key_2") is False  # New
    assert cache.check_and_add("key_3") is False  # New
    assert len(cache) == 3

    # Duplicate check on key 2
    assert cache.check_and_add("key_2") is True   # Duplicate!

    # Adding key 4 should evict key 1 (oldest)
    assert cache.check_and_add("key_4") is False  # New
    assert len(cache) == 3
    assert cache.contains("key_1") is False       # Evicted!
    assert cache.contains("key_2") is True
    assert cache.contains("key_4") is True


def test_bounded_deduplication_invalid_capacity():
    """Verify invalid capacity raises ValueError."""
    with pytest.raises(ValueError, match="capacity must be positive"):
        BoundedDeduplicationCache(capacity=0)


# -----------------------------------------------------------------------------
# 2. Fraud Scoring Engine Tests (PaySim & IEEE)
# -----------------------------------------------------------------------------

def test_paysim_scoring_success_and_output_schema(trained_paysim_artifact, synthetic_paysim_df):
    """Verify single-message PaySim scoring matches Member 5 schema."""
    engine = FraudScoringEngine(
        dataset="paysim",
        model_artifact_path=trained_paysim_artifact,
        model_version="v1.0",
    )

    row_dict = synthetic_paysim_df.iloc[0].to_dict()
    payload, status = engine.process_record(row_dict)

    assert status == "SCORED"
    assert payload is not None
    assert "txn_id" in payload
    assert payload["txn_id"] == row_dict["txn_id"]
    assert "fraud_score" in payload
    assert 0.0 <= payload["fraud_score"] <= 1.0
    assert payload["model_version"] == "v1.0"
    assert "scored_at" in payload
    # Assert label is NEVER leaked into scored alert
    assert "isFraud" not in payload


def test_ieee_scoring_success_and_output_schema(trained_ieee_artifact, synthetic_ieee_df):
    """Verify single-message IEEE scoring matches Member 5 schema."""
    engine = FraudScoringEngine(
        dataset="ieee",
        model_artifact_path=trained_ieee_artifact,
        model_version="v1.0",
    )

    row_dict = synthetic_ieee_df.iloc[0].to_dict()
    payload, status = engine.process_record(row_dict)

    assert status == "SCORED"
    assert payload is not None
    assert "TransactionID" in payload
    assert payload["TransactionID"] == row_dict["TransactionID"]
    assert "fraud_score" in payload
    assert 0.0 <= payload["fraud_score"] <= 1.0
    assert payload["model_version"] == "v1.0"
    assert "scored_at" in payload


def test_scoring_duplicate_suppression(trained_paysim_artifact, synthetic_paysim_df):
    """Verify re-sent duplicate message is detected and suppressed without re-scoring."""
    engine = FraudScoringEngine(
        dataset="paysim",
        model_artifact_path=trained_paysim_artifact,
    )

    row_dict = synthetic_paysim_df.iloc[0].to_dict()

    # First arrival
    payload1, status1 = engine.process_record(row_dict)
    assert status1 == "SCORED"
    assert payload1 is not None

    # At-least-once duplicate replay
    payload2, status2 = engine.process_record(row_dict)
    assert status2 == "DUPLICATE"
    assert payload2 is None


def test_scoring_missing_primary_key(trained_paysim_artifact, synthetic_paysim_df):
    """Verify record with missing or empty primary key is flagged without crashing."""
    engine = FraudScoringEngine(
        dataset="paysim",
        model_artifact_path=trained_paysim_artifact,
    )

    bad_dict = synthetic_paysim_df.iloc[0].to_dict()
    bad_dict["txn_id"] = None
    payload, status = engine.process_record(bad_dict)
    assert status == "MISSING_KEY"
    assert payload is None


def test_scoring_missing_feature(trained_paysim_artifact, synthetic_paysim_df):
    """Verify record missing a required feature is flagged as MISSING_FEATURES."""
    engine = FraudScoringEngine(
        dataset="paysim",
        model_artifact_path=trained_paysim_artifact,
    )

    bad_dict = synthetic_paysim_df.iloc[0].to_dict()
    del bad_dict["amount_log1p"]
    payload, status = engine.process_record(bad_dict)
    assert status == "MISSING_FEATURES"
    assert payload is None


def test_scoring_malformed_input(trained_paysim_artifact):
    """Verify non-dictionary input is flagged as MALFORMED."""
    engine = FraudScoringEngine(
        dataset="paysim",
        model_artifact_path=trained_paysim_artifact,
    )
    payload, status = engine.process_record("this is a raw string, not a dict")
    assert status == "MALFORMED"
    assert payload is None


def test_consumer_fails_fast_on_missing_model_artifact():
    """Verify consumer startup fails fast if model artifact is absent (no silent training)."""
    with pytest.raises(FileNotFoundError, match="Model artifact not found"):
        KafkaScoringConsumer(
            dataset="paysim",
            model_artifact_path="non_existent/model.joblib",
        )
