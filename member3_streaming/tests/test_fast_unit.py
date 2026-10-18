"""Fast in-memory unit tests and negative-control verification."""

import os
import sys
import json
import uuid
import pytest
import numpy as np
import pandas as pd
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

from member3_streaming.consumer_paysim import (
    classify_dlq_record,
    DLQ_REASON_CODES,
    PAYSIM_OUTPUT_SCHEMA,
)

BROKER = "localhost:9092"


def test_kafka_connectivity():
    """Verify live connectivity to Kafka broker by creating and deleting an ephemeral test topic."""
    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    topic_name = f"test_conn_{uuid.uuid4().hex[:6]}"
    admin.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
    topics = admin.list_topics()
    assert topic_name in topics, f"Created topic {topic_name} must exist on broker"
    admin.delete_topics([topic_name])
    admin.close()


def test_schema_parsing_valid_and_malformed():
    """Verify schema parsing and validation on valid and malformed JSON payloads."""
    valid_payload = json.dumps({
        "step": 1, "type": "PAYMENT", "amount": 9839.64,
        "nameOrig": "C1231006815", "oldbalanceOrg": 170136.0,
        "newbalanceOrig": 160296.36, "nameDest": "M1979787155",
        "oldbalanceDest": 0.0, "newbalanceDest": 0.0,
        "isFraud": 0, "isFlaggedFraud": 0
    })
    reason, ts, is_valid = classify_dlq_record(valid_payload)
    assert is_valid is True
    assert reason is None

    malformed_payload = '{"step": 1, "type": "PAYMENT", "amount": 9839.64'  # unclosed JSON
    reason_bad, _, is_valid_bad = classify_dlq_record(malformed_payload)
    assert is_valid_bad is False
    assert reason_bad == "MALFORMED_JSON"


def test_all_dlq_reason_codes_unit():
    """Verify all DLQ reason codes are correctly assigned by the evaluation logic."""
    cases = [
        # (payload_dict, expected_reason)
        (None, "MALFORMED_JSON"),
        ({"type": "PAYMENT", "amount": 100.0, "nameOrig": "C1", "nameDest": "M1", "isFraud": 0}, "MISSING_FIELD"),
        ({"step": "ten", "type": "TRANSFER", "amount": 100.0, "nameOrig": "C1", "nameDest": "M1", "isFraud": 0}, "BAD_STEP_TYPE"),
        ({"step": 1, "type": "TELEPORT", "amount": 100.0, "nameOrig": "C1", "nameDest": "M1", "isFraud": 0}, "BAD_TYPE"),
        ({"step": 1, "type": "TRANSFER", "amount": "invalid_num", "nameOrig": "C1", "nameDest": "M1", "isFraud": 0}, "BAD_FIELD_TYPE"),
        ({"step": 1, "type": "TRANSFER", "amount": -50.0, "nameOrig": "C1", "nameDest": "M1", "isFraud": 0}, "NEGATIVE_AMOUNT"),
        ({"step": 0, "type": "TRANSFER", "amount": 100.0, "nameOrig": "C1", "nameDest": "M1", "isFraud": 0}, "INVALID_STEP"),
        ({"step": 1, "type": "TRANSFER", "amount": 100.0, "nameOrig": "C1", "nameDest": "M1", "isFraud": 99}, "INVALID_LABEL"),
    ]

    for payload, expected in cases:
        raw_str = payload if isinstance(payload, str) else json.dumps(payload) if payload is not None else "{not_json"
        reason, _, ok = classify_dlq_record(raw_str)
        assert not ok, f"Expected invalid for {expected}"
        assert reason == expected, f"Expected {expected}, got {reason}"


def test_negative_control_parity_failure():
    """Negative control test: verify that corrupting a single feature value causes parity assertion to FAIL.

    This proves that the parity comparison logic is not a tautology or trivial pass.
    """
    # Create two identical sample dataframes representing stream vs batch
    df_stream = pd.DataFrame([{
        "txn_id": "test_txn_001",
        "step": 1,
        "dest_prior_txn_count": 5,
        "dest_first_seen_flag": 0,
        "dest_prior_amount_sum": 5000.0,
        "dest_prior_amount_mean": 1000.0,
        "dest_prior_amount_std": 150.0,
        "dest_prior_amount_max": 2000.0,
        "dest_steps_since_last_txn": 3,
        "dest_amount_zscore": 0.5,
        "dest_txn_count_prev_24h": 2,
        "dest_txn_count_prev_168h": 5,
        "dest_amount_sum_prev_24h": 2000.0,
        "dest_amount_sum_prev_168h": 5000.0,
        "dest_prior_cashout_share": 0.4,
    }])
    df_batch = df_stream.copy()

    # Function that runs parity check
    def assert_parity(stream_df, batch_df):
        joined = pd.merge(stream_df, batch_df, on="txn_id", suffixes=("_stream", "_batch"))
        stateful_cols = [
            "dest_prior_txn_count", "dest_first_seen_flag", "dest_prior_amount_sum",
            "dest_prior_amount_mean", "dest_prior_amount_std", "dest_prior_amount_max",
            "dest_steps_since_last_txn", "dest_amount_zscore", "dest_txn_count_prev_24h",
            "dest_txn_count_prev_168h", "dest_amount_sum_prev_24h", "dest_amount_sum_prev_168h",
            "dest_prior_cashout_share"
        ]
        mismatches = []
        for col in stateful_cols:
            s = joined[f"{col}_stream"]
            b = joined[f"{col}_batch"]
            diff = ((s.isna() != b.isna()) | ((~s.isna()) & (np.abs(s.astype(float) - b.astype(float)) > 1e-6))).sum()
            if diff > 0:
                mismatches.append(f"{col}: {diff} mismatches")
        if mismatches:
            raise AssertionError(f"Parity check failed: {'; '.join(mismatches)}")

    # 1. Parity passes when data is identical
    assert_parity(df_stream, df_batch)

    # 2. Corrupt one feature value: dest_prior_amount_sum
    df_corrupted = df_stream.copy()
    df_corrupted.loc[0, "dest_prior_amount_sum"] = 9999.0

    # 3. Assert that parity check strictly raises AssertionError
    with pytest.raises(AssertionError) as exc_info:
        assert_parity(df_corrupted, df_batch)
    assert "dest_prior_amount_sum: 1 mismatches" in str(exc_info.value), (
        f"Assertion did not report dest_prior_amount_sum mismatch: {exc_info.value}"
    )
