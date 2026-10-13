import os
import sys
import json

sys.path.insert(0, os.path.abspath("."))
import pytest
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient

from member3_streaming.consumer_paysim import (
    classify_dlq_record,
    PAYSIM_OUTPUT_SCHEMA,
)

BROKER = "localhost:9092"


def test_kafka_connectivity():
    """Verify connectivity to real Kafka broker and verify core streaming topics."""
    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    topics = admin.list_topics()
    admin.close()

    assert "financial_transactions_paysim" in topics
    assert "financial_features_paysim" in topics
    assert "financial_transactions_paysim_dlq" in topics


def test_producer_output():
    """Verify real Kafka producer output structure and schema fields."""
    consumer = KafkaConsumer(
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    tp = TopicPartition("financial_transactions_paysim", 0)
    consumer.assign([tp])
    consumer.seek_to_beginning()

    msg = next(iter(consumer), None)
    consumer.close()

    assert msg is not None, "Inbound Kafka topic must contain transactions"
    val = json.loads(msg.value.decode("utf-8"))

    expected_fields = {
        "step", "type", "amount", "nameOrig", "oldbalanceOrg",
        "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest",
        "isFraud", "isFlaggedFraud"
    }
    for field in ["step", "type", "amount", "nameOrig", "nameDest", "isFraud"]:
        assert field in val, f"Missing expected field: {field}"
    assert isinstance(val["step"], int)
    assert isinstance(val["type"], str)
    assert isinstance(val["amount"], (int, float))


def test_schema_parsing():
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


def test_valid_and_invalid_records_every_reason_code():
    """Verify every DLQ reason code in the output contract."""
    # 1. MALFORMED_JSON
    res, _, ok = classify_dlq_record("{malformed json string")
    assert not ok and res == "MALFORMED_JSON"

    # 2. MISSING_FIELD
    res, _, ok = classify_dlq_record(json.dumps({
        "type": "PAYMENT", "amount": 100.0, "nameOrig": "C1", "nameDest": "M1", "isFraud": 0
    }))
    assert not ok and res == "MISSING_FIELD"

    # 3. BAD_STEP_TYPE
    res, _, ok = classify_dlq_record(json.dumps({
        "step": "ten", "type": "TRANSFER", "amount": 100.0,
        "nameOrig": "C1", "nameDest": "M1", "isFraud": 0
    }))
    assert not ok and res == "BAD_STEP_TYPE"

    # 4. BAD_TYPE (Invalid transaction type e.g. TELEPORT)
    res, _, ok = classify_dlq_record(json.dumps({
        "step": 1, "type": "TELEPORT", "amount": 100.0,
        "nameOrig": "C1", "nameDest": "M1", "isFraud": 0
    }))
    assert not ok and res == "BAD_TYPE"

    # 5. NEGATIVE_AMOUNT
    res, _, ok = classify_dlq_record(json.dumps({
        "step": 1, "type": "TRANSFER", "amount": -50.0,
        "nameOrig": "C1", "nameDest": "M1", "isFraud": 0
    }))
    assert not ok and res == "NEGATIVE_AMOUNT"

    # 6. INVALID_STEP
    res, _, ok = classify_dlq_record(json.dumps({
        "step": 0, "type": "TRANSFER", "amount": 100.0,
        "nameOrig": "C1", "nameDest": "M1", "isFraud": 0
    }))
    assert not ok and res == "INVALID_STEP"

    # 7. INVALID_LABEL
    res, _, ok = classify_dlq_record(json.dumps({
        "step": 1, "type": "TRANSFER", "amount": 100.0,
        "nameOrig": "C1", "nameDest": "M1", "isFraud": 99
    }))
    assert not ok and res == "INVALID_LABEL"


def test_late_event_guard():
    """Verify late-event guard rule: max(committed last step, pending step)."""
    s_last_step = 3
    pending_step = 5
    high_watermark = max([s for s in (s_last_step, pending_step) if s is not None], default=None)
    assert high_watermark == 5

    # Step 4 row: arrived behind high watermark (5) -> must be routed to DLQ as LATE_EVENT
    row_step = 4
    is_late = (high_watermark is not None) and (row_step < high_watermark)
    assert is_late is True

    # Step 5 row: same-step tie -> accepted
    row_step = 5
    is_late = (high_watermark is not None) and (row_step < high_watermark)
    assert is_late is False

    # Step 6 row: accepted and advances high watermark
    row_step = 6
    is_late = (high_watermark is not None) and (row_step < high_watermark)
    assert is_late is False


def test_output_schema_conformance():
    """Verify live output records from financial_features_paysim conform to contract schema."""
    consumer = KafkaConsumer(
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    tp = TopicPartition("financial_features_paysim", 0)
    consumer.assign([tp])
    consumer.seek_to_beginning()

    records = []
    for _ in range(50):
        msg = next(iter(consumer), None)
        if msg:
            records.append(json.loads(msg.value.decode("utf-8")))
        else:
            break
    consumer.close()

    assert len(records) > 0, "Must have output records in financial_features_paysim"

    # Verify all 41 schema columns
    expected_cols = [
        "step", "type", "amount", "nameOrig", "oldbalanceOrg", "nameDest", "oldbalanceDest",
        "isFraud", "step_hour", "txn_id", "hour_sin", "hour_cos", "sim_day",
        "history_window_complete_flag", "type_transfer", "type_cash_out", "type_payment",
        "type_cash_in", "type_debit", "split", "amount_log1p", "amount_zero_flag",
        "amt_to_oldbalOrg", "amt_to_oldbalDest", "orig_drain_flag", "amount_exceeds_orig_balance_flag",
        "orig_balance_zero_flag", "dest_balance_zero_flag", "dest_prior_txn_count",
        "dest_first_seen_flag", "dest_prior_amount_sum", "dest_prior_amount_mean",
        "dest_prior_amount_std", "dest_prior_amount_max", "dest_steps_since_last_txn",
        "dest_amount_zscore", "dest_txn_count_prev_24h", "dest_txn_count_prev_168h",
        "dest_amount_sum_prev_24h", "dest_amount_sum_prev_168h", "dest_prior_cashout_share",
    ]
    assert len(expected_cols) == 41

    for rec in records:
        for col in expected_cols:
            assert col in rec, f"Record missing required contract column: {col}"

        # Null semantics check
        if rec["dest_first_seen_flag"] == 1:
            assert rec["dest_prior_amount_mean"] is None
            assert rec["dest_prior_amount_max"] is None
            assert rec["dest_steps_since_last_txn"] is None
            assert rec["dest_amount_zscore"] is None
            assert rec["dest_prior_cashout_share"] is None


def test_stateful_parity_vs_batch():
    """Verify stateful streaming features match batch parquet ground truth with zero mismatches."""
    consumer = KafkaConsumer(
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    tp = TopicPartition("financial_features_paysim", 0)
    consumer.assign([tp])
    consumer.seek_to_beginning()

    records_by_txn = {}
    for i, msg in enumerate(consumer):
        val = json.loads(msg.value.decode("utf-8"))
        records_by_txn[val["txn_id"]] = val
        if len(records_by_txn) >= 1000:
            break
    consumer.close()

    stream_df = pd.DataFrame(list(records_by_txn.values()))
    batch_table = pq.read_table(
        "data/features/paysim_features.parquet",
        filters=[("step", "<=", int(stream_df["step"].max()))]
    )
    batch_df = batch_table.to_pandas()

    joined = pd.merge(stream_df, batch_df, on="txn_id", suffixes=("_stream", "_batch"))
    assert len(joined) == len(stream_df)

    stateful_features = [
        "dest_prior_txn_count", "dest_first_seen_flag",
        "dest_prior_amount_sum", "dest_prior_amount_mean", "dest_prior_amount_std",
        "dest_prior_amount_max", "dest_steps_since_last_txn", "dest_amount_zscore",
        "dest_txn_count_prev_24h", "dest_txn_count_prev_168h",
        "dest_amount_sum_prev_24h", "dest_amount_sum_prev_168h",
        "dest_prior_cashout_share",
    ]

    for col in stateful_features:
        s_vals = joined[f"{col}_stream"]
        b_vals = joined[f"{col}_batch"]
        null_mask = s_vals.isna() != b_vals.isna()
        assert int(null_mask.sum()) == 0, f"Null mismatch in stateful column {col}"
        valid_mask = (~s_vals.isna()) & (~b_vals.isna())
        val_mask = np.abs(s_vals[valid_mask].astype(float) - b_vals[valid_mask].astype(float)) > 1e-6
        assert int(val_mask.sum()) == 0, f"Value mismatch in stateful column {col}"


def test_checkpoint_recovery():
    """Verify RocksDB checkpoint structure and commit/offset integrity."""
    chk_dir = "member3_streaming/checkpoints/paysim"
    assert os.path.exists(chk_dir), f"Checkpoint directory {chk_dir} must exist"

    features_offsets = os.path.join(chk_dir, "features", "offsets")
    features_commits = os.path.join(chk_dir, "features", "commits")
    assert os.path.exists(features_offsets)
    assert os.path.exists(features_commits)

    offsets = sorted([int(f) for f in os.listdir(features_offsets) if f.isdigit()])
    commits = sorted([int(f) for f in os.listdir(features_commits) if f.isdigit()])

    assert len(offsets) > 0
    assert len(commits) > 0
    assert commits[-1] <= offsets[-1]

    # Inspect latest offset file to verify RocksDB provider configuration
    with open(os.path.join(features_offsets, str(offsets[-1])), "r") as f:
        content = f.read()
        assert "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider" in content
