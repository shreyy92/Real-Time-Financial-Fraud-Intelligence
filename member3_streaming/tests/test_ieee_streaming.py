"""Integration test suite for IEEE streaming consumer per Guidelines Phase C (C11).

All streaming tests run on real Kafka (127.0.0.1:9092) and real PySpark Structured Streaming.
All tests use ephemeral topics (in_<uuid>, out_<uuid>, dlq_<uuid>) and isolated checkpoints.
No test reads leftover topic data.
Slow streaming tests are decorated with @pytest.mark.slow.
"""

import os
import sys
import time
import json
import uuid
import shutil
import subprocess
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

if "HADOOP_HOME" not in os.environ and os.path.exists(r"C:\hadoop"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    bin_path = r"C:\hadoop\bin"
    if bin_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_path + os.pathsep + os.environ.get("PATH", "")

_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_member2_dir = os.path.join(_repo_root, "member2_spark")
if _member2_dir not in sys.path:
    sys.path.insert(0, _member2_dir)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from member3_streaming.ieee_raw_schema import IEEE_RAW_SCHEMA
from member3_streaming.producer_ieee import sanitize_record, json_serializer

BROKER = "127.0.0.1:9092"
CLEAN_PARQUET = os.path.join(_repo_root, "data", "processed", "ieee_clean.parquet")
FEATURES_PARQUET = os.path.join(_repo_root, "data", "features", "ieee_features.parquet")

STATEFUL_COLS = [
    "card_prior_txn_count",
    "card_first_seen_flag",
    "card_prior_amt_sum",
    "card_prior_amt_mean",
    "card_prior_amt_std",
    "card_prior_amt_max",
    "amt_zscore_vs_card",
    "card_secs_since_last_txn",
    "card_txn_count_1h",
    "card_txn_count_24h",
    "card_txn_count_7d",
]


def _kill_process(proc):
    if proc is not None and proc.poll() is None:
        try:
            if sys.platform == "win32":
                subprocess.run(f"taskkill /F /T /PID {proc.pid}", shell=True, capture_output=True)
            else:
                proc.kill()
            try:
                proc.wait(timeout=10)
            except Exception:
                pass
            time.sleep(8)
        except Exception:
            pass


def _cleanup_topics(admin, topics):
    try:
        existing = admin.list_topics()
        to_del = [t for t in topics if t in existing]
        if to_del:
            admin.delete_topics(to_del)
    except Exception:
        pass


def evaluate_parity(stream_records, gt_df, columns_to_compare, tol=1e-6):
    """Compares stream output against batch ground truth at absolute tolerance 1e-6."""
    stream_df = pd.DataFrame(list(stream_records.values()))
    cols_needed = ["TransactionID"] + [c for c in columns_to_compare if c != "TransactionID" and c in stream_df.columns]
    s_sub = stream_df[cols_needed]
    g_sub = gt_df[cols_needed]
    merged = pd.merge(s_sub, g_sub, on="TransactionID", suffixes=("_stream", "_batch"))
    assert len(merged) == len(stream_records), f"Merged count mismatch: {len(merged)} vs {len(stream_records)}"

    mismatch_counts = {}
    null_mismatch_counts = {}
    for col in columns_to_compare:
        s_col = merged[f"{col}_stream"]
        b_col = merged[f"{col}_batch"]

        null_diff = int((s_col.isna() != b_col.isna()).sum())
        null_mismatch_counts[col] = null_diff

        valid = (~s_col.isna()) & (~b_col.isna())
        if valid.sum() == 0:
            val_diff = 0
        elif pd.api.types.is_numeric_dtype(s_col) and pd.api.types.is_numeric_dtype(b_col):
            val_diff = int((np.abs(s_col[valid].astype(float) - b_col[valid].astype(float)) > tol).sum())
        else:
            val_diff = int((s_col[valid].astype(str) != b_col[valid].astype(str)).sum())

        mismatch_counts[col] = null_diff + val_diff

    total_mismatches = sum(mismatch_counts.values())
    return merged, mismatch_counts, total_mismatches


# ---------------------------------------------------------------------------
# Test 1: Fast Negative Control
# ---------------------------------------------------------------------------
def test_ieee_parity_negative_control():
    """Verify that evaluate_parity detects deliberately corrupted formula output (negative control).

    Executes in-memory: starts real Spark query: no. Reads leftover data: no.
    """
    gt_df = pq.read_table(FEATURES_PARQUET, columns=["TransactionID"] + STATEFUL_COLS).to_pandas().head(30)
    records = {int(r["TransactionID"]): dict(r) for r in gt_df.to_dict(orient="records")}

    # Perturb card_prior_amt_mean by +1.0
    for tid in records:
        if records[tid]["card_prior_amt_mean"] is not None and not np.isnan(records[tid]["card_prior_amt_mean"]):
            records[tid]["card_prior_amt_mean"] = float(records[tid]["card_prior_amt_mean"]) + 1.0

    _, mismatches, total_mismatches = evaluate_parity(records, gt_df, STATEFUL_COLS)
    assert mismatches["card_prior_amt_mean"] > 0, "Negative control failed: corrupted column was not detected!"
    assert total_mismatches > 0, "Negative control failed: total mismatches was 0!"


# ---------------------------------------------------------------------------
# Test 2: Stateless Parity Small (~500 rows, 471 compared cols)
# ---------------------------------------------------------------------------
@pytest.mark.slow
def test_ieee_stateless_parity_small():
    """Verify stateless streaming parity on ~500 rows across all 471 compared columns.

    Starts real PySpark query: yes (stateless consumer_ieee.py).
    Reads leftover data: no (ephemeral topics, isolated checkpoint).
    """
    uid = uuid.uuid4().hex[:6]
    in_topic = f"in_stateless_{uid}"
    out_topic = f"out_stateless_{uid}"
    dlq_topic = f"dlq_stateless_{uid}"
    chk_dir = os.path.abspath(f"member3_streaming/checkpoints/chk_stateless_{uid}")
    log_file = os.path.abspath(f"member3_streaming/scratch_stateless_{uid}.log")

    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    admin.create_topics([
        NewTopic(name=in_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=out_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=dlq_topic, num_partitions=1, replication_factor=1),
    ])

    proc = None
    try:
        # Load 500 rows
        df_clean = pd.read_parquet(CLEAN_PARQUET).sort_values(
            by=["TransactionDT", "TransactionID"], ascending=[True, True], kind="stable"
        ).head(500)

        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
            value_serializer=json_serializer,
        )
        for rec in df_clean.to_dict(orient="records"):
            msg = sanitize_record(rec)
            producer.send(in_topic, key=str(msg["card1"]), value=msg)
        producer.flush()
        producer.close()

        cmd = [
            sys.executable, "-m", "member3_streaming.consumer_ieee",
            "--bootstrap-servers", BROKER,
            "--input-topic", in_topic,
            "--output-topic", out_topic,
            "--dlq-topic", dlq_topic,
            "--checkpoint-location", chk_dir,
            "--stateless-only",
        ]
        with open(log_file, "w") as f:
            proc = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)

        # Poll output topic
        consumer = KafkaConsumer(bootstrap_servers=[BROKER], auto_offset_reset="earliest", consumer_timeout_ms=1000)
        consumer.assign([TopicPartition(out_topic, 0)])

        records = {}
        t0 = time.time()
        while len(records) < 500 and (time.time() - t0) < 240:
            rec_map = consumer.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        val = json.loads(msg.value.decode("utf-8"))
                        records[val["TransactionID"]] = val
            time.sleep(0.5)
        consumer.close()

        if len(records) < 500 and os.path.exists(log_file):
            with open(log_file, "r") as lf:
                print("\n[CONSUMER LOG ON TIMEOUT]:\n" + lf.read()[-3000:])

        assert len(records) == 500, f"Expected 500 output records, got {len(records)}"

        # Ground truth
        gt_full = pq.read_table(FEATURES_PARQUET).to_pandas()
        gt_sub = gt_full[gt_full["TransactionID"].isin(records.keys())]

        # Compare all columns except 11 stateful and label/keys
        ignore_cols = set(STATEFUL_COLS + ["card_proxy_id"])
        compare_cols = [c for c in gt_full.columns if c not in ignore_cols and c != "TransactionID"]

        _, mismatches, total_mismatch = evaluate_parity(records, gt_sub, compare_cols)
        assert total_mismatch == 0, f"Stateless parity mismatches found: {mismatches}"

    finally:
        _kill_process(proc)
        _cleanup_topics(admin, [in_topic, out_topic, dlq_topic])
        admin.close()
        if os.path.exists(chk_dir):
            shutil.rmtree(chk_dir, ignore_errors=True)
        if os.path.exists(log_file):
            try:
                os.remove(log_file)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Test 3: Stateful Tie & Boundary Oracle Test
# ---------------------------------------------------------------------------
@pytest.mark.slow
def test_ieee_stateful_tie_boundary_oracle():
    """Verify stateful features on synthetic tie and window boundary events (t-W, t-W-1, t-1, t).

    Starts real PySpark query: yes (stateful consumer_ieee.py).
    Reads leftover data: no (ephemeral topics, isolated checkpoint).
    """
    uid = uuid.uuid4().hex[:6]
    in_topic = f"in_oracle_{uid}"
    out_topic = f"out_oracle_{uid}"
    dlq_topic = f"dlq_oracle_{uid}"
    chk_dir = os.path.abspath(f"member3_streaming/checkpoints/chk_oracle_{uid}")
    log_file = os.path.abspath(f"member3_streaming/scratch_oracle_{uid}.log")

    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    admin.create_topics([
        NewTopic(name=in_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=out_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=dlq_topic, num_partitions=1, replication_factor=1),
    ])

    proc = None
    try:
        T = 1000000
        all_raw_fields = [f.name for f in IEEE_RAW_SCHEMA.fields]
        base_card = {
            "card1": 12345, "card2": 500.0, "card3": 150.0, "card4": "visa",
            "card5": 226.0, "card6": "debit", "ProductCD": "W", "isFraud": 0,
        }

        test_specs = [
            (100000, 1001, 50.0),
            (100000, 1002, 75.0),
            (100000, 1003, 100.0),
            (T - 604800 - 1, 1004, 60.0),
            (T - 604800, 1005, 70.0),
            (T - 86400 - 1, 1006, 80.0),
            (T - 86400, 1007, 90.0),
            (T - 3600 - 1, 1008, 110.0),
            (T - 3600, 1009, 120.0),
            (T - 1, 1010, 130.0),
            (T, 1011, 150.0),
        ]

        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
            value_serializer=json_serializer,
        )
        for dt, tid, amt in test_specs:
            row = {f: None for f in all_raw_fields}
            row.update(base_card)
            row["TransactionID"] = int(tid)
            row["TransactionDT"] = int(dt)
            row["TransactionAmt"] = float(amt)
            producer.send(in_topic, key="12345", value=sanitize_record(row))
        producer.flush()
        producer.close()

        cmd = [
            sys.executable, "-m", "member3_streaming.consumer_ieee",
            "--bootstrap-servers", BROKER,
            "--input-topic", in_topic,
            "--output-topic", out_topic,
            "--dlq-topic", dlq_topic,
            "--checkpoint-location", chk_dir,
        ]
        with open(log_file, "w") as f:
            proc = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)

        consumer = KafkaConsumer(bootstrap_servers=[BROKER], auto_offset_reset="earliest", consumer_timeout_ms=1000)
        consumer.assign([TopicPartition(out_topic, 0)])

        records = {}
        t0 = time.time()
        while len(records) < len(test_specs) and (time.time() - t0) < 150:
            rec_map = consumer.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        val = json.loads(msg.value.decode("utf-8"))
                        records[val["TransactionID"]] = val
            time.sleep(0.5)
        consumer.close()

        assert len(records) == len(test_specs), f"Expected {len(test_specs)} records, got {len(records)}"

        # Verify probe row T (tid 1011)
        r_probe = records[1011]
        assert r_probe["card_prior_txn_count"] == 10
        # 1h count: only 1009 and 1010 fall in [T-3600, T-1] -> exactly 2
        assert r_probe["card_txn_count_1h"] == 2
        # 24h count: 1007, 1008, 1009, 1010 -> exactly 4
        assert r_probe["card_txn_count_24h"] == 4
        # 7d count: 1005, 1006, 1007, 1008, 1009, 1010 -> exactly 6
        assert r_probe["card_txn_count_7d"] == 6

        # Verify same-second ties: 1001, 1002, 1003 all have 0 for rolling 1h
        assert records[1001]["card_txn_count_1h"] == 0
        assert records[1002]["card_txn_count_1h"] == 0
        assert records[1003]["card_txn_count_1h"] == 0
        # Cumulative count observes preceding tie rows
        assert records[1001]["card_prior_txn_count"] == 0
        assert records[1002]["card_prior_txn_count"] == 1
        assert records[1003]["card_prior_txn_count"] == 2

    finally:
        _kill_process(proc)
        _cleanup_topics(admin, [in_topic, out_topic, dlq_topic])
        admin.close()
        if os.path.exists(chk_dir):
            shutil.rmtree(chk_dir, ignore_errors=True)
        if os.path.exists(log_file):
            try:
                os.remove(log_file)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Test 4: Stateful Long-Span Parity with Eviction Non-Vacuity
# ---------------------------------------------------------------------------
@pytest.mark.slow
def test_ieee_stateful_long_span_parity():
    """Verify stateful streaming parity on long-span cards with non-vacuous 7-day eviction.

    Starts real PySpark query: yes (consumer_ieee.py).
    Reads leftover data: no (ephemeral topics, isolated checkpoint).
    """
    uid = uuid.uuid4().hex[:6]
    in_topic = f"in_longspan_{uid}"
    out_topic = f"out_longspan_{uid}"
    dlq_topic = f"dlq_longspan_{uid}"
    chk_dir = os.path.abspath(f"member3_streaming/checkpoints/chk_longspan_{uid}")
    log_file = os.path.abspath(f"member3_streaming/scratch_longspan_{uid}.log")

    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    admin.create_topics([
        NewTopic(name=in_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=out_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=dlq_topic, num_partitions=1, replication_factor=1),
    ])

    proc = None
    try:
        # Select cards with long spans (> 30 days) and >= 150 rows
        gt_df = pq.read_table(FEATURES_PARQUET).to_pandas()
        card_spans = gt_df.groupby("card_proxy_id").agg(
            min_dt=("TransactionDT", "min"),
            max_dt=("TransactionDT", "max"),
            count=("TransactionID", "count"),
        )
        long_cards = card_spans[(card_spans["max_dt"] - card_spans["min_dt"] >= 86400 * 30) & (card_spans["count"] >= 150)].index[:3]
        target_df = gt_df[gt_df["card_proxy_id"].isin(long_cards)].sort_values(
            by=["TransactionDT", "TransactionID"], ascending=[True, True], kind="stable"
        ).head(500)

        clean_full = pd.read_parquet(CLEAN_PARQUET)
        target_clean = clean_full[clean_full["TransactionID"].isin(target_df["TransactionID"])].sort_values(
            by=["TransactionDT", "TransactionID"], ascending=[True, True], kind="stable"
        )

        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
            value_serializer=json_serializer,
        )
        for rec in target_clean.to_dict(orient="records"):
            msg = sanitize_record(rec)
            producer.send(in_topic, key=str(msg["card1"]), value=msg)
        producer.flush()
        producer.close()

        cmd = [
            sys.executable, "-m", "member3_streaming.consumer_ieee",
            "--bootstrap-servers", BROKER,
            "--input-topic", in_topic,
            "--output-topic", out_topic,
            "--dlq-topic", dlq_topic,
            "--checkpoint-location", chk_dir,
        ]
        with open(log_file, "w") as f:
            proc = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)

        consumer = KafkaConsumer(bootstrap_servers=[BROKER], auto_offset_reset="earliest", consumer_timeout_ms=1000)
        consumer.assign([TopicPartition(out_topic, 0)])

        records = {}
        t0 = time.time()
        while len(records) < len(target_clean) and (time.time() - t0) < 120:
            rec_map = consumer.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        val = json.loads(msg.value.decode("utf-8"))
                        records[val["TransactionID"]] = val
            time.sleep(0.5)
        consumer.close()

        assert len(records) == len(target_clean), f"Expected {len(target_clean)} records, got {len(records)}"

        # Evaluate parity
        gt_sub = target_df[target_df["TransactionID"].isin(records.keys())]
        merged, mismatches, total_mismatch = evaluate_parity(records, gt_sub, STATEFUL_COLS)
        assert total_mismatch == 0, f"Long span parity mismatches found: {mismatches}"

        # Assert non-vacuity: eviction occurred (card_txn_count_7d < card_prior_txn_count for some rows)
        eviction_rows = (merged["card_txn_count_7d_stream"] < merged["card_prior_txn_count_stream"]).sum()
        print(f"\n[LONG SPAN NON-VACUITY] Rows with 7d eviction: {eviction_rows} / {len(merged)}")
        assert eviction_rows > 0, "Non-vacuity failure: 0 rows experienced 7-day eviction!"

    finally:
        _kill_process(proc)
        _cleanup_topics(admin, [in_topic, out_topic, dlq_topic])
        admin.close()
        if os.path.exists(chk_dir):
            shutil.rmtree(chk_dir, ignore_errors=True)
        if os.path.exists(log_file):
            try:
                os.remove(log_file)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Test 5: IEEE Late-Event Routing
# ---------------------------------------------------------------------------
@pytest.mark.slow
def test_ieee_late_event_routing():
    """Verify IEEE LATE_EVENT routing and negative control for forward events.

    Starts real PySpark query: yes (consumer_ieee.py).
    Reads leftover data: no (ephemeral topics, isolated checkpoint).
    """
    uid = uuid.uuid4().hex[:6]
    in_topic = f"in_late_{uid}"
    out_topic = f"out_late_{uid}"
    dlq_topic = f"dlq_late_{uid}"
    chk_dir = os.path.abspath(f"member3_streaming/checkpoints/chk_late_{uid}")
    log_file = os.path.abspath(f"member3_streaming/scratch_late_{uid}.log")

    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    admin.create_topics([
        NewTopic(name=in_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=out_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=dlq_topic, num_partitions=1, replication_factor=1),
    ])

    clean_path = CLEAN_PARQUET
    table = pq.read_table(clean_path)
    sample_dict = {col: table[col][0].as_py() for col in table.column_names}
    sample_dict["card1"] = 10001
    sample_dict["card2"] = 111.0
    sample_dict["card3"] = 150.0
    sample_dict["card4"] = "visa"
    sample_dict["card5"] = 226.0
    sample_dict["card6"] = "debit"
    sample_dict["ProductCD"] = "W"
    sample_dict["isFraud"] = 0
    sample_dict["TransactionAmt"] = 100.0

    def make_event(dt, tid, amt=100.0):
        rec = dict(sample_dict)
        rec["TransactionDT"] = dt
        rec["TransactionID"] = tid
        rec["TransactionAmt"] = amt
        return rec

    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )

    proc = None
    try:
        env = os.environ.copy()
        env["IEEE_DLQ_TOPIC"] = dlq_topic
        env["KAFKA_BOOTSTRAP_SERVERS"] = BROKER

        cmd = [
            sys.executable, "-u", "-m", "member3_streaming.consumer_ieee",
            "--bootstrap-servers", BROKER,
            "--input-topic", in_topic,
            "--output-topic", out_topic,
            "--dlq-topic", dlq_topic,
            "--checkpoint-location", chk_dir,
            "--max-offsets-per-trigger", "10",
        ]
        with open(log_file, "w") as f:
            proc = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, env=env)

        # 1. Send seed event DT=1000, TID=5
        e1 = make_event(dt=1000, tid=5, amt=100.0)
        producer.send(in_topic, key="10001", value=e1)
        producer.flush()

        # Wait until output topic receives TID=5
        consumer_out = KafkaConsumer(bootstrap_servers=[BROKER], auto_offset_reset="earliest", consumer_timeout_ms=1000)
        consumer_out.assign([TopicPartition(out_topic, 0)])

        out_seen = {}
        t0 = time.time()
        while 5 not in out_seen and (time.time() - t0) < 150:
            rec_map = consumer_out.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        val = json.loads(msg.value.decode("utf-8"))
                        out_seen[val["TransactionID"]] = val
            time.sleep(0.5)

        assert 5 in out_seen, "Seed transaction TID=5 was not processed into output topic"

        # 2. Send 2 late events
        e2_late_dt = make_event(dt=900, tid=3, amt=150.0)
        e3_late_tid = make_event(dt=1000, tid=4, amt=175.0)
        producer.send(in_topic, key="10001", value=e2_late_dt)
        producer.send(in_topic, key="10001", value=e3_late_tid)
        producer.flush()

        # 3. Negative control: send valid forward event DT=1001, TID=6
        e4_valid = make_event(dt=1001, tid=6, amt=200.0)
        producer.send(in_topic, key="10001", value=e4_valid)
        producer.flush()
        producer.close()

        # Wait for valid TID=6 in output topic
        t0 = time.time()
        while 6 not in out_seen and (time.time() - t0) < 150:
            rec_map = consumer_out.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        val = json.loads(msg.value.decode("utf-8"))
                        out_seen[val["TransactionID"]] = val
            time.sleep(0.5)
        consumer_out.close()

        assert 6 in out_seen, "Negative control failed: valid forward event TID=6 was not accepted"

        # Poll DLQ topic
        consumer_dlq = KafkaConsumer(bootstrap_servers=[BROKER], auto_offset_reset="earliest", consumer_timeout_ms=1000)
        consumer_dlq.assign([TopicPartition(dlq_topic, 0)])

        dlq_records = []
        t0 = time.time()
        while len(dlq_records) < 2 and (time.time() - t0) < 90:
            rec_map = consumer_dlq.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        dlq_records.append(json.loads(msg.value.decode("utf-8")))
            time.sleep(0.5)
        consumer_dlq.close()

        assert len(dlq_records) == 2, f"Expected 2 DLQ records, got {len(dlq_records)}"
        for d in dlq_records:
            assert d["reason_code"] == "LATE_EVENT", f"Expected reason LATE_EVENT, got {d['reason_code']}"

    finally:
        _kill_process(proc)
        _cleanup_topics(admin, [in_topic, out_topic, dlq_topic])
        admin.close()
        if os.path.exists(chk_dir):
            shutil.rmtree(chk_dir, ignore_errors=True)
        if os.path.exists(log_file):
            try:
                os.remove(log_file)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Test 6: All 6 DLQ Reason Codes
# ---------------------------------------------------------------------------
@pytest.mark.slow
def test_ieee_all_dlq_codes():
    """Verify that all 6 DLQ reason codes are correctly routed and valid rows are unaffected.

    Starts real PySpark query: yes (consumer_ieee.py).
    Reads leftover data: no (ephemeral topics, isolated checkpoint).
    """
    uid = uuid.uuid4().hex[:6]
    in_topic = f"in_dlq6_{uid}"
    out_topic = f"out_dlq6_{uid}"
    dlq_topic = f"dlq_dlq6_{uid}"
    chk_dir = os.path.abspath(f"member3_streaming/checkpoints/chk_dlq6_{uid}")
    log_file = os.path.abspath(f"member3_streaming/scratch_dlq6_{uid}.log")

    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    admin.create_topics([
        NewTopic(name=in_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=out_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=dlq_topic, num_partitions=1, replication_factor=1),
    ])

    proc = None
    try:
        producer = KafkaProducer(bootstrap_servers=[BROKER])
        bad_cases = [
            ("{malformed_json_unclosed", "MALFORMED_JSON"),
            (json.dumps({"TransactionID": 9999001, "TransactionDT": 100, "TransactionAmt": 50.0, "ProductCD": "W"}), "MISSING_FIELD"),
            (json.dumps({"TransactionID": 9999002, "isFraud": 0, "TransactionDT": 100, "TransactionAmt": "not_a_number", "ProductCD": "W", "card1": 1000}), "BAD_FIELD_TYPE"),
            (json.dumps({"TransactionID": 9999003, "isFraud": 0, "TransactionDT": 100, "TransactionAmt": -25.0, "ProductCD": "W", "card1": 1000}), "NEGATIVE_AMOUNT"),
            (json.dumps({"TransactionID": 9999004, "isFraud": 5, "TransactionDT": 100, "TransactionAmt": 50.0, "ProductCD": "W", "card1": 1000}), "INVALID_LABEL"),
            (json.dumps({"TransactionID": 9999005, "isFraud": 0, "TransactionDT": -100, "TransactionAmt": 50.0, "ProductCD": "W", "card1": 1000}), "INVALID_TRANSACTIONDT"),
        ]

        for payload, _ in bad_cases:
            producer.send(in_topic, key=b"BAD_KEY", value=payload.encode("utf-8"))
        producer.flush()
        producer.close()

        cmd = [
            sys.executable, "-m", "member3_streaming.consumer_ieee",
            "--bootstrap-servers", BROKER,
            "--input-topic", in_topic,
            "--output-topic", out_topic,
            "--dlq-topic", dlq_topic,
            "--checkpoint-location", chk_dir,
            "--stateless-only",
        ]
        with open(log_file, "w") as f:
            proc = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)

        consumer_dlq = KafkaConsumer(bootstrap_servers=[BROKER], auto_offset_reset="earliest", consumer_timeout_ms=1000)
        consumer_dlq.assign([TopicPartition(dlq_topic, 0)])

        dlq_records = []
        t0 = time.time()
        while len(dlq_records) < 6 and (time.time() - t0) < 150:
            rec_map = consumer_dlq.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        dlq_records.append(json.loads(msg.value.decode("utf-8")))
            time.sleep(0.5)
        consumer_dlq.close()

        assert len(dlq_records) == 6, f"Expected 6 DLQ records, got {len(dlq_records)}"
        observed_codes = [r["reason_code"] for r in dlq_records]
        expected_codes = [code for _, code in bad_cases]
        assert observed_codes == expected_codes, f"DLQ codes mismatch: {observed_codes} vs {expected_codes}"

    finally:
        _kill_process(proc)
        _cleanup_topics(admin, [in_topic, out_topic, dlq_topic])
        admin.close()
        if os.path.exists(chk_dir):
            shutil.rmtree(chk_dir, ignore_errors=True)
        if os.path.exists(log_file):
            try:
                os.remove(log_file)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Test 7: IEEE Crash Recovery with Operator Rollback
# ---------------------------------------------------------------------------
@pytest.mark.slow
def test_ieee_checkpoint_recovery():
    """Verify IEEE state continuity recovery after an abrupt mid-batch crash using operator rollback.

    Starts real PySpark query: yes (consumer_ieee.py).
    Reads leftover data: no (ephemeral topics, isolated checkpoint).
    """
    uid = uuid.uuid4().hex[:6]
    in_topic = f"in_recov_{uid}"
    out_topic = f"out_recov_{uid}"
    dlq_topic = f"dlq_recov_{uid}"
    chk_dir = os.path.abspath(f"member3_streaming/checkpoints/chk_recov_{uid}")
    log1 = os.path.abspath(f"member3_streaming/scratch_recov1_{uid}.log")
    log2 = os.path.abspath(f"member3_streaming/scratch_recov2_{uid}.log")

    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    admin.create_topics([
        NewTopic(name=in_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=out_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=dlq_topic, num_partitions=1, replication_factor=1),
    ])

    proc1 = None
    proc2 = None
    try:
        # Load 40 rows: 20 batch 1, 20 batch 2
        df_clean = pd.read_parquet(CLEAN_PARQUET).sort_values(
            by=["TransactionDT", "TransactionID"], ascending=[True, True], kind="stable"
        ).head(40)

        df1 = df_clean.iloc[:10]
        df2 = df_clean.iloc[10:40]

        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
            value_serializer=json_serializer,
        )
        for rec in df1.to_dict(orient="records"):
            msg = sanitize_record(rec)
            producer.send(in_topic, key=str(msg["card1"]), value=msg)
        producer.flush()

        cmd = [
            sys.executable, "-m", "member3_streaming.consumer_ieee",
            "--bootstrap-servers", BROKER,
            "--input-topic", in_topic,
            "--output-topic", out_topic,
            "--dlq-topic", dlq_topic,
            "--checkpoint-location", chk_dir,
            "--max-offsets-per-trigger", "10",
        ]
        with open(log1, "w") as f:
            proc1 = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)

        # Wait until first micro-batch commits
        feat_commits = os.path.join(chk_dir, "features", "commits")
        t0 = time.time()
        committed = False
        while (time.time() - t0) < 150:
            if os.path.exists(feat_commits) and len([f for f in os.listdir(feat_commits) if f.isdigit()]) > 0:
                committed = True
                break
            time.sleep(0.5)
        if not committed:
            _kill_process(proc1)
            time.sleep(10)
            with open(log1, "w") as f:
                proc1 = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)
            t0 = time.time()
            while (time.time() - t0) < 150:
                if os.path.exists(feat_commits) and len([f for f in os.listdir(feat_commits) if f.isdigit()]) > 0:
                    committed = True
                    break
                time.sleep(0.5)
        assert committed, "Consumer 1 did not commit initial micro-batch within 150s"

        pre_consumer = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        pre_consumer.assign([TopicPartition(out_topic, 0)])

        for rec in df2.to_dict(orient="records"):
            msg = sanitize_record(rec)
            producer.send(in_topic, key=str(msg["card1"]), value=msg)
        producer.flush()
        producer.close()

        pre_crash_records = []
        t0 = time.time()
        while len(pre_crash_records) < 15 and (time.time() - t0) < 60:
            rec_map = pre_consumer.poll(timeout_ms=500)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        pre_crash_records.append(json.loads(msg.value.decode("utf-8")))
            if len(pre_crash_records) >= 14 and (len(pre_crash_records) % 10) != 0:
                break
            time.sleep(0.2)
        pre_consumer.close()

        # Hard-kill mid-batch
        _kill_process(proc1)
        proc1 = None
        time.sleep(5)

        pre_count = len(pre_crash_records)
        is_mid_batch = (pre_count % 10) != 0
        print(f"\n[IEEE OPERATOR ROLLBACK CRASH TEST] Pre-kill records: {pre_count}, remainder mod 10: {pre_count % 10}")

        # Roll back uncommitted orphan offset for both features and dlq (Spark 4.1.1 workaround)
        for subchk in ["features", "dlq"]:
            sub_chk_dir = os.path.join(chk_dir, subchk)
            offsets_dir = os.path.join(sub_chk_dir, "offsets")
            commits_dir = os.path.join(sub_chk_dir, "commits")
            if os.path.exists(offsets_dir) and os.path.exists(commits_dir):
                offsets = sorted([int(f) for f in os.listdir(offsets_dir) if f.isdigit()])
                commits = sorted([int(f) for f in os.listdir(commits_dir) if f.isdigit()])
                if offsets and (not commits or offsets[-1] > commits[-1]):
                    orphan = os.path.join(offsets_dir, str(offsets[-1]))
                    orphan_crc = os.path.join(offsets_dir, f".{offsets[-1]}.crc")
                    try:
                        os.remove(orphan)
                    except Exception:
                        pass
                    try:
                        os.remove(orphan_crc)
                    except Exception:
                        pass

        # Restart Consumer 2 with same checkpoint
        with open(log2, "w") as f:
            proc2 = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)

        consumer = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        consumer.assign([TopicPartition(out_topic, 0)])
        consumer.seek_to_beginning()

        all_records = []
        distinct_txns = set()
        t0 = time.time()
        empty_polls = 0
        while (time.time() - t0) < 180:
            if proc2.poll() is not None:
                with open(log2, "r") as lf:
                    print(f"\n[CONSUMER 2 EXITED EARLY]:\n" + lf.read())
                break
            rec_map = consumer.poll(timeout_ms=1000)
            if rec_map:
                empty_polls = 0
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        val = json.loads(msg.value.decode("utf-8"))
                        all_records.append(val)
                        distinct_txns.add(val["TransactionID"])
            else:
                if len(distinct_txns) >= 40:
                    empty_polls += 1
                    if empty_polls >= 4:
                        break
            time.sleep(0.5)
        consumer.close()

        assert len(distinct_txns) == 40, f"Expected 40 distinct transactions, got {len(distinct_txns)}"

        # Assert duplicate byte-identity
        txn_payloads = {}
        dup_count = 0
        for r in all_records:
            tid = r["TransactionID"]
            r_str = json.dumps(r, sort_keys=True)
            if tid in txn_payloads:
                assert txn_payloads[tid] == r_str, f"Duplicate byte mismatch for TransactionID {tid}"
                dup_count += 1
            else:
                txn_payloads[tid] = r_str

        print(f"[IEEE CRASH STATS] Total records: {len(all_records)}, unique: {len(distinct_txns)}, duplicates: {dup_count}")
        assert dup_count > 0, f"Vacuous duplicate check: expected dup_count > 0 after mid-batch crash, got {dup_count}"

    finally:
        _kill_process(proc1)
        _kill_process(proc2)
        _cleanup_topics(admin, [in_topic, out_topic, dlq_topic])
        admin.close()
        if os.path.exists(chk_dir):
            shutil.rmtree(chk_dir, ignore_errors=True)
        if os.path.exists(log1):
            try:
                os.remove(log1)
            except Exception:
                pass
        if os.path.exists(log2):
            try:
                os.remove(log2)
            except Exception:
                pass
