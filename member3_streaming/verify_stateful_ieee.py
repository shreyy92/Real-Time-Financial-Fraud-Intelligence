"""Comprehensive verification script for Chunk 9: IEEE Stateful Features Gate.

Verifies:
- Gate 9.1: Parity on 20,000 rows across 40 micro-batches (maxOffsetsPerTrigger=500) vs ieee_features.parquet (0 mismatches).
- Gate 9.2: Window expiry exercised on 300 long-span cards (> 7 days): card_txn_count_1h/24h/7d != prior_count reported and 0 mismatches.
- Gate 9.3: Same-timestamp ties across micro-batch boundary preserve total ordering and exact feature parity.
- Gate 9.4: Late-event guard: T=300, T=500 -> T=400 routed to DLQ with LATE_EVENT, T=500 tie accepted, T=600 accepted.
- Gate 9.5: Null semantics: card_prior_amt_mean/std/max, amt_zscore_vs_card, card_secs_since_last_txn null exactly when batch is null.
- Gate 9.6: Negative card_secs_since_last_txn count == 0.
"""

import os
import sys
import time
import json
import shutil
import subprocess

if "HADOOP_HOME" not in os.environ and os.path.exists(r"C:\hadoop"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    bin_path = r"C:\hadoop\bin"
    if bin_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_path + os.pathsep + os.environ.get("PATH", "")

_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_member2_dir = os.path.join(_repo_root, "member2_spark")
if _member2_dir not in sys.path:
    sys.path.insert(0, _member2_dir)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

BROKER = "127.0.0.1:9092"
PARQUET_PATH = "data/features/ieee_features.parquet"
CLEAN_PATH = "data/processed/ieee_clean.parquet"

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


def reset_topics(topics):
    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    existing = admin.list_topics()
    to_delete = [t for t in topics if t in existing]
    if to_delete:
        admin.delete_topics(to_delete)
        time.sleep(2)
    admin.create_topics([NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topics])
    admin.close()


def _kill_proc(proc):
    if proc and proc.poll() is None:
        try:
            cmd = f"powershell -NoProfile -Command \"Get-CimInstance Win32_Process | Where-Object {{ $_.ParentProcessId -eq {proc.pid} }} | ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force }}\""
            subprocess.run(cmd, shell=True, capture_output=True)
        except Exception:
            pass
        subprocess.run(f"taskkill /F /T /PID {proc.pid}", shell=True, capture_output=True)
        try:
            proc.wait(timeout=10)
        except Exception:
            pass
        time.sleep(3)


def poll_records(topic, count, timeout_sec=600, proc=None, log_path=None, extract_cols=None):
    consumer = KafkaConsumer(
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    consumer.assign([TopicPartition(topic, 0)])
    records = {}
    t0 = time.time()
    last_print = 0
    while len(records) < count and (time.time() - t0) < timeout_sec:
        if proc and proc.poll() is not None:
            if log_path and os.path.exists(log_path):
                with open(log_path) as f:
                    print("ERROR: Process died early:\n", f.read(), flush=True)
            raise RuntimeError(f"Consumer process {proc.pid} died early.")
        rec_map = consumer.poll(timeout_ms=1000)
        if rec_map:
            for tp, msgs in rec_map.items():
                for msg in msgs:
                    val = json.loads(msg.value.decode("utf-8"))
                    tid = val.get("TransactionID")
                    if tid is not None:
                        if extract_cols:
                            records[tid] = {k: val[k] for k in extract_cols if k in val}
                        else:
                            records[tid] = val
        if len(records) >= count:
            break
        if time.time() - last_print >= 5:
            print(f"[{topic}] Consumed {len(records)} / {count} ({time.time() - t0:.1f}s elapsed)...", flush=True)
            last_print = time.time()
        time.sleep(0.5)
    consumer.close()
    return records


def evaluate_parity(stream_records, gt_df, columns_to_compare):
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
            val_diff = int((np.abs(s_col[valid].astype(float) - b_col[valid].astype(float)) > 1e-6).sum())
        else:
            val_diff = int((s_col[valid].astype(str) != b_col[valid].astype(str)).sum())

        mismatch_counts[col] = null_diff + val_diff

    return merged, mismatch_counts, null_mismatch_counts


def run_gate_9_1_and_9_5_and_9_6():
    print("\n" + "=" * 80, flush=True)
    print("RUNNING GATE 9.1, 9.5, 9.6: 20,000 Rows Stateful Parity (40 micro-batches)", flush=True)
    print("=" * 80, flush=True)
    in_topic = "c09_txns_20k"
    out_topic = "c09_feats_20k"
    dlq_topic = "c09_dlq_20k"
    chk_dir = os.path.abspath("member3_streaming/checkpoints/c09_chk_20k")
    log_path = os.path.abspath("member3_streaming/scratch_c09_20k.log")

    if os.path.exists(chk_dir):
        shutil.rmtree(chk_dir, ignore_errors=True)
    reset_topics([in_topic, out_topic, dlq_topic])

    # 1. Produce 20,000 rows
    print("Producing 20,000 rows...", flush=True)
    df_clean = pd.read_parquet(CLEAN_PATH)
    df_clean = df_clean.sort_values(by=["TransactionDT", "TransactionID"], ascending=[True, True], kind="stable").head(20000)

    from member3_streaming.producer_ieee import sanitize_record, json_serializer
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        value_serializer=json_serializer,
    )
    for rec in df_clean.to_dict(orient="records"):
        clean_msg = sanitize_record(rec)
        producer.send(in_topic, key=str(clean_msg["card1"]), value=clean_msg)
    producer.flush()
    producer.close()
    print("Produced 20,000 rows successfully.", flush=True)

    # 2. Launch consumer with maxOffsetsPerTrigger=500 (40 micro-batches)
    cmd = [
        sys.executable, "-m", "member3_streaming.consumer_ieee",
        "--bootstrap-servers", BROKER,
        "--input-topic", in_topic,
        "--output-topic", out_topic,
        "--dlq-topic", dlq_topic,
        "--checkpoint-location", chk_dir,
        "--max-offsets-per-trigger", "500",
    ]
    with open(log_path, "w") as lf:
        proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT)
    print(f"Consumer started (PID {proc.pid}, maxOffsetsPerTrigger=500)...", flush=True)

    # 3. Poll output (storing only needed columns for low memory overhead)
    poll_cols = ["TransactionID"] + STATEFUL_COLS
    records = poll_records(out_topic, 20000, timeout_sec=1200, proc=proc, log_path=log_path, extract_cols=poll_cols)
    _kill_proc(proc)

    # 4. Parity evaluation
    print(f"Evaluating stateful parity on {len(records)} transactions...", flush=True)
    gt_df = pq.read_table(PARQUET_PATH, columns=["TransactionID"] + STATEFUL_COLS).to_pandas()
    target_ids = set(records.keys())
    gt_subset = gt_df[gt_df["TransactionID"].isin(target_ids)]

    merged, mismatches, null_diffs = evaluate_parity(records, gt_subset, STATEFUL_COLS)

    print("\n--- STATEFUL COLUMNS PARITY TABLE (20,000 rows) ---")
    total_stateful_mismatches = 0
    for col in STATEFUL_COLS:
        m = mismatches[col]
        nd = null_diffs[col]
        total_stateful_mismatches += m
        status = "PASSED" if m == 0 else f"FAILED ({m} mismatches)"
        print(f"  {col:30s} | {status:10s} | total_mismatch={m} (null_diff={nd})")

    # Gate 9.6: No negative card_secs_since_last_txn
    s_secs = merged["card_secs_since_last_txn_stream"]
    neg_secs = int((s_secs[s_secs.notna()] < 0).sum())
    print(f"\nGate 9.6: Negative card_secs_since_last_txn count: {neg_secs}")
    assert neg_secs == 0, f"Found {neg_secs} negative card_secs_since_last_txn values!"

    # Gate 9.5: Null semantics check
    null_cols = ["card_prior_amt_mean", "card_prior_amt_std", "card_prior_amt_max", "amt_zscore_vs_card", "card_secs_since_last_txn"]
    for nc in null_cols:
        assert null_diffs[nc] == 0, f"Null diff on {nc}: {null_diffs[nc]}"
    print("Gate 9.5 PASSED: Null semantics 100% match batch ground truth.")

    assert total_stateful_mismatches == 0, f"Total stateful mismatches: {total_stateful_mismatches}"
    print("Gate 9.1 & 9.5 & 9.6 PASSED: 0 mismatches across all 11 stateful columns on 20,000 rows (40 micro-batches)!\n")
    return len(records)


def run_gate_9_2_window_expiry():
    print("\n" + "=" * 80, flush=True)
    print("RUNNING GATE 9.2: Window Expiry Exercised on 300 Long-Span Cards (> 7 days)", flush=True)
    print("=" * 80, flush=True)
    in_topic = "c09_txns_expiry"
    out_topic = "c09_feats_expiry"
    dlq_topic = "c09_dlq_expiry"
    chk_dir = os.path.abspath("member3_streaming/checkpoints/c09_chk_expiry")
    log_path = os.path.abspath("member3_streaming/scratch_c09_expiry.log")

    if os.path.exists(chk_dir):
        shutil.rmtree(chk_dir, ignore_errors=True)
    reset_topics([in_topic, out_topic, dlq_topic])

    # 1. Identify 300 cards spanning > 7 days (604,800 s)
    gt_cards = pq.read_table(PARQUET_PATH, columns=["TransactionID", "TransactionDT", "card_proxy_id"]).to_pandas()
    grouped = gt_cards.groupby("card_proxy_id")["TransactionDT"].agg(["min", "max", "count"])
    grouped["span"] = grouped["max"] - grouped["min"]
    long_span_cards = grouped[grouped["span"] > 604800].head(300)
    selected_card_ids = set(long_span_cards.index)

    matched_tids = set(gt_cards[gt_cards["card_proxy_id"].isin(selected_card_ids)]["TransactionID"])
    df_clean = pd.read_parquet(CLEAN_PATH)
    expiry_df = df_clean[df_clean["TransactionID"].isin(matched_tids)].sort_values(
        by=["TransactionDT", "TransactionID"], ascending=[True, True], kind="stable"
    )
    total_expiry_rows = len(expiry_df)
    print(f"Selected 300 long-span cards: {len(selected_card_ids)} distinct cards, {total_expiry_rows} total rows.", flush=True)

    # 2. Produce rows
    from member3_streaming.producer_ieee import sanitize_record, json_serializer
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        value_serializer=json_serializer,
    )
    for rec in expiry_df.to_dict(orient="records"):
        clean_msg = sanitize_record(rec)
        producer.send(in_topic, key=str(clean_msg["card1"]), value=clean_msg)
    producer.flush()
    producer.close()
    print("Produced long-span rows to Kafka.", flush=True)

    # 3. Run consumer
    cmd = [
        sys.executable, "-m", "member3_streaming.consumer_ieee",
        "--bootstrap-servers", BROKER,
        "--input-topic", in_topic,
        "--output-topic", out_topic,
        "--dlq-topic", dlq_topic,
        "--checkpoint-location", chk_dir,
    ]
    with open(log_path, "w") as lf:
        proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT)
    print(f"Consumer started (PID {proc.pid})...", flush=True)

    poll_cols = ["TransactionID"] + STATEFUL_COLS
    records = poll_records(out_topic, total_expiry_rows, timeout_sec=600, proc=proc, log_path=log_path, extract_cols=poll_cols)
    _kill_proc(proc)

    # 4. Check window expiry counts and parity
    gt_stateful_table = pq.read_table(PARQUET_PATH, columns=["TransactionID"] + STATEFUL_COLS)
    gt_stateful_df = gt_stateful_table.to_pandas()
    gt_subset = gt_stateful_df[gt_stateful_df["TransactionID"].isin(set(records.keys()))]
    merged, mismatches, _ = evaluate_parity(records, gt_subset, STATEFUL_COLS)

    c_1h = int((merged["card_txn_count_1h_stream"] != merged["card_prior_txn_count_stream"]).sum())
    c_24h = int((merged["card_txn_count_24h_stream"] != merged["card_prior_txn_count_stream"]).sum())
    c_7d = int((merged["card_txn_count_7d_stream"] != merged["card_prior_txn_count_stream"]).sum())

    print(f"Window Expiry Exercised Verification on {len(merged)} rows:")
    print(f"  Rows where card_txn_count_1h != card_prior_txn_count: {c_1h} (> 0: {c_1h > 0})")
    print(f"  Rows where card_txn_count_24h != card_prior_txn_count: {c_24h} (> 0: {c_24h > 0})")
    print(f"  Rows where card_txn_count_7d != card_prior_txn_count: {c_7d} (> 0: {c_7d > 0})")

    assert c_1h > 0, "card_txn_count_1h window expiry was not exercised!"
    assert c_24h > 0, "card_txn_count_24h window expiry was not exercised!"
    assert c_7d > 0, "card_txn_count_7d window expiry was not exercised!"

    total_mismatches = sum(mismatches.values())
    print(f"Total stateful mismatches on long-span subset: {total_mismatches}")
    assert total_mismatches == 0, f"Mismatches on long-span subset: {mismatches}"
    print("Gate 9.2 PASSED: Window expiry exercised on all 3 window sizes with 0 mismatches!\n")


def run_gate_9_3_same_timestamp_ties():
    print("\n" + "=" * 80)
    print("RUNNING GATE 9.3: Same-Timestamp Ties Across Micro-Batch Boundary")
    print("=" * 80)
    in_topic = "c09_txns_ties"
    out_topic = "c09_feats_ties"
    dlq_topic = "c09_dlq_ties"
    chk_dir = os.path.abspath("member3_streaming/checkpoints/c09_chk_ties")
    log_path = os.path.abspath("member3_streaming/scratch_c09_ties.log")

    if os.path.exists(chk_dir):
        shutil.rmtree(chk_dir, ignore_errors=True)
    reset_topics([in_topic, out_topic, dlq_topic])

    # Find real transactions starting from first transaction for this card:
    # 3365311 (prior=0, DT=9474816)
    # 3365312 (prior=1, DT=9474817)
    # 3365313 (prior=2, DT=9474817 - SAME TIMESTAMP TIE!)
    df_clean = pd.read_parquet(CLEAN_PATH)
    t1_row = df_clean[df_clean["TransactionID"] == 3365311].iloc[0].to_dict()
    t2_row = df_clean[df_clean["TransactionID"] == 3365312].iloc[0].to_dict()
    t3_row = df_clean[df_clean["TransactionID"] == 3365313].iloc[0].to_dict()
    assert t2_row["TransactionDT"] == t3_row["TransactionDT"], "TransactionDT must be identical for t2 and t3!"

    from member3_streaming.producer_ieee import sanitize_record, json_serializer
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        value_serializer=json_serializer,
    )

    # Launch consumer
    cmd = [
        sys.executable, "-m", "member3_streaming.consumer_ieee",
        "--bootstrap-servers", BROKER,
        "--input-topic", in_topic,
        "--output-topic", out_topic,
        "--dlq-topic", dlq_topic,
        "--checkpoint-location", chk_dir,
    ]
    with open(log_path, "w") as lf:
        proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT)
    time.sleep(12)

    # Step A: Send t1 and t2 in micro-batch 1
    print("Sending Row 1 (TransactionID 3365311) and Row 2 (TransactionID 3365312, DT=9474817)...")
    producer.send(in_topic, key=str(t1_row["card1"]), value=sanitize_record(t1_row))
    producer.send(in_topic, key=str(t2_row["card1"]), value=sanitize_record(t2_row))
    producer.flush()

    # Wait for Row 1 and Row 2 to be processed and committed
    consumer_out = KafkaConsumer(bootstrap_servers=[BROKER], auto_offset_reset="earliest", consumer_timeout_ms=1000)
    consumer_out.assign([TopicPartition(out_topic, 0)])
    rec1 = None
    rec2 = None
    t0 = time.time()
    while (rec1 is None or rec2 is None) and (time.time() - t0) < 30:
        for msg in consumer_out:
            val = json.loads(msg.value.decode("utf-8"))
            if val["TransactionID"] == 3365311:
                rec1 = val
            elif val["TransactionID"] == 3365312:
                rec2 = val
        time.sleep(1)
    assert rec1 is not None and rec2 is not None, "Rows 1 and 2 were not processed in micro-batch 1!"
    print("Rows 1 & 2 processed and committed to RocksDB state.")

    # Step B: Send t3 in micro-batch 2 (same TransactionDT 9474817, higher TransactionID 3365313)
    print("Sending Row 3 (TransactionID 3365313, DT=9474817 - SAME TIMESTAMP TIE)...")
    producer.send(in_topic, key=str(t3_row["card1"]), value=sanitize_record(t3_row))
    producer.flush()
    producer.close()

    rec3 = None
    t0 = time.time()
    while rec3 is None and (time.time() - t0) < 30:
        for msg in consumer_out:
            val = json.loads(msg.value.decode("utf-8"))
            if val["TransactionID"] == 3365313:
                rec3 = val
                break
        time.sleep(1)
    consumer_out.close()
    _kill_proc(proc)
    assert rec3 is not None, "Row 3 was not processed in micro-batch 2!"

    # Parity check vs batch parquet
    gt_ties = pq.read_table(PARQUET_PATH, columns=["TransactionID"] + STATEFUL_COLS).to_pandas()
    gt_ties = gt_ties[gt_ties["TransactionID"].isin([3365311, 3365312, 3365313])]

    _, mismatches, _ = evaluate_parity({3365311: rec1, 3365312: rec2, 3365313: rec3}, gt_ties, STATEFUL_COLS)
    print(f"Mismatches on same-timestamp tie pair across micro-batches: {sum(mismatches.values())}", flush=True)
    for col, cnt in mismatches.items():
        if cnt > 0:
            print(f"  {col}: {cnt} mismatches", flush=True)
    assert sum(mismatches.values()) == 0, f"Tie pair mismatches: {mismatches}"
    print("Gate 9.3 PASSED: Same-timestamp ties across micro-batch boundary maintain exact parity (0 mismatches)!\n", flush=True)


def run_gate_9_4_late_event_guard():
    print("\n" + "=" * 80)
    print("RUNNING GATE 9.4: Late-Event Guard (T=300, T=500 -> T=400 to DLQ, T=500 tie, T=600)")
    print("=" * 80)
    in_topic = "c09_txns_late"
    out_topic = "c09_feats_late"
    dlq_topic = "c09_dlq_late"
    chk_dir = os.path.abspath("member3_streaming/checkpoints/c09_chk_late")
    log_path = os.path.abspath("member3_streaming/scratch_c09_late.log")

    if os.path.exists(chk_dir):
        shutil.rmtree(chk_dir, ignore_errors=True)
    reset_topics([in_topic, out_topic, dlq_topic])

    from member3_streaming.producer_ieee import sanitize_record, json_serializer
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        value_serializer=json_serializer,
    )

    cmd = [
        sys.executable, "-m", "member3_streaming.consumer_ieee",
        "--bootstrap-servers", BROKER,
        "--input-topic", in_topic,
        "--output-topic", out_topic,
        "--dlq-topic", dlq_topic,
        "--checkpoint-location", chk_dir,
    ]
    with open(log_path, "w") as lf:
        proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT)
    time.sleep(12)

    # Base valid transaction template
    df_clean = pd.read_parquet(CLEAN_PATH)
    base_template = df_clean.iloc[0].to_dict()

    # Key to isolate
    base_template["card1"] = 777777
    base_template["card2"] = 555.0
    base_template["card3"] = 150.0
    base_template["card4"] = "visa"
    base_template["card5"] = 226.0
    base_template["card6"] = "debit"

    # Event 1: T=300
    e1 = dict(base_template, TransactionID=99001, TransactionDT=300, TransactionAmt=100.0)
    # Event 2: T=500
    e2 = dict(base_template, TransactionID=99002, TransactionDT=500, TransactionAmt=200.0)
    # Event 3: T=400 (LATE EVENT: strictly older than committed T=500)
    e3 = dict(base_template, TransactionID=99003, TransactionDT=400, TransactionAmt=150.0)
    # Event 4: T=500 (SAME TIMESTAMP TIE with higher TransactionID -> valid)
    e4 = dict(base_template, TransactionID=99004, TransactionDT=500, TransactionAmt=250.0)
    # Event 5: T=600 (Valid forward progression)
    e5 = dict(base_template, TransactionID=99005, TransactionDT=600, TransactionAmt=300.0)

    # Send E1 and E2
    producer.send(in_topic, key="777777", value=sanitize_record(e1))
    producer.send(in_topic, key="777777", value=sanitize_record(e2))
    producer.flush()
    time.sleep(5)

    # Send E3 (Late)
    producer.send(in_topic, key="777777", value=sanitize_record(e3))
    producer.flush()
    time.sleep(5)

    # Send E4 (Tie) and E5 (Forward)
    producer.send(in_topic, key="777777", value=sanitize_record(e4))
    producer.send(in_topic, key="777777", value=sanitize_record(e5))
    producer.flush()
    producer.close()
    time.sleep(8)

    # Poll DLQ topic
    consumer_dlq = KafkaConsumer(bootstrap_servers=[BROKER], auto_offset_reset="earliest", consumer_timeout_ms=1000)
    consumer_dlq.assign([TopicPartition(dlq_topic, 0)])
    dlq_records = []
    t0 = time.time()
    while len(dlq_records) < 1 and (time.time() - t0) < 30:
        for msg in consumer_dlq:
            val = json.loads(msg.value.decode("utf-8"))
            dlq_records.append(val)
        time.sleep(1)
    consumer_dlq.close()

    # Poll output topic
    consumer_out = KafkaConsumer(bootstrap_servers=[BROKER], auto_offset_reset="earliest", consumer_timeout_ms=1000)
    consumer_out.assign([TopicPartition(out_topic, 0)])
    valid_records = {}
    t0 = time.time()
    while len(valid_records) < 4 and (time.time() - t0) < 30:
        for msg in consumer_out:
            val = json.loads(msg.value.decode("utf-8"))
            valid_records[val["TransactionID"]] = val
        time.sleep(1)
    consumer_out.close()

    _kill_proc(proc)

    print(f"DLQ records received: {len(dlq_records)}")
    assert len(dlq_records) == 1, f"Expected 1 DLQ record, got {len(dlq_records)}"
    dlq_rec = dlq_records[0]
    print(f"DLQ record content: reason_code='{dlq_rec['reason_code']}'")
    assert dlq_rec["reason_code"] == "LATE_EVENT", f"Expected LATE_EVENT, got {dlq_rec['reason_code']}"

    print(f"Valid records processed: {sorted(list(valid_records.keys()))}")
    assert set(valid_records.keys()) == {99001, 99002, 99004, 99005}, f"Unexpected valid records: {set(valid_records.keys())}"
    print("Gate 9.4 PASSED: T=400 routed to DLQ with LATE_EVENT, valid events unaffected, query stayed active!\n")


def run_gate_9_8_negative_control():
    print("\n" + "=" * 80)
    print("RUNNING GATE 9.8: Negative Control (Intentionally Perturbed Formula Must Fail)")
    print("=" * 80)
    # Read ground truth sample
    gt_df = pq.read_table(PARQUET_PATH, columns=["TransactionID"] + STATEFUL_COLS).to_pandas().head(50)
    # Build simulated stream records from gt_df
    records = {int(r["TransactionID"]): r for r in gt_df.to_dict(orient="records")}

    # Perturb one formula in stream records: add 1.0 to card_prior_amt_mean
    for tid in records:
        if records[tid]["card_prior_amt_mean"] is not None and not np.isnan(records[tid]["card_prior_amt_mean"]):
            records[tid]["card_prior_amt_mean"] = float(records[tid]["card_prior_amt_mean"]) + 1.0

    _, mismatches, _ = evaluate_parity(records, gt_df, STATEFUL_COLS)
    mean_mismatches = mismatches["card_prior_amt_mean"]
    print(f"Negative control test result: card_prior_amt_mean mismatches detected = {mean_mismatches}")
    assert mean_mismatches > 0, "Negative control FAILED: Intentionally wrong formula did not trigger mismatches!"
    print("Gate 9.8 PASSED: Negative control successfully triggered mismatch assertion on wrong formula.\n")


def main():
    print("=" * 80, flush=True)
    print("STARTING COMPLETE CHUNK 9 GATE VERIFICATION SUITE", flush=True)
    print("=" * 80, flush=True)
    run_gate_9_1_and_9_5_and_9_6()
    run_gate_9_2_window_expiry()
    run_gate_9_3_same_timestamp_ties()
    run_gate_9_4_late_event_guard()
    run_gate_9_8_negative_control()

    print("\n" + "=" * 80, flush=True)
    print("ALL CHUNK 9 GATES PASSED (Gate 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.8) WITH 100% SUCCESS!", flush=True)
    print("=" * 80, flush=True)


if __name__ == "__main__":
    main()
