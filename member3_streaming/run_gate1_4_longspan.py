"""Gate 1.4: Long-Span Parity Verification (39,965 rows).

Verifies:
- Deterministic whole cards chosen from ieee_features.parquet:
  (a) 10 cards with highest max card_txn_count_7d
  (b) up to 30 cards with same-second ties
  (c) up to 30 cards spanning >= 30 days with >= 200 rows
  Capped at 40,000 rows (39,965 actual rows across 15 cards).
- Real Kafka (127.0.0.1:9092) and real PySpark Structured Streaming consumer.
- Compare the 11 stateful columns against ieee_features.parquet joined on TransactionID.
- Tolerance: 1e-6 absolute, null <-> null equal, null <-> non-null mismatch.
- DLQ record count must be exactly 0 (LATE_EVENT = 0).
- Non-vacuity counts:
  * card_txn_count_7d != card_prior_txn_count
  * card_txn_count_24h != card_txn_count_7d
  * card_txn_count_1h > 0
  * tie rows covered
  * max history buffer length
"""

import os
import sys
import time
import json
import shutil
import subprocess
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

BROKER = "127.0.0.1:9092"
IN_TOPIC = "financial_transactions_ieee"
OUT_TOPIC = "financial_features_ieee"
DLQ_TOPIC = "financial_transactions_ieee_dlq"
CHK_DIR = os.path.abspath("member3_streaming/checkpoints/ieee_stateful")
LOG_PATH = os.path.abspath("member3_streaming/consumer_gate1_4.log")
PARQUET_PATH = "data/features/ieee_features.parquet"
EXPECTED_ROWS = 39965

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


def reset_topics():
    print("Step 1: Resetting Kafka topics and checkpoint directory...")
    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    existing = admin.list_topics()
    for t in [IN_TOPIC, OUT_TOPIC, DLQ_TOPIC]:
        if t in existing:
            try:
                admin.delete_topics([t])
            except Exception:
                pass
    admin.close()
    time.sleep(2)

    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    admin.create_topics([
        NewTopic(name=IN_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(name=OUT_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(name=DLQ_TOPIC, num_partitions=1, replication_factor=1),
    ])
    admin.close()

    if os.path.exists(CHK_DIR):
        shutil.rmtree(CHK_DIR, ignore_errors=True)
    if os.path.exists(LOG_PATH):
        try:
            os.remove(LOG_PATH)
        except Exception:
            pass
    print("Kafka topics and checkpoints reset.")


def produce_data():
    print(f"Step 2: Producing deterministic subset to {IN_TOPIC} using produce_ieee_subset.py...")
    cmd = [
        sys.executable, "-m", "member3_streaming.produce_ieee_subset",
        "--cap", str(40000),
        "--delay", "0.0",
        "--bootstrap-servers", BROKER,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"Producer failed: {res.stderr}"
    print(f"Producer output:\n{res.stdout.strip()}")


def main():
    print("=" * 80)
    print("GATE 1.4: LONG-SPAN STATEFUL PARITY & EVICTION AUDIT")
    print("Real Kafka: yes (127.0.0.1:9092)")
    print("Real Spark query: yes (PySpark Structured Streaming via consumer_ieee.py)")
    print(f"Expected Rows: {EXPECTED_ROWS}")
    print("=" * 80)

    reset_topics()
    produce_data()

    print(f"\nStep 3: Launching stateful consumer_ieee.py...")
    cmd = [
        sys.executable, "-m", "member3_streaming.consumer_ieee",
        "--bootstrap-servers", BROKER,
        "--input-topic", IN_TOPIC,
        "--output-topic", OUT_TOPIC,
        "--dlq-topic", DLQ_TOPIC,
        "--checkpoint-location", CHK_DIR,
    ]
    with open(LOG_PATH, "w") as log_file:
        proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)

    consumer_out = KafkaConsumer(
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    consumer_out.assign([TopicPartition(OUT_TOPIC, 0)])

    out_records = {}
    t0 = time.time()
    last_print = 0
    while len(out_records) < EXPECTED_ROWS and (time.time() - t0) < 600:
        if proc.poll() is not None:
            with open(LOG_PATH) as lf:
                print("ERROR: Consumer died early:\n", lf.read())
            raise RuntimeError("Consumer process died early.")
        rec_map = consumer_out.poll(timeout_ms=1000)
        if rec_map:
            for tp, msgs in rec_map.items():
                for msg in msgs:
                    val = json.loads(msg.value.decode("utf-8"))
                    out_records[val["TransactionID"]] = val
        if len(out_records) >= EXPECTED_ROWS:
            break
        if time.time() - last_print > 15:
            print(f"Consumed {len(out_records)} / {EXPECTED_ROWS} output records ({time.time() - t0:.1f}s elapsed)...")
            last_print = time.time()
        time.sleep(0.5)
    consumer_out.close()

    # Poll DLQ topic
    consumer_dlq = KafkaConsumer(
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    consumer_dlq.assign([TopicPartition(DLQ_TOPIC, 0)])
    dlq_records = []
    t_dlq = time.time()
    while (time.time() - t_dlq) < 5:
        rec_map = consumer_dlq.poll(timeout_ms=1000)
        if rec_map:
            for tp, msgs in rec_map.items():
                for msg in msgs:
                    dlq_records.append(json.loads(msg.value.decode("utf-8")))
        time.sleep(0.5)
    consumer_dlq.close()

    # Terminate consumer
    try:
        cmd_kill = f"powershell -NoProfile -Command \"Get-CimInstance Win32_Process | Where-Object {{ $_.ParentProcessId -eq {proc.pid} }} | ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force }}\""
        subprocess.run(cmd_kill, shell=True, capture_output=True)
    except Exception:
        pass
    subprocess.run(f"taskkill /F /T /PID {proc.pid}", shell=True, capture_output=True)
    time.sleep(2)

    print(f"\nConsumed {len(out_records)} output records from Kafka.")
    print(f"DLQ records intercepted (LATE_EVENT): {len(dlq_records)} (Expected: 0)")
    assert len(dlq_records) == 0, f"Expected 0 DLQ records, got {len(dlq_records)}"
    assert len(out_records) == EXPECTED_ROWS, f"Expected {EXPECTED_ROWS} output records, got {len(out_records)}"

    # Load matching batch parquet ground truth
    print(f"\nLoading batch ground truth from {PARQUET_PATH}...")
    stream_df = pd.DataFrame(list(out_records.values()))
    tids = set(stream_df["TransactionID"].values)
    pq_table = pq.read_table(PARQUET_PATH, filters=[("TransactionID", "in", tids)])
    batch_df = pq_table.to_pandas()
    print(f"Loaded {len(batch_df)} matching ground truth rows.")

    merged = pd.merge(stream_df, batch_df, on="TransactionID", suffixes=("_stream", "_batch"))
    print(f"Merged row count: {len(merged)} / {EXPECTED_ROWS}")
    assert len(merged) == EXPECTED_ROWS, f"Expected {EXPECTED_ROWS} joined rows, got {len(merged)}"

    # Gate 1.4 Parity Audit across the 11 stateful columns
    print("\n" + "=" * 95)
    print(f"{'column':<35} | {'rows compared':>14} | {'null mismatches':>15} | {'val mismatches':>14} | {'max abs diff':>12}")
    print("=" * 95)

    total_mismatches = 0
    mismatch_cols = []
    for col in STATEFUL_COLS:
        s_col = merged[f"{col}_stream"]
        b_col = merged[f"{col}_batch"]

        null_mismatch = int((s_col.isna() != b_col.isna()).sum())
        valid_mask = (~s_col.isna()) & (~b_col.isna())
        rows_valid = int(valid_mask.sum())
        max_diff_str = "0.0"
        if rows_valid == 0:
            val_mismatch = 0
            max_diff_str = "N/A"
        elif pd.api.types.is_numeric_dtype(s_col) and pd.api.types.is_numeric_dtype(b_col):
            diffs = np.abs(s_col[valid_mask].astype(float) - b_col[valid_mask].astype(float))
            val_mismatch = int((diffs > 1e-6).sum())
            max_d = float(np.max(diffs)) if len(diffs) > 0 else 0.0
            max_diff_str = f"{max_d:.2e}" if max_d > 0 else "0.0"
        else:
            diffs = (s_col[valid_mask].astype(str) != b_col[valid_mask].astype(str))
            val_mismatch = int(diffs.sum())
            max_diff_str = "0.0" if val_mismatch == 0 else "STR_DIFF"

        print(f"{col:<35} | {EXPECTED_ROWS:>14} | {null_mismatch:>15} | {val_mismatch:>14} | {max_diff_str:>12}")
        col_mismatches = null_mismatch + val_mismatch
        if col_mismatches > 0:
            total_mismatches += col_mismatches
            mismatch_cols.append((col, col_mismatches, max_diff_str))

    print("=" * 95)
    print(f"Total Stateful Column Mismatches across 11 columns on {EXPECTED_ROWS} rows: {total_mismatches}")
    if total_mismatches > 0:
        print(f"Mismatched columns: {mismatch_cols}")
    assert total_mismatches == 0, f"Gate 1.4 failed with {total_mismatches} mismatches!"

    # Non-vacuity Audit
    print("\n" + "=" * 80)
    print("NON-VACUITY AUDIT ON 39,965 LONG-SPAN TRANSACTIONS")
    print("=" * 80)

    # 1. card_txn_count_7d != card_prior_txn_count
    c_7d_ne_prior = int((merged["card_txn_count_7d_stream"] != merged["card_prior_txn_count_stream"]).sum())
    # 2. 24h != 7d
    c_24h_ne_7d = int((merged["card_txn_count_24h_stream"] != merged["card_txn_count_7d_stream"]).sum())
    # 3. 1h > 0
    c_1h_gt_0 = int((merged["card_txn_count_1h_stream"] > 0).sum())
    # 4. Tie rows covered
    tie_groups = merged.groupby(["card_proxy_id_stream", "TransactionDT_stream"]).size()
    c_tie_rows = int(tie_groups[tie_groups > 1].sum())

    # 5. Max history buffer length seen
    # Computed from stream output directly
    max_history_len = int(merged["card_txn_count_7d_stream"].max())

    print(f"  Rows with card_txn_count_7d != prior:     {c_7d_ne_prior:>6} ({(c_7d_ne_prior / EXPECTED_ROWS) * 100:.2f}%)")
    print(f"  Rows with card_txn_count_24h != 7d:       {c_24h_ne_7d:>6} ({(c_24h_ne_7d / EXPECTED_ROWS) * 100:.2f}%)")
    print(f"  Rows with card_txn_count_1h > 0:          {c_1h_gt_0:>6} ({(c_1h_gt_0 / EXPECTED_ROWS) * 100:.2f}%)")
    print(f"  Rows in same-card same-DT tie groups:     {c_tie_rows:>6} ({(c_tie_rows / EXPECTED_ROWS) * 100:.2f}%)")
    print(f"  Max 7-day History Buffer Length Seen:     {max_history_len:>6}")
    print("=" * 80)

    assert c_7d_ne_prior > 0, "Non-vacuity failure: expected rows with 7d != prior (eviction test)"
    assert c_24h_ne_7d > 0, "Non-vacuity failure: expected rows with 24h != 7d"
    assert c_1h_gt_0 > 0, "Non-vacuity failure: expected rows with 1h > 0"
    assert c_tie_rows > 0, "Non-vacuity failure: expected tie rows"

    print("Gate 1.4 PASSED: 100% LONG-SPAN PARITY ACROSS ALL 11 COLUMNS WITH ZERO LATE_EVENTS!")
    print("=" * 80)


if __name__ == "__main__":
    main()
