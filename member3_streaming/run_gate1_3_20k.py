"""Gate 1.3: 20k Row End-to-End Parity Verification across ALL 483 Columns.

Verifies:
- First 20,000 rows by (TransactionDT, TransactionID) via real producer.
- Real Kafka (127.0.0.1:9092) and real PySpark Structured Streaming consumer.
- Compare ALL 483 columns against ieee_features.parquet joined on TransactionID.
- Tolerance: 1e-6 absolute, null <-> null equal, null <-> non-null mismatch.
- Per-column comparison table printed in full.
- Non-vacuity counts reported:
    * rows with prior_count >= 3
    * rows with zscore non-null
    * rows with 1h > 0
    * rows with 24h > 0
    * rows with 7d > 0
    * rows with 24h count != prior count
    * rows in same-card same-DT tie groups
- DLQ record count must be exactly 0 on valid data.
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
LOG_PATH = os.path.abspath("member3_streaming/consumer_gate1_3.log")
PARQUET_PATH = "data/features/ieee_features.parquet"
TOTAL_ROWS = 20000


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
    print(f"Step 2: Producing {TOTAL_ROWS} rows to {IN_TOPIC} using producer_ieee.py...")
    cmd = [
        sys.executable, "-m", "member3_streaming.producer_ieee",
        "--limit", str(TOTAL_ROWS),
        "--delay", "0.0",
        "--bootstrap-servers", BROKER,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"Producer failed: {res.stderr}"
    print(f"Producer finished successfully: {res.stdout.strip()}")


def main():
    print("=" * 80)
    print("GATE 1.3: 20,000 ROWS ALL-COLUMN PARITY & NON-VACUITY AUDIT")
    print("Real Kafka: yes (127.0.0.1:9092)")
    print("Real Spark query: yes (PySpark Structured Streaming via consumer_ieee.py)")
    print("=" * 80)

    reset_topics()
    produce_data()

    print(f"Step 3: Launching stateful consumer_ieee.py...")
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
    while len(out_records) < TOTAL_ROWS and (time.time() - t0) < 600:
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
        if len(out_records) >= TOTAL_ROWS:
            break
        if time.time() - last_print > 15:
            print(f"Consumed {len(out_records)} / {TOTAL_ROWS} output records ({time.time() - t0:.1f}s elapsed)...")
            last_print = time.time()
        time.sleep(0.5)
    consumer_out.close()

    # Poll DLQ topic to assert 0 errors on valid data
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
    print(f"DLQ records intercepted: {len(dlq_records)} (Expected: 0)")
    assert len(dlq_records) == 0, f"Expected 0 DLQ records on valid data, got {len(dlq_records)}: {dlq_records[:5]}"
    assert len(out_records) == TOTAL_ROWS, f"Expected {TOTAL_ROWS} output records, got {len(out_records)}"

    # Load matching batch parquet ground truth
    print(f"\nLoading batch ground truth from {PARQUET_PATH}...")
    t_load = time.time()
    pq_table = pq.read_table(PARQUET_PATH)
    batch_df = pq_table.to_pandas()
    print(f"Loaded {len(batch_df)} ground truth rows in {time.time() - t_load:.2f}s.")

    stream_df = pd.DataFrame(list(out_records.values()))
    merged = pd.merge(stream_df, batch_df, on="TransactionID", suffixes=("_stream", "_batch"))
    print(f"Merged row count: {len(merged)} / {TOTAL_ROWS}")
    assert len(merged) == TOTAL_ROWS, f"Expected {TOTAL_ROWS} joined rows, got {len(merged)}"

    # Gate 1.3 Parity Audit across ALL 483 columns
    all_columns = sorted([f.name for f in pq_table.schema])
    print(f"Total columns evaluated: {len(all_columns)} (Expected: 483)")
    assert len(all_columns) == 483, f"Expected 483 columns, got {len(all_columns)}"

    print("\n" + "=" * 95)
    print(f"{'column':<35} | {'rows compared':>14} | {'null mismatches':>15} | {'val mismatches':>14} | {'max abs diff':>12}")
    print("=" * 95)

    total_mismatches = 0
    mismatch_cols = []
    for col in all_columns:
        if col == "TransactionID":
            print(f"{col:<35} | {TOTAL_ROWS:>14} | {0:>15} | {0:>14} | {'0.0':>12}")
            continue

        s_col = merged[f"{col}_stream"]
        b_col = merged[f"{col}_batch"]

        # Null equivalence: null <-> null equal, null <-> non-null mismatch
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

        print(f"{col:<35} | {TOTAL_ROWS:>14} | {null_mismatch:>15} | {val_mismatch:>14} | {max_diff_str:>12}")
        col_mismatches = null_mismatch + val_mismatch
        if col_mismatches > 0:
            total_mismatches += col_mismatches
            mismatch_cols.append((col, col_mismatches, max_diff_str))

    print("=" * 95)
    print(f"Total Column Mismatches across all 483 columns on 20,000 rows: {total_mismatches}")
    if total_mismatches > 0:
        print(f"Mismatched columns: {mismatch_cols}")
    assert total_mismatches == 0, f"Gate 1.3 failed with {total_mismatches} mismatches!"

    # Non-vacuity Audit
    print("\n" + "=" * 80)
    print("NON-VACUITY AUDIT ON 20,000 TRANSACTIONS")
    print("=" * 80)

    # 1. rows with prior_count >= 3
    c_prior_gte_3 = int((merged["card_prior_txn_count_stream"] >= 3).sum())
    # 2. rows with zscore non-null
    c_zscore_nonnull = int((~merged["amt_zscore_vs_card_stream"].isna()).sum())
    # 3. rows with 1h > 0
    c_1h_gt_0 = int((merged["card_txn_count_1h_stream"] > 0).sum())
    # 4. rows with 24h > 0
    c_24h_gt_0 = int((merged["card_txn_count_24h_stream"] > 0).sum())
    # 5. rows with 7d > 0
    c_7d_gt_0 = int((merged["card_txn_count_7d_stream"] > 0).sum())
    # 6. rows with 24h count != prior count
    c_24h_ne_prior = int((merged["card_txn_count_24h_stream"] != merged["card_prior_txn_count_stream"]).sum())
    # 7. rows in same-card same-DT tie groups
    tie_groups = merged.groupby(["card_proxy_id_stream", "TransactionDT_stream"]).size()
    c_tie_rows = int(tie_groups[tie_groups > 1].sum())

    print(f"  Rows with card_prior_txn_count >= 3:      {c_prior_gte_3:>6} ({(c_prior_gte_3 / TOTAL_ROWS) * 100:.2f}%)")
    print(f"  Rows with amt_zscore_vs_card non-null:    {c_zscore_nonnull:>6} ({(c_zscore_nonnull / TOTAL_ROWS) * 100:.2f}%)")
    print(f"  Rows with card_txn_count_1h > 0:          {c_1h_gt_0:>6} ({(c_1h_gt_0 / TOTAL_ROWS) * 100:.2f}%)")
    print(f"  Rows with card_txn_count_24h > 0:         {c_24h_gt_0:>6} ({(c_24h_gt_0 / TOTAL_ROWS) * 100:.2f}%)")
    print(f"  Rows with card_txn_count_7d > 0:          {c_7d_gt_0:>6} ({(c_7d_gt_0 / TOTAL_ROWS) * 100:.2f}%)")
    print(f"  Rows with card_txn_count_24h != prior:    {c_24h_ne_prior:>6} ({(c_24h_ne_prior / TOTAL_ROWS) * 100:.2f}%)")
    print(f"  Rows in same-card same-DT tie groups:     {c_tie_rows:>6} ({(c_tie_rows / TOTAL_ROWS) * 100:.2f}%)")
    print("=" * 80)

    assert c_prior_gte_3 > 0, "Non-vacuity failure: expected rows with prior_count >= 3"
    assert c_zscore_nonnull > 0, "Non-vacuity failure: expected rows with non-null zscore"
    assert c_1h_gt_0 > 0, "Non-vacuity failure: expected rows with 1h > 0"
    assert c_24h_gt_0 > 0, "Non-vacuity failure: expected rows with 24h > 0"
    assert c_7d_gt_0 > 0, "Non-vacuity failure: expected rows with 7d > 0"
    assert c_tie_rows > 0, "Non-vacuity failure: expected same-card same-DT tie rows"

    print("Gate 1.3 PASSED: 100% PARITY ACROSS ALL 483 COLUMNS AT 1e-6 TOLERANCE WITH POSITIVE NON-VACUITY COUNTS!")
    print("=" * 80)


if __name__ == "__main__":
    main()
