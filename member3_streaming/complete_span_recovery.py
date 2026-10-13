import os
import sys
import time
import json
import subprocess
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from kafka import KafkaConsumer, TopicPartition

def main():
    chk_dir = os.path.abspath('member3_streaming/checkpoints/paysim_span')
    in_topic = 'financial_transactions_paysim_span'
    out_topic = 'financial_features_paysim_span'

    # Check uncommitted offsets and remove if any
    for q in ['features', 'dlq']:
        q_offsets = os.path.join(chk_dir, q, 'offsets')
        q_commits = os.path.join(chk_dir, q, 'commits')
        if os.path.exists(q_offsets) and os.path.exists(q_commits):
            for f in os.listdir(q_offsets):
                if f.isdigit():
                    if not os.path.exists(os.path.join(q_commits, f)):
                        print(f"Removing uncommitted {q}/offsets/{f}...")
                        os.remove(os.path.join(q_offsets, f))
                        crc = os.path.join(q_offsets, f".{f}.crc")
                        if os.path.exists(crc):
                            os.remove(crc)

    cmd = [
        sys.executable, '-u', '-m', 'member3_streaming.consumer_paysim',
        '--bootstrap-servers', 'localhost:9092',
        '--input-topic', in_topic,
        '--output-topic', out_topic,
        '--dlq-topic', 'financial_transactions_paysim_span_dlq',
        '--checkpoint-location', chk_dir,
        '--max-offsets-per-trigger', '500',
    ]
    f_log = open('member3_streaming/consumer_span_finish.log', 'w', encoding='utf-8')
    p = subprocess.Popen(cmd, stdout=f_log, stderr=subprocess.STDOUT)
    print(f"Started span consumer (PID: {p.pid}). Monitoring output offset...")

    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
    tp = TopicPartition(out_topic, 0)
    offsets_dir = os.path.join(chk_dir, 'features', 'offsets')
    commits_dir = os.path.join(chk_dir, 'features', 'commits')

    t0 = time.time()
    while time.time() - t0 < 180:
        time.sleep(3)
        curr = consumer.end_offsets([tp]).get(tp, 0)
        print(f"  Current span offset: {curr}...")
        if os.path.exists(offsets_dir):
            nums = sorted([int(f) for f in os.listdir(offsets_dir) if f.isdigit()])
            if nums:
                latest = nums[-1]
                if os.path.exists(os.path.join(commits_dir, str(latest))):
                    with open(os.path.join(offsets_dir, str(latest)), 'r') as f:
                        data = f.read()
                        if '"0":3979' in data:
                            print(f"Committed all 3,979 span transactions at batch {latest}!")
                            time.sleep(3)
                            break

    consumer.close()
    subprocess.run(['taskkill', '/F', '/T', '/PID', str(p.pid)], capture_output=True)
    f_log.close()
    time.sleep(2)

    # Audit records & parity vs ground truth
    print("\n--- Auditing Span Output Records & Ground Truth Parity ---")
    c = KafkaConsumer(
        out_topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
    )
    records = []
    seen = {}
    duplicates = []
    byte_identical = True

    for msg in c:
        val_str = msg.value.decode('utf-8')
        row = json.loads(val_str)
        tid = row['txn_id']
        records.append(row)
        if tid in seen:
            duplicates.append(row)
            if seen[tid] != val_str:
                byte_identical = False
        else:
            seen[tid] = val_str
    c.close()

    total_records = len(records)
    distinct_count = len(seen)
    dup_count = len(duplicates)

    print(f"Total Span Records Received: {total_records}")
    print(f"Distinct txn_id Count      : {distinct_count} (expected 3979)")
    print(f"Duplicate Count            : {dup_count}")
    print(f"All Duplicates Byte-Identical: {byte_identical}")

    assert distinct_count == 3979, f"Distinct count {distinct_count} != 3979!"
    assert byte_identical, "Duplicate records were not byte-identical!"

    # Join vs parquet ground truth
    batch_df = pq.read_table("data/features/paysim_features.parquet").to_pandas()
    stream_df = pd.DataFrame([json.loads(s) for s in seen.values()])
    joined = pd.merge(stream_df, batch_df, on="txn_id", suffixes=("_stream", "_batch"))
    missing = 3979 - len(joined)
    print(f"Matched Ground Truth Rows: {len(joined)} / 3979, Missing: {missing}")
    assert missing == 0, f"Missing {missing} transactions from ground truth!"

    # Window expiry verification
    c24_diff = int((joined["dest_txn_count_prev_24h_stream"] != joined["dest_prior_txn_count_stream"]).sum())
    c168_diff = int((joined["dest_txn_count_prev_168h_stream"] != joined["dest_prior_txn_count_stream"]).sum())
    print(f"Window expiry checks: 24h expired={c24_diff}, 168h expired={c168_diff}")
    assert c24_diff > 0, "24h window expiry not exercised!"
    assert c168_diff > 0, "168h window expiry not exercised!"

    # Compare stateful features
    stateful_features = [
        "dest_prior_txn_count", "dest_first_seen_flag",
        "dest_prior_amount_sum", "dest_prior_amount_mean", "dest_prior_amount_std",
        "dest_prior_amount_max", "dest_steps_since_last_txn", "dest_amount_zscore",
        "dest_txn_count_prev_24h", "dest_txn_count_prev_168h",
        "dest_amount_sum_prev_24h", "dest_amount_sum_prev_168h",
        "dest_prior_cashout_share",
    ]

    total_mismatches = 0
    print("\nStateful Column Parity Summary vs Ground Truth:")
    for col in stateful_features:
        s_vals = joined[f"{col}_stream"]
        b_vals = joined[f"{col}_batch"]
        null_mask = s_vals.isna() != b_vals.isna()
        valid_mask = (~s_vals.isna()) & (~b_vals.isna())
        val_mask = np.abs(s_vals[valid_mask].astype(float) - b_vals[valid_mask].astype(float)) > 1e-4
        cnt = int(null_mask.sum() + val_mask.sum())
        total_mismatches += cnt
        status = "PASS" if cnt == 0 else f"FAIL ({cnt})"
        print(f"  {col:32s}: {status}")

    print(f"\nTOTAL MISMATCHES ACROSS STATEFUL COLUMNS: {total_mismatches}")
    assert total_mismatches == 0, f"Stateful parity failed with {total_mismatches} mismatches!"

    print("\n=======================================================")
    print("CHUNK 2R ITEM 9 (LONG-SPAN CRASH RECOVERY): 100% PASSED!")
    print("=======================================================")

if __name__ == '__main__':
    main()
