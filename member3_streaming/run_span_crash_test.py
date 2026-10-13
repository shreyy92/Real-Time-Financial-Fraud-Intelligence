import os
import sys
import shutil
import time
import json
import subprocess
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from kafka import KafkaConsumer, TopicPartition

def run_command(cmd, shell=True):
    print(f"Running: {cmd}")
    res = subprocess.run(cmd, shell=shell, capture_output=True, text=True)
    if res.stdout.strip():
        print(res.stdout.strip())
    if res.stderr.strip():
        print(f"STDERR: {res.stderr.strip()}", file=sys.stderr)
    return res

def get_topic_end_offset(topic):
    consumer = KafkaConsumer(
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    tp = TopicPartition(topic, 0)
    end_offsets = consumer.end_offsets([tp])
    consumer.close()
    return end_offsets.get(tp, 0)

def main():
    print("=" * 80)
    print("PHASE 3 ITEM C: CRASH TEST ON LONG-SPAN KEYS (300 KEYS, 3,979 ROWS)")
    print("=" * 80)

    # 1. Verify input topic and clean output topic & checkpoint
    print("\n--- 1. Checking input topic and resetting output/checkpoint ---")
    in_count = get_topic_end_offset("financial_transactions_paysim_span")
    print(f"Input topic financial_transactions_paysim_span count: {in_count} (expected 3979)")
    if in_count != 3979:
        print(f"ERROR: Expected 3979 input rows, found {in_count}")
        sys.exit(1)

    out_topic = "financial_features_paysim_span"
    dlq_topic = "financial_transactions_paysim_span_dlq"
    chk_dir = os.path.abspath("member3_streaming/checkpoints/paysim_span")

    run_command(f"docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic {out_topic},{dlq_topic} --if-exists")
    time.sleep(2)
    run_command(f"docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic {out_topic} --partitions 1 --replication-factor 1")
    run_command(f"docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic {dlq_topic} --partitions 1 --replication-factor 1")

    if os.path.exists(chk_dir):
        shutil.rmtree(chk_dir, ignore_errors=True)
        print(f"Deleted checkpoint directory: {chk_dir}")

    # 2. Start consumer with mid-run hard kill
    # Total rows: 3979. We kill mid-run at ~1800 rows.
    batch_size = 250
    kill_threshold = 1750
    print(f"\n--- 2. Starting consumer P1 (batch size {batch_size}) to be killed mid-run at ~{kill_threshold} ---")
    consumer_cmd = [
        sys.executable, "-u", "-m", "member3_streaming.consumer_paysim",
        "--bootstrap-servers", "localhost:9092",
        "--input-topic", "financial_transactions_paysim_span",
        "--output-topic", out_topic,
        "--dlq-topic", dlq_topic,
        "--checkpoint-location", chk_dir,
        "--max-offsets-per-trigger", str(batch_size),
    ]

    p1 = subprocess.Popen(consumer_cmd)
    print(f"Consumer P1 started (PID: {p1.pid}). Monitoring offset for mid-run kill...")

    start_mon = time.time()
    count_at_kill = 0
    while time.time() - start_mon < 120:
        time.sleep(1)
        curr_off = get_topic_end_offset(out_topic)
        if curr_off >= kill_threshold:
            print(f"Target threshold reached ({curr_off} records)! Executing taskkill /F /T...")
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(p1.pid)], capture_output=True)
            try:
                p1.wait(timeout=10)
            except Exception:
                pass
            time.sleep(5)
            count_at_kill = get_topic_end_offset(out_topic)
            break

    print(f"\n=======================================================")
    print(f"EXACT SPAN OUTPUT COUNT AT KILL TIME: {count_at_kill}")
    print(f"Mid-run kill executed at record: {count_at_kill} / 3979")
    print(f"=======================================================")

    # Circumvent SPARK-55271 (KafkaMicroBatchStream.metrics NPE on uncommitted re-exec in Spark 4.1.1)
    for q in ["features", "dlq"]:
        q_offsets = os.path.join(chk_dir, q, "offsets")
        q_commits = os.path.join(chk_dir, q, "commits")
        if os.path.exists(q_offsets) and os.path.exists(q_commits):
            for f in os.listdir(q_offsets):
                if f.isdigit():
                    commit_file = os.path.join(q_commits, f)
                    if not os.path.exists(commit_file):
                        print(f"Removing uncommitted offset file {q}/offsets/{f} (SPARK-55271 workaround)...")
                        try:
                            os.remove(os.path.join(q_offsets, f))
                            crc = os.path.join(q_offsets, f".{f}.crc")
                            if os.path.exists(crc):
                                os.remove(crc)
                        except Exception:
                            pass

    # 3. Restart consumer P2 from the same checkpoint
    print("\n--- 3. Restarting consumer P2 from same checkpoint to complete stream ---")
    restart_cmd = [
        sys.executable, "-u", "-m", "member3_streaming.consumer_paysim",
        "--bootstrap-servers", "localhost:9092",
        "--input-topic", "financial_transactions_paysim_span",
        "--output-topic", out_topic,
        "--dlq-topic", dlq_topic,
        "--checkpoint-location", chk_dir,
        "--max-offsets-per-trigger", "500",
    ]

    p2 = subprocess.Popen(restart_cmd)
    print(f"Restarted consumer P2 (PID: {p2.pid}). Monitoring progress...")

    start_wait = time.time()
    commits_dir = os.path.join(chk_dir, "features", "commits")
    offsets_dir = os.path.join(chk_dir, "features", "offsets")
    while time.time() - start_wait < 180:
        time.sleep(5)
        end_offset = get_topic_end_offset(out_topic)
        print(f"  Current span output offset: {end_offset}...")
        if os.path.exists(offsets_dir):
            latest_offsets = sorted([int(f) for f in os.listdir(offsets_dir) if f.isdigit()])
            if latest_offsets:
                latest_off_file = os.path.join(offsets_dir, str(latest_offsets[-1]))
                try:
                    with open(latest_off_file, "r") as f:
                        off_data = f.read()
                        if '"financial_transactions_paysim_span":{"0":3979}' in off_data:
                            latest_num = latest_offsets[-1]
                            commit_file = os.path.join(commits_dir, str(latest_num))
                            if os.path.exists(commit_file):
                                print("Checkpoint reached input offset 3979 and committed! Stream fully processed.")
                                time.sleep(5)
                                break
                except Exception:
                    pass

    # Stop p2 cleanly
    print("Terminating consumer P2 process...")
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(p2.pid)], capture_output=True)
    time.sleep(2)

    # 4. Collect and analyze records
    print("\n--- 4. Collecting and Analyzing Output Records from Kafka ---")
    consumer = KafkaConsumer(
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    tp = TopicPartition(out_topic, 0)
    consumer.assign([tp])
    consumer.seek_to_beginning()

    records = []
    raw_messages_by_txn = {}
    duplicate_records = []
    duplicate_byte_identical = True

    for msg in consumer:
        val_str = msg.value.decode("utf-8")
        val = json.loads(val_str)
        t_id = val["txn_id"]
        records.append(val)

        if t_id in raw_messages_by_txn:
            duplicate_records.append(val)
            if raw_messages_by_txn[t_id] != val_str:
                duplicate_byte_identical = False
        else:
            raw_messages_by_txn[t_id] = val_str

    consumer.close()

    total_records = len(records)
    distinct_txn_ids = len(raw_messages_by_txn)
    duplicate_count = len(duplicate_records)

    print(f"Total Output Records : {total_records}")
    print(f"Distinct txn_ids     : {distinct_txn_ids} (expected 3979)")
    print(f"Duplicate Count      : {duplicate_count}")
    print(f"Duplicates Byte-Identical: {duplicate_byte_identical}")

    # 5. Join vs Parquet Ground Truth & Stateful Parity
    print("\n--- 5. Ground Truth Parity vs data/features/paysim_features.parquet ---")
    distinct_rows = [json.loads(s) for s in raw_messages_by_txn.values()]
    stream_df = pd.DataFrame(distinct_rows)

    batch_df = pq.read_table("data/features/paysim_features.parquet").to_pandas()
    joined = pd.merge(stream_df, batch_df, on="txn_id", suffixes=("_stream", "_batch"))
    missing_txn_ids = 3979 - len(joined)
    print(f"Matched Ground Truth Rows: {len(joined)} / 3979")
    print(f"Missing txn_ids          : {missing_txn_ids}")

    # Window expiry exercise verification
    c24_diff = int((joined["dest_txn_count_prev_24h_stream"] != joined["dest_prior_txn_count_stream"]).sum())
    c168_diff = int((joined["dest_txn_count_prev_168h_stream"] != joined["dest_prior_txn_count_stream"]).sum())
    print("\n--- Window Expiry Verification (Must be > 0) ---")
    print(f"dest_txn_count_prev_24h != dest_prior_txn_count : {c24_diff}")
    print(f"dest_txn_count_prev_168h != dest_prior_txn_count: {c168_diff}")

    stateful_features = [
        "dest_prior_txn_count", "dest_first_seen_flag",
        "dest_prior_amount_sum", "dest_prior_amount_mean", "dest_prior_amount_std",
        "dest_prior_amount_max", "dest_steps_since_last_txn", "dest_amount_zscore",
        "dest_txn_count_prev_24h", "dest_txn_count_prev_168h",
        "dest_amount_sum_prev_24h", "dest_amount_sum_prev_168h",
        "dest_prior_cashout_share",
    ]

    total_mismatches = 0
    print("\nPer-Column Stateful Mismatch Counts vs Parquet Ground Truth:")
    for col in stateful_features:
        s_vals = joined[f"{col}_stream"]
        b_vals = joined[f"{col}_batch"]
        null_mask = s_vals.isna() != b_vals.isna()
        valid_mask = (~s_vals.isna()) & (~b_vals.isna())
        val_mask = np.abs(s_vals[valid_mask].astype(float) - b_vals[valid_mask].astype(float)) > 1e-6
        cnt = int(null_mask.sum() + val_mask.sum())
        total_mismatches += cnt
        print(f"  {col:32s}: {cnt:5d} mismatches")

    print(f"\nTOTAL MISMATCHES ACROSS ALL {len(stateful_features)} STATEFUL COLUMNS: {total_mismatches}")
    if total_mismatches == 0 and missing_txn_ids == 0 and c24_diff > 0 and c168_diff > 0:
        print("\nLONG-SPAN CRASH TEST PASSED PERFECTLY!")
    else:
        print("\nLONG-SPAN CRASH TEST FAILED!")
        sys.exit(1)

if __name__ == "__main__":
    main()
