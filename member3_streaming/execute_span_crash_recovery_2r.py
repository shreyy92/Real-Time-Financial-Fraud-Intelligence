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

def run_cmd(cmd, shell=True):
    print(f"[CMD] {cmd}", flush=True)
    res = subprocess.run(cmd, shell=shell, capture_output=True, text=True)
    if res.stdout.strip():
        print(res.stdout.strip(), flush=True)
    if res.stderr.strip():
        print(f"[STDERR] {res.stderr.strip()}", file=sys.stderr, flush=True)
    return res

def get_end_offset(topic):
    consumer = KafkaConsumer(
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        consumer_timeout_ms=2000,
    )
    tp = TopicPartition(topic, 0)
    end_offsets = consumer.end_offsets([tp])
    consumer.close()
    return end_offsets.get(tp, 0)

def main():
    print("=" * 80, flush=True)
    print("CHUNK 2R ITEM 9: LONG-SPAN KEYS CRASH RECOVERY (300 KEYS, 3,979 ROWS)", flush=True)
    print("Real Kafka: yes | Real Spark query: yes", flush=True)
    print("=" * 80, flush=True)

    in_topic = "financial_transactions_paysim_span"
    out_topic = "financial_features_paysim_span"
    dlq_topic = "financial_transactions_paysim_span_dlq"
    chk_dir = os.path.abspath("member3_streaming/checkpoints/paysim_span")

    # 1. Verify input topic and clean output & checkpoint
    print("\n--- Step 1: Verify Input & Reset Output/Checkpoint ---", flush=True)
    in_off = get_end_offset(in_topic)
    print(f"Confirmed input topic count: {in_off} (expected 3979)", flush=True)
    assert in_off == 3979, f"Expected 3979 input rows, found {in_off}"

    run_cmd(f"docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic {out_topic},{dlq_topic} --if-exists")
    time.sleep(2)
    run_cmd(f"docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic {out_topic} --partitions 1 --replication-factor 1")
    run_cmd(f"docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic {dlq_topic} --partitions 1 --replication-factor 1")

    if os.path.exists(chk_dir):
        shutil.rmtree(chk_dir, ignore_errors=True)
        print(f"Purged checkpoint directory: {chk_dir}", flush=True)

    # 2. Launch consumer with batch_size=250 and mid-batch trickle
    print("\n--- Step 2: Launch Consumer with batch_size=250 & Mid-Batch Trickle ---", flush=True)
    batch_size = 250
    env = os.environ.copy()
    env["PAYSIM_TRICKLE_SLEEP"] = "0.003"

    consumer_cmd = [
        sys.executable, "-u", "-m", "member3_streaming.consumer_paysim",
        "--bootstrap-servers", "localhost:9092",
        "--input-topic", in_topic,
        "--output-topic", out_topic,
        "--dlq-topic", dlq_topic,
        "--checkpoint-location", chk_dir,
        "--max-offsets-per-trigger", str(batch_size),
    ]

    log_p1 = "member3_streaming/consumer_span_p1.log"
    f_p1 = open(log_p1, "w", encoding="utf-8")
    p1 = subprocess.Popen(consumer_cmd, env=env, stdout=f_p1, stderr=subprocess.STDOUT)
    print(f"Span consumer started (PID: {p1.pid}). Monitoring offset for mid-batch kill...", flush=True)

    # Kill mid-batch between 1500 and 1750 (e.g. >= 1600)
    count_at_kill = 0
    t0 = time.time()
    while time.time() - t0 < 180:
        time.sleep(0.1)
        curr = get_end_offset(out_topic)
        if curr >= 1600 and curr % batch_size != 0:
            print(f"Target mid-batch reached! curr_offset={curr} (remainder {curr % batch_size}). Executing taskkill /F /T...", flush=True)
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(p1.pid)], capture_output=True)
            try:
                p1.wait(timeout=10)
            except Exception:
                pass
            time.sleep(3)
            count_at_kill = get_end_offset(out_topic)
            break
        elif curr >= 1650:
            print(f"Threshold reached! curr_offset={curr}. Executing taskkill /F /T...", flush=True)
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(p1.pid)], capture_output=True)
            try:
                p1.wait(timeout=10)
            except Exception:
                pass
            time.sleep(3)
            count_at_kill = get_end_offset(out_topic)
            break

    f_p1.close()

    print(f"\n=======================================================", flush=True)
    print(f"EXACT SPAN OUTPUT COUNT AT KILL TIME: {count_at_kill}", flush=True)
    print(f"Is {count_at_kill} a multiple of {batch_size}? {count_at_kill % batch_size == 0} (Remainder: {count_at_kill % batch_size})", flush=True)
    print(f"=======================================================", flush=True)
    assert count_at_kill % batch_size != 0, f"Kill count {count_at_kill} is a multiple of batch size {batch_size}!"

    # 3. Check checkpoint and test restart without workaround
    print("\n--- Step 3: Checkpoint Audit & Test Restart WITHOUT Workaround ---", flush=True)
    feat_chk = os.path.join(chk_dir, "features")
    offsets_dir = os.path.join(feat_chk, "offsets")
    commits_dir = os.path.join(feat_chk, "commits")

    offset_files = sorted([int(f) for f in os.listdir(offsets_dir) if f.isdigit()])
    commit_files = sorted([int(f) for f in os.listdir(commits_dir) if f.isdigit()])
    print(f"Offset files: {offset_files}", flush=True)
    print(f"Commit files: {commit_files}", flush=True)

    orphan_files = [f for f in offset_files if f not in commit_files]
    print(f"Uncommitted orphan offset files: {orphan_files}", flush=True)

    workaround_needed = False
    if orphan_files:
        print("Testing restart WITHOUT workaround to determine if workaround is needed...", flush=True)
        log_nofix = "member3_streaming/consumer_span_restart_nofix.log"
        restart_nofix_cmd = [
            sys.executable, "-u", "-m", "member3_streaming.consumer_paysim",
            "--bootstrap-servers", "localhost:9092",
            "--input-topic", in_topic,
            "--output-topic", out_topic,
            "--dlq-topic", dlq_topic,
            "--checkpoint-location", chk_dir,
            "--max-offsets-per-trigger", "500",
        ]
        f_nofix = open(log_nofix, "w", encoding="utf-8")
        p_nofix = subprocess.Popen(restart_nofix_cmd, stdout=f_nofix, stderr=subprocess.STDOUT)
        try:
            p_nofix.wait(timeout=25)
        except subprocess.TimeoutExpired:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(p_nofix.pid)], capture_output=True)
        f_nofix.close()

        with open(log_nofix, "r", encoding="utf-8", errors="ignore") as f:
            nofix_out = f.read()
        if "NullPointerException" in nofix_out:
            workaround_needed = True
            print("Restart WITHOUT workaround FAILED with NullPointerException in KafkaMicroBatchStream.metrics! Workaround IS NEEDED.", flush=True)
        else:
            print(f"Restart finished with code {p_nofix.returncode}. Workaround needed: {workaround_needed}", flush=True)

    # 4. Apply workaround if needed
    if workaround_needed or orphan_files:
        print("\n--- Step 4: Applying Workaround (Rollback Orphan Offsets) ---", flush=True)
        for q in ["features", "dlq"]:
            q_offsets = os.path.join(chk_dir, q, "offsets")
            q_commits = os.path.join(chk_dir, q, "commits")
            if os.path.exists(q_offsets) and os.path.exists(q_commits):
                for f in os.listdir(q_offsets):
                    if f.isdigit():
                        commit_f = os.path.join(q_commits, f)
                        if not os.path.exists(commit_f):
                            print(f"Removing uncommitted offset file {q}/offsets/{f}...", flush=True)
                            os.remove(os.path.join(q_offsets, f))
                            crc = os.path.join(q_offsets, f".{f}.crc")
                            if os.path.exists(crc):
                                os.remove(crc)

    # 5. Restart consumer to complete span stream
    print("\n--- Step 5: Restart Consumer WITH Workaround to Completion ---", flush=True)
    log_fix = "member3_streaming/consumer_span_restart_fix.log"
    restart_fix_cmd = [
        sys.executable, "-u", "-m", "member3_streaming.consumer_paysim",
        "--bootstrap-servers", "localhost:9092",
        "--input-topic", in_topic,
        "--output-topic", out_topic,
        "--dlq-topic", dlq_topic,
        "--checkpoint-location", chk_dir,
        "--max-offsets-per-trigger", "500",
    ]
    f_fix = open(log_fix, "w", encoding="utf-8")
    p_fix = subprocess.Popen(restart_fix_cmd, stdout=f_fix, stderr=subprocess.STDOUT)
    print(f"Span consumer restarted (PID: {p_fix.pid}). Waiting for completion...", flush=True)

    t_start = time.time()
    while time.time() - t_start < 300:
        time.sleep(4)
        curr_out = get_end_offset(out_topic)
        print(f"  Current span output offset: {curr_out}...", flush=True)
        if os.path.exists(offsets_dir):
            curr_offsets = sorted([int(f) for f in os.listdir(offsets_dir) if f.isdigit()])
            if curr_offsets:
                latest_num = curr_offsets[-1]
                latest_off_f = os.path.join(offsets_dir, str(latest_num))
                commit_f = os.path.join(commits_dir, str(latest_num))
                try:
                    with open(latest_off_f, "r") as f:
                        data = f.read()
                        if '"0":3979' in data and os.path.exists(commit_f):
                            print(f"All 3,979 span transactions committed at batch {latest_num}!", flush=True)
                            time.sleep(3)
                            break
                except Exception:
                    pass

    subprocess.run(["taskkill", "/F", "/T", "/PID", str(p_fix.pid)], capture_output=True)
    f_fix.close()
    time.sleep(2)

    # 6. Audit records & parity vs ground truth
    print("\n--- Step 6: Consuming All Span Records & Ground Truth Parity ---", flush=True)
    consumer = KafkaConsumer(
        out_topic,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    records = []
    seen = {}
    duplicates = []
    byte_identical = True

    for msg in consumer:
        val_str = msg.value.decode("utf-8")
        row = json.loads(val_str)
        tid = row["txn_id"]
        records.append(row)
        if tid in seen:
            duplicates.append(row)
            if seen[tid] != val_str:
                byte_identical = False
        else:
            seen[tid] = val_str
    consumer.close()

    total_records = len(records)
    distinct_count = len(seen)
    dup_count = len(duplicates)

    print(f"Total Span Records Received: {total_records}", flush=True)
    print(f"Distinct txn_id Count      : {distinct_count} (expected 3979)", flush=True)
    print(f"Duplicate Count            : {dup_count}", flush=True)
    print(f"All Duplicates Byte-Identical: {byte_identical}", flush=True)

    assert distinct_count == 3979, f"Distinct count {distinct_count} != 3979!"
    assert byte_identical, "Duplicate records were not byte-identical!"

    # Join vs parquet ground truth
    batch_df = pq.read_table("data/features/paysim_features.parquet").to_pandas()
    stream_df = pd.DataFrame([json.loads(s) for s in seen.values()])
    joined = pd.merge(stream_df, batch_df, on="txn_id", suffixes=("_stream", "_batch"))
    missing = 3979 - len(joined)
    print(f"Matched Ground Truth Rows: {len(joined)} / 3979, Missing: {missing}", flush=True)
    assert missing == 0, f"Missing {missing} transactions from ground truth!"

    # Window expiry verification
    c24_diff = int((joined["dest_txn_count_prev_24h_stream"] != joined["dest_prior_txn_count_stream"]).sum())
    c168_diff = int((joined["dest_txn_count_prev_168h_stream"] != joined["dest_prior_txn_count_stream"]).sum())
    print(f"Window expiry checks: 24h expired={c24_diff}, 168h expired={c168_diff}", flush=True)
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
    print("Stateful Column Parity Summary vs Ground Truth:", flush=True)
    for col in stateful_features:
        s_vals = joined[f"{col}_stream"]
        b_vals = joined[f"{col}_batch"]
        null_mask = s_vals.isna() != b_vals.isna()
        valid_mask = (~s_vals.isna()) & (~b_vals.isna())
        val_mask = np.abs(s_vals[valid_mask].astype(float) - b_vals[valid_mask].astype(float)) > 1e-4
        cnt = int(null_mask.sum() + val_mask.sum())
        total_mismatches += cnt
        status = "PASS" if cnt == 0 else f"FAIL ({cnt})"
        print(f"  {col:32s}: {status}", flush=True)

    print(f"\nTOTAL MISMATCHES ACROSS STATEFUL COLUMNS: {total_mismatches}", flush=True)
    assert total_mismatches == 0, f"Stateful parity failed with {total_mismatches} mismatches!"

    print("\n" + "=" * 80, flush=True)
    print("CHUNK 2R ITEM 9 (LONG-SPAN CRASH RECOVERY): ALL PASS CONDITIONS MET!", flush=True)
    print(f"- Kill count: {count_at_kill} (remainder {count_at_kill % batch_size} != 0)")
    print(f"- Workaround needed on restart: {workaround_needed}")
    print(f"- Distinct txn_id: 3979 / 3979, Missing: 0")
    print(f"- Duplicates: {dup_count} (all byte-identical: {byte_identical})")
    print(f"- Stateful Parity: 0 mismatches, Window expiry verified (24h={c24_diff}, 168h={c168_diff})")
    print("=" * 80, flush=True)

if __name__ == "__main__":
    main()
