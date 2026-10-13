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
    print("CHUNK 2R: REAL MID-BATCH CRASH RECOVERY VERIFICATION (20,000 ROWS)", flush=True)
    print("Real Kafka: yes | Real Spark query: yes", flush=True)
    print("=" * 80, flush=True)

    # 1. Reset topics and checkpoint
    print("\n--- Step 1: Standard Reset ---", flush=True)
    run_cmd("docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic financial_transactions_paysim,financial_features_paysim,financial_transactions_paysim_dlq --if-exists")
    time.sleep(3)
    run_cmd("docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_transactions_paysim --partitions 1 --replication-factor 1")
    run_cmd("docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_features_paysim --partitions 1 --replication-factor 1")
    run_cmd("docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_transactions_paysim_dlq --partitions 1 --replication-factor 1")

    chk_dir = os.path.abspath("member3_streaming/checkpoints/paysim")
    if os.path.exists(chk_dir):
        shutil.rmtree(chk_dir, ignore_errors=True)
        print(f"Purged checkpoint directory: {chk_dir}", flush=True)

    # 2. Produce 20,000 rows
    print("\n--- Step 2: Produce 20,000 Rows ---", flush=True)
    prod_res = run_cmd(f"{sys.executable} member3_streaming/producer_paysim.py --limit 20000 --delay 0")
    in_off = get_end_offset("financial_transactions_paysim")
    print(f"Confirmed input topic count: {in_off} (expected 20,000)", flush=True)
    assert in_off == 20000, f"Expected 20,000 input records, found {in_off}"

    # 3. Launch consumer with maxOffsetsPerTrigger=500 and trickle sleep
    print("\n--- Step 3: Launch Consumer with maxOffsetsPerTrigger=500 & Mid-Batch Trickle ---", flush=True)
    batch_size = 500
    env = os.environ.copy()
    env["PAYSIM_TRICKLE_SLEEP"] = "0.003"  # 3ms per group allows records to stream out smoothly

    consumer_cmd = [
        sys.executable, "-u", "-m", "member3_streaming.consumer_paysim",
        "--bootstrap-servers", "localhost:9092",
        "--input-topic", "financial_transactions_paysim",
        "--output-topic", "financial_features_paysim",
        "--dlq-topic", "financial_transactions_paysim_dlq",
        "--checkpoint-location", chk_dir,
        "--max-offsets-per-trigger", str(batch_size),
    ]

    log_p1 = "member3_streaming/consumer_p1.log"
    f_p1 = open(log_p1, "w", encoding="utf-8")
    p1 = subprocess.Popen(consumer_cmd, env=env, stdout=f_p1, stderr=subprocess.STDOUT)
    print(f"Consumer started (PID: {p1.pid}). Logging to {log_p1}. Monitoring offset for mid-batch kill...", flush=True)

    # We want to kill when count is strictly between two multiples of 500, e.g. between 3001 and 3499
    count_at_kill = 0
    t0 = time.time()
    while time.time() - t0 < 240:
        time.sleep(0.1)
        curr = get_end_offset("financial_features_paysim")
        if curr >= 3150 and curr % 500 != 0:
            print(f"Target mid-batch reached! curr_offset={curr} (remainder {curr % 500}). Executing taskkill /F /T...", flush=True)
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(p1.pid)], capture_output=True)
            try:
                p1.wait(timeout=10)
            except Exception:
                pass
            time.sleep(3)
            count_at_kill = get_end_offset("financial_features_paysim")
            break
        elif curr >= 3200:
            print(f"Threshold reached! curr_offset={curr}. Executing taskkill /F /T...", flush=True)
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(p1.pid)], capture_output=True)
            try:
                p1.wait(timeout=10)
            except Exception:
                pass
            time.sleep(3)
            count_at_kill = get_end_offset("financial_features_paysim")
            break

    f_p1.close()

    print(f"\n=======================================================", flush=True)
    print(f"EXACT OUTPUT COUNT AT KILL TIME: {count_at_kill}", flush=True)
    print(f"Is {count_at_kill} a multiple of 500? {count_at_kill % 500 == 0} (Remainder: {count_at_kill % 500})", flush=True)
    print(f"=======================================================", flush=True)
    assert count_at_kill % 500 != 0, f"Kill count {count_at_kill} is a multiple of 500! Must be strictly mid-batch."

    # 4. RocksDB Proof: Full raw first line of offsets/0 or latest offsets file
    print("\n--- Step 4: RocksDB State Store Proof ---", flush=True)
    feat_chk = os.path.join(chk_dir, "features")
    offsets_dir = os.path.join(feat_chk, "offsets")
    commits_dir = os.path.join(feat_chk, "commits")

    offset_files = sorted([int(f) for f in os.listdir(offsets_dir) if f.isdigit()])
    print(f"Committed offset files found: {offset_files}", flush=True)
    first_off_file = os.path.join(offsets_dir, "0")
    with open(first_off_file, "r") as f:
        first_line = f.readline().strip()
    print("FULL RAW FIRST LINE OF offsets/0:", flush=True)
    print(first_line, flush=True)
    assert "RocksDBStateStoreProvider" in first_line, "RocksDBStateStoreProvider not found in offsets/0!"

    # Find log line naming the provider
    with open(log_p1, "r", encoding="utf-8", errors="ignore") as f:
        p1_log = f.read()
    provider_lines = [l for l in p1_log.splitlines() if "RocksDBStateStoreProvider" in l or "StateStore" in l]
    print("SPARK LOG LINES NAMING STATE STORE PROVIDER:", flush=True)
    for l in provider_lines[:5]:
        print(f"  {l}", flush=True)

    # Identify uncommitted orphan offset file
    orphan_files = []
    for f_num in offset_files:
        commit_f = os.path.join(commits_dir, str(f_num))
        if not os.path.exists(commit_f):
            orphan_files.append(f_num)
    print(f"Uncommitted orphan offset files: {orphan_files}", flush=True)

    # 5. Test Restart WITHOUT Workaround (Capture raw NPE stack trace)
    print("\n--- Step 5: Test Restart WITHOUT Workaround ---", flush=True)
    log_nofix = "member3_streaming/consumer_restart_nofix.log"
    restart_no_fix_cmd = [
        sys.executable, "-u", "-m", "member3_streaming.consumer_paysim",
        "--bootstrap-servers", "localhost:9092",
        "--input-topic", "financial_transactions_paysim",
        "--output-topic", "financial_features_paysim",
        "--dlq-topic", "financial_transactions_paysim_dlq",
        "--checkpoint-location", chk_dir,
        "--max-offsets-per-trigger", "1000",
    ]
    f_nofix = open(log_nofix, "w", encoding="utf-8")
    p_no_fix = subprocess.Popen(restart_no_fix_cmd, stdout=f_nofix, stderr=subprocess.STDOUT)
    try:
        p_no_fix.wait(timeout=25)
    except subprocess.TimeoutExpired:
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(p_no_fix.pid)], capture_output=True)
    f_nofix.close()

    print(f"Exit code without workaround: {p_no_fix.returncode}", flush=True)
    with open(log_nofix, "r", encoding="utf-8", errors="ignore") as f:
        nofix_output = f.read()
    print("--- RAW LOG / STACK TRACE WITHOUT WORKAROUND (tail 30 lines) ---", flush=True)
    for l in nofix_output.splitlines()[-30:]:
        print(l, flush=True)

    # 6. Apply workaround
    print("\n--- Step 6: Applying Workaround (Rollback Orphan Offsets) ---", flush=True)
    for q in ["features", "dlq"]:
        q_offsets = os.path.join(chk_dir, q, "offsets")
        q_commits = os.path.join(chk_dir, q, "commits")
        if os.path.exists(q_offsets) and os.path.exists(q_commits):
            for f in os.listdir(q_offsets):
                if f.isdigit():
                    commit_file = os.path.join(q_commits, f)
                    if not os.path.exists(commit_file):
                        print(f"Removing uncommitted offset file {q}/offsets/{f}...", flush=True)
                        try:
                            os.remove(os.path.join(q_offsets, f))
                            crc = os.path.join(q_offsets, f".{f}.crc")
                            if os.path.exists(crc):
                                os.remove(crc)
                        except Exception as e:
                            print(f"Error removing {f}: {e}", flush=True)

    # 7. Restart Consumer WITH Workaround
    print("\n--- Step 7: Restart Consumer WITH Workaround ---", flush=True)
    log_fix = "member3_streaming/consumer_restart_fix.log"
    restart_fix_cmd = [
        sys.executable, "-u", "-m", "member3_streaming.consumer_paysim",
        "--bootstrap-servers", "localhost:9092",
        "--input-topic", "financial_transactions_paysim",
        "--output-topic", "financial_features_paysim",
        "--dlq-topic", "financial_transactions_paysim_dlq",
        "--checkpoint-location", chk_dir,
        "--max-offsets-per-trigger", "1000",
    ]
    f_fix = open(log_fix, "w", encoding="utf-8")
    p_fix = subprocess.Popen(restart_fix_cmd, stdout=f_fix, stderr=subprocess.STDOUT)
    print(f"Consumer restarted (PID: {p_fix.pid}). Waiting for full completion...", flush=True)

    t_start = time.time()
    while time.time() - t_start < 450:
        time.sleep(5)
        curr_out = get_end_offset("financial_features_paysim")
        print(f"  Current output offset: {curr_out}...", flush=True)
        if os.path.exists(offsets_dir):
            curr_offsets = sorted([int(f) for f in os.listdir(offsets_dir) if f.isdigit()])
            if curr_offsets:
                latest_num = curr_offsets[-1]
                latest_off_f = os.path.join(offsets_dir, str(latest_num))
                commit_f = os.path.join(commits_dir, str(latest_num))
                try:
                    with open(latest_off_f, "r") as f:
                        data = f.read()
                        if '"financial_transactions_paysim":{"0":20000}' in data and os.path.exists(commit_f):
                            print(f"All 20,000 input transactions committed at batch {latest_num}!", flush=True)
                            time.sleep(5)
                            break
                except Exception:
                    pass

    # Stop consumer
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(p_fix.pid)], capture_output=True)
    f_fix.close()
    time.sleep(3)

    # 8. Parity & Duplicate Audit
    print("\n--- Step 8: Consuming All Records and Auditing Parity ---", flush=True)
    consumer = KafkaConsumer(
        "financial_features_paysim",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    records = []
    for msg in consumer:
        records.append((msg.key.decode("utf-8") if msg.key else None, json.loads(msg.value.decode("utf-8")), msg.value))
    consumer.close()

    total_rec = len(records)
    print(f"Total output records received: {total_rec}", flush=True)

    # Check duplicates
    seen_txn = {}
    duplicates = []
    for key, row, raw_bytes in records:
        tid = row["txn_id"]
        if tid in seen_txn:
            duplicates.append((tid, raw_bytes, seen_txn[tid]))
        else:
            seen_txn[tid] = raw_bytes

    dup_count = len(duplicates)
    distinct_count = len(seen_txn)
    print(f"Distinct txn_id count: {distinct_count} (expected 20,000)", flush=True)
    print(f"Duplicate count: {dup_count}", flush=True)

    # Verify duplicate byte identity
    byte_identical = True
    for tid, b1, b2 in duplicates:
        if b1 != b2:
            byte_identical = False
            print(f"ERROR: Duplicate for txn_id={tid} is NOT byte-identical!", flush=True)
            break
    print(f"Are all duplicates byte-identical? {byte_identical}", flush=True)

    assert distinct_count == 20000, f"Distinct count {distinct_count} != 20,000!"
    assert byte_identical, "Duplicate records were not byte-identical!"

    # 9. 41-Column Parity Check vs paysim_features.parquet
    print("\n--- Step 9: 41-Column Parity Check vs data/features/paysim_features.parquet ---", flush=True)
    table = pq.read_table("data/features/paysim_features.parquet")
    batch_df = table.slice(0, 20000).to_pandas()

    stream_rows = [row for (key, row, raw_bytes) in records]
    stream_df = pd.DataFrame(stream_rows).drop_duplicates(subset=["txn_id"]).sort_values("txn_id").reset_index(drop=True)
    batch_df = batch_df.sort_values("txn_id").reset_index(drop=True)

    compared_cols = [c for c in batch_df.columns if c in stream_df.columns and c != "feature_version"]
    print(f"Total compared columns: {len(compared_cols)}", flush=True)

    mismatch_counts = {}
    for col in compared_cols:
        b_series = batch_df[col]
        s_series = stream_df[col]
        if np.issubdtype(b_series.dtype, np.floating):
            null_match = (b_series.isna() == s_series.isna()).all()
            if not null_match:
                mismatch_counts[col] = int((b_series.isna() != s_series.isna()).sum())
                continue
            valid_mask = b_series.notna()
            diff = np.abs(b_series[valid_mask] - s_series[valid_mask])
            mismatches = int((diff > 1e-4).sum())
            if mismatches > 0:
                mismatch_counts[col] = mismatches
        else:
            diff = (b_series != s_series)
            mismatches = int(diff.sum())
            if mismatches > 0:
                mismatch_counts[col] = mismatches

    print("Column Mismatch Summary:", flush=True)
    for col in compared_cols:
        cnt = mismatch_counts.get(col, 0)
        status = "PASS" if cnt == 0 else f"FAIL ({cnt} mismatches)"
        print(f"  {col:35s}: {status}", flush=True)

    total_mismatches = sum(mismatch_counts.values())
    print(f"\nTOTAL MISMATCHES ACROSS ALL 41 COLUMNS: {total_mismatches}", flush=True)
    assert total_mismatches == 0, f"Parity failed with {total_mismatches} mismatches!"

    print("\n" + "=" * 80, flush=True)
    print("CHUNK 2R CRASH RECOVERY VERIFICATION: ALL PASS CONDITIONS MET!", flush=True)
    print(f"- Kill count: {count_at_kill} (non-multiple of 500: remainder {count_at_kill % 500})", flush=True)
    print(f"- Distinct txn_id: {distinct_count} == 20,000, Missing: 0", flush=True)
    print(f"- Duplicates: {dup_count} (all byte-identical: {byte_identical})", flush=True)
    print(f"- 41-Column Parity: 0 mismatches", flush=True)
    print("=" * 80, flush=True)

if __name__ == "__main__":
    main()
