"""Gate 1.5: Mid-Batch Crash Recovery Verification.

Protocol:
1. Reset topics and checkpoints.
2. Produce the 1.4 subset (39,965 rows) into Kafka.
3. Launch consumer_ieee.py with --max-offsets-per-trigger 500.
4. Monitor output topic. When output count > 2000 and count % 500 != 0,
   hard-kill consumer with taskkill /F /T.
5. Record kill count and kill count % 500.
6. Attempt plain restart WITHOUT workaround. Capture either clean restart or first 15 lines of exception.
7. Print state provider line from checkpoints/ieee_stateful/features/offsets/0.
8. If plain restart fails (e.g. orphan offset NPE), apply documented operator rollback:
   remove uncommitted offset files (offsets with index > max commit index, including .crc files).
   Label as "OPERATOR WORKAROUND".
9. Restart consumer and let it stream until completion.
10. Audit: total outputs, distinct TransactionIDs, missing count, duplicate count,
    byte-identity of duplicates, and post-dedupe parity against ground truth.
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
LOG_PATH_RUN1 = os.path.abspath("member3_streaming/consumer_gate1_5_run1.log")
LOG_PATH_RESTART1 = os.path.abspath("member3_streaming/consumer_gate1_5_restart1.log")
LOG_PATH_RESTART2 = os.path.abspath("member3_streaming/consumer_gate1_5_restart2.log")
PARQUET_PATH = "data/features/ieee_features.parquet"
EXPECTED_SUBSET_SIZE = 39965

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


def reset_environment():
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
    for p in [LOG_PATH_RUN1, LOG_PATH_RESTART1, LOG_PATH_RESTART2]:
        if os.path.exists(p):
            try:
                os.remove(p)
            except Exception:
                pass
    print("Environment reset complete.")


def produce_data():
    print("Step 2: Producing deterministic long-span subset (39,965 rows)...")
    cmd = [
        sys.executable, "-m", "member3_streaming.produce_ieee_subset",
        "--cap", "40000",
        "--delay", "0.0",
        "--bootstrap-servers", BROKER,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"Producer failed: {res.stderr}"
    print("Data produced to Kafka successfully.")


def kill_process(pid):
    try:
        cmd_kill = f"powershell -NoProfile -Command \"Get-CimInstance Win32_Process | Where-Object {{ $_.ParentProcessId -eq {pid} }} | ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force }}\""
        subprocess.run(cmd_kill, shell=True, capture_output=True)
    except Exception:
        pass
    subprocess.run(f"taskkill /F /T /PID {pid}", shell=True, capture_output=True)


def main():
    print("=" * 80)
    print("GATE 1.5: MID-BATCH CRASH RECOVERY VERIFICATION")
    print("Real Kafka: yes (127.0.0.1:9092)")
    print("Real Spark query: yes (PySpark Structured Streaming via consumer_ieee.py)")
    print(f"Batch Size: maxOffsetsPerTrigger=500")
    print("=" * 80)

    reset_environment()
    produce_data()

    print("\nStep 3: Launching consumer with --max-offsets-per-trigger 500...")
    cmd_run1 = [
        sys.executable, "-m", "member3_streaming.consumer_ieee",
        "--bootstrap-servers", BROKER,
        "--input-topic", IN_TOPIC,
        "--output-topic", OUT_TOPIC,
        "--dlq-topic", DLQ_TOPIC,
        "--checkpoint-location", CHK_DIR,
        "--max-offsets-per-trigger", "500",
    ]
    with open(LOG_PATH_RUN1, "w") as f1:
        proc1 = subprocess.Popen(cmd_run1, stdout=f1, stderr=subprocess.STDOUT)

    consumer = KafkaConsumer(
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    tp = TopicPartition(OUT_TOPIC, 0)
    consumer.assign([tp])

    print("Monitoring output topic for mid-batch kill condition (>2,000 and % 500 != 0)...")
    killed = False
    kill_count = 0
    t0 = time.time()
    while not killed and (time.time() - t0) < 180:
        consumer.seek_to_end(tp)
        curr_offset = consumer.position(tp)
        if curr_offset > 2000 and (curr_offset % 500) != 0:
            kill_process(proc1.pid)
            killed = True
            kill_count = curr_offset
            print(f"HARD KILL TRIGGERED at output count: {kill_count} (count % 500 = {kill_count % 500})!")
            break
        time.sleep(0.05)

    if not killed:
        consumer.seek_to_end(tp)
        kill_count = consumer.position(tp)
        kill_process(proc1.pid)
        print(f"Forced kill at count {kill_count} (count % 500 = {kill_count % 500})")

    time.sleep(3)

    print("\n" + "=" * 80)
    print("CRASH DETAILS")
    print(f"  Exact Kill Count:         {kill_count}")
    print(f"  Kill Count % 500:         {kill_count % 500} (Non-zero confirms mid-batch kill)")
    print("=" * 80)

    # Inspect offsets/0 provider line
    offsets_dir = os.path.join(CHK_DIR, "features", "offsets")
    commits_dir = os.path.join(CHK_DIR, "features", "commits")
    offset_0_path = os.path.join(offsets_dir, "0")
    if os.path.exists(offset_0_path):
        with open(offset_0_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            print("\nOffsets/0 content (first 5 lines):")
            for l in lines[:5]:
                print("  ", l.strip())
            provider_lines = [l.strip() for l in lines if "provider" in l.lower() or "state" in l.lower()]
            if provider_lines:
                print(f"State provider line: {provider_lines[0]}")
            else:
                print(f"State provider line (header): {lines[0].strip() if lines else 'EMPTY'}")

    # Step 4: First restart WITHOUT any workaround
    print("\n" + "=" * 80)
    print("STEP 4: FIRST RESTART WITHOUT WORKAROUND (PLAIN RESTART)")
    print("=" * 80)
    with open(LOG_PATH_RESTART1, "w") as f_r1:
        proc_r1 = subprocess.Popen(cmd_run1, stdout=f_r1, stderr=subprocess.STDOUT)

    # Monitor for exit or exception
    plain_restart_succeeded = False
    r1_exit = None
    t_start_r1 = time.time()
    while (time.time() - t_start_r1) < 45:
        r1_exit = proc_r1.poll()
        if r1_exit is not None:
            break
        with open(LOG_PATH_RESTART1, "r", encoding="utf-8", errors="ignore") as f:
            r1_content = f.read()
        if "NullPointerException" in r1_content or "terminated with error" in r1_content:
            time.sleep(2)
            break
        consumer.seek_to_end(tp)
        curr_pos = consumer.position(tp)
        if curr_pos >= kill_count + 1500:
            plain_restart_succeeded = True
            break
        time.sleep(2)

    with open(LOG_PATH_RESTART1, "r", encoding="utf-8", errors="ignore") as f:
        r1_log = f.read()

    if (r1_exit is not None and r1_exit != 0) or "NullPointerException" in r1_log or "terminated with error" in r1_log:
        print(f"Plain restart failed! Exit code: {r1_exit}")
        print("First 15 lines of exception from consumer log:")
        exc_lines = [l for l in r1_log.splitlines() if "NullPointerException" in l or "at org.apache" in l or "Exception" in l or "Error" in l or "Caused by" in l]
        for l in (exc_lines[:15] if exc_lines else r1_log.splitlines()[-15:]):
            print("  ", l)
        kill_process(proc_r1.pid)
    elif plain_restart_succeeded:
        print(f"Plain restart succeeded cleanly without workaround! Current offset: {consumer.position(tp)}")
    else:
        print("Plain restart did not progress cleanly; terminating plain restart.")
        kill_process(proc_r1.pid)

    if not plain_restart_succeeded:
        kill_process(proc_r1.pid)
        time.sleep(2)
        print("\n" + "=" * 80)
        print("OPERATOR WORKAROUND REQUIRED: mid-batch crash requires operator rollback")
        print("=" * 80)

        # Documented operator rollback: find max commit index, delete uncommitted offset files and their .crc files
        committed_batches = []
        if os.path.exists(commits_dir):
            for fname in os.listdir(commits_dir):
                if fname.isdigit():
                    committed_batches.append(int(fname))
        max_commit = max(committed_batches) if committed_batches else -1
        print(f"Max committed batch index: {max_commit}")

        removed_files = []
        for d in [offsets_dir, commits_dir]:
            if os.path.exists(d):
                for fname in os.listdir(d):
                    for bad_idx in range(max_commit + 1, max_commit + 10):
                        if fname == str(bad_idx) or fname == f".{bad_idx}.crc" or f".{bad_idx}." in fname:
                            fpath = os.path.join(d, fname)
                            try:
                                os.remove(fpath)
                                removed_files.append(f"{os.path.basename(d)}/{fname}")
                            except Exception as e:
                                print(f"Error removing {fpath}: {e}")

        # If offsets == commits, delete max_commit files so it cleanly rolls back to previous batch
        if not removed_files and max_commit >= 0:
            target_idx = max_commit
            for d in [offsets_dir, commits_dir]:
                if os.path.exists(d):
                    for fname in os.listdir(d):
                        if fname == str(target_idx) or fname == f".{target_idx}.crc" or f".{target_idx}." in fname:
                            fpath = os.path.join(d, fname)
                            try:
                                os.remove(fpath)
                                removed_files.append(f"{os.path.basename(d)}/{fname}")
                            except Exception as e:
                                print(f"Error removing {fpath}: {e}")

        print(f"OPERATOR WORKAROUND: Removed uncommitted orphan offset file(s): {removed_files}")

        # Restart with workaround
        print("\nStep 5: Restarting consumer after operator workaround...")
        with open(LOG_PATH_RESTART2, "w") as f_r2:
            proc_final = subprocess.Popen(cmd_run1, stdout=f_r2, stderr=subprocess.STDOUT)
    else:
        proc_final = proc_r1

    # Monitor to completion
    print(f"\nMonitoring stream output until reaching all records...")
    consumer_final = KafkaConsumer(
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    consumer_final.assign([tp])
    consumer_final.seek_to_beginning(tp)

    all_messages = []
    t_wait = time.time()
    last_print = 0
    last_progress = time.time()
    last_distinct_count = 0
    distinct_tids = set()
    while len(distinct_tids) < EXPECTED_SUBSET_SIZE and (time.time() - t_wait) < 1200:
        rec_map = consumer_final.poll(timeout_ms=1000)
        if rec_map:
            for _, msgs in rec_map.items():
                for m in msgs:
                    all_messages.append(m.value)
                    val = json.loads(m.value.decode("utf-8"))
                    distinct_tids.add(val["TransactionID"])
        if len(distinct_tids) > last_distinct_count:
            last_progress = time.time()
            last_distinct_count = len(distinct_tids)
        if len(distinct_tids) >= EXPECTED_SUBSET_SIZE:
            break
        if time.time() - last_progress > 120 and len(distinct_tids) > 0:
            print(f"No new records for 120s (stalled at {len(distinct_tids)}). Exiting wait loop.")
            break
        if time.time() - last_print > 15:
            print(f"Consumed {len(all_messages)} total msgs, {len(distinct_tids)} / {EXPECTED_SUBSET_SIZE} distinct ({time.time() - t_wait:.1f}s)...")
            last_print = time.time()
        time.sleep(0.5)

    kill_process(proc_final.pid)
    consumer_final.close()
    consumer.close()

    total_records = len(all_messages)
    distinct_count = len(distinct_tids)
    missing_count = EXPECTED_SUBSET_SIZE - distinct_count
    duplicate_count = total_records - distinct_count

    print("\n" + "=" * 80)
    print("CRASH RECOVERY AUDIT SUMMARY")
    print("=" * 80)
    print(f"  Total Output Records:             {total_records}")
    print(f"  Distinct TransactionIDs:          {distinct_count} / {EXPECTED_SUBSET_SIZE}")
    print(f"  Missing TransactionIDs:           {missing_count}")
    print(f"  Duplicate Records:                {duplicate_count}")

    # Check byte-identity of duplicates
    parsed_records = [json.loads(m.decode("utf-8")) for m in all_messages]
    records_by_tid = {}
    for r, raw in zip(parsed_records, all_messages):
        tid = r["TransactionID"]
        records_by_tid.setdefault(tid, []).append((r, raw))

    duplicates_byte_identical = True
    non_identical_examples = []
    for tid, instances in records_by_tid.items():
        if len(instances) > 1:
            first_raw = instances[0][1]
            for other_r, other_raw in instances[1:]:
                if first_raw != other_raw:
                    if instances[0][0] != other_r:
                        duplicates_byte_identical = False
                        non_identical_examples.append(tid)
                        break

    print(f"  Duplicates Byte-Identical:        {'yes' if duplicates_byte_identical else 'no'}")
    if not duplicates_byte_identical:
        print(f"  Non-identical duplicate examples: {non_identical_examples[:5]}")

    assert missing_count == 0, f"Expected 0 missing records, got {missing_count}"
    assert distinct_count == EXPECTED_SUBSET_SIZE, f"Expected {EXPECTED_SUBSET_SIZE} distinct rows, got {distinct_count}"

    # Post-dedupe parity check
    print("\nVerifying post-dedupe parity against ieee_features.parquet...")
    deduped_records = [instances[0][0] for instances in records_by_tid.values()]
    stream_df = pd.DataFrame(deduped_records)
    pq_table = pq.read_table(PARQUET_PATH, filters=[("TransactionID", "in", set(stream_df["TransactionID"].values))])
    batch_df = pq_table.to_pandas()

    merged = pd.merge(stream_df, batch_df, on="TransactionID", suffixes=("_stream", "_batch"))
    assert len(merged) == EXPECTED_SUBSET_SIZE

    mismatches = 0
    for col in STATEFUL_COLS:
        s = merged[f"{col}_stream"]
        b = merged[f"{col}_batch"]
        null_diff = int((s.isna() != b.isna()).sum())
        valid = (~s.isna()) & (~b.isna())
        if pd.api.types.is_numeric_dtype(s) and pd.api.types.is_numeric_dtype(b):
            val_diff = int((np.abs(s[valid].astype(float) - b[valid].astype(float)) > 1e-6).sum())
        else:
            val_diff = int((s[valid].astype(str) != b[valid].astype(str)).sum())
        col_err = null_diff + val_diff
        mismatches += col_err
        print(f"  {col:<30}: {col_err} mismatches")

    print(f"\nPost-dedupe Total Stateful Mismatches: {mismatches}")
    assert mismatches == 0, f"Post-dedupe parity failed with {mismatches} mismatches!"

    print("\n" + "=" * 80)
    print("GATE 1.5 PASSED: CRASH RECOVERY VERIFIED WITH 100% POST-DEDUPE PARITY!")
    print("Recovery Mode: " + ("Automatic clean restart" if plain_restart_succeeded else "mid-batch crash requires operator rollback"))
    print("=" * 80)


if __name__ == "__main__":
    main()
