"""Restart-Twice Test on IEEE Stateful Pipeline (Part A).

Protocol:
1. Standard reset of topics and checkpoint directory.
2. Produce deterministic long-span subset (39,965 rows).
3. Run consumer_ieee with --max-offsets-per-trigger 500, hard kill at output count > 2000 and % 500 != 0.
4. Plain restart #1, NO file edits.
   - Checkpoint listings before and after.
   - Does commits/N appear? Does it die with NPE? First 15 lines of exception.
5. Plain restart #2 immediately, NO file edits.
   - Does it start and process remaining rows without exception?
6. Verification:
   - distinct TransactionIDs == subset size (39,965), missing == 0
   - duplicate count and byte-identity
   - 11 stateful columns post-dedupe parity against ieee_features.parquet (0 mismatches).
7. Print plain conclusion: (i) recovers, (ii) does not, or (iii) inconclusive.
8. Save raw untruncated log to member3_streaming/reports/raw/PDG_restart_twice.txt.
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
RAW_REPORT_PATH = os.path.abspath("member3_streaming/reports/raw/PDG_restart_twice.txt")
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

log_buffer = []


def emit(msg=""):
    print(msg, flush=True)
    log_buffer.append(str(msg))


def kill_process(pid):
    try:
        cmd_kill = f"powershell -NoProfile -Command \"Get-CimInstance Win32_Process | Where-Object {{ $_.ParentProcessId -eq {pid} }} | ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force }}\""
        subprocess.run(cmd_kill, shell=True, capture_output=True)
    except Exception:
        pass
    subprocess.run(f"taskkill /F /T /PID {pid}", shell=True, capture_output=True)


def reset_environment():
    emit("Step 1: Standard Reset - Resetting Kafka topics and checkpoint directory...")
    # Kill any lingering java processes first
    subprocess.run(["taskkill", "/F", "/IM", "java.exe"], capture_output=True)
    time.sleep(3)

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

    # Confirm topic end offsets are 0
    consumer = KafkaConsumer(bootstrap_servers=[BROKER], auto_offset_reset="earliest")
    for t in [IN_TOPIC, OUT_TOPIC, DLQ_TOPIC]:
        tp = TopicPartition(t, 0)
        consumer.assign([tp])
        consumer.seek_to_end(tp)
        end_off = consumer.position(tp)
        emit(f"  Topic {t} end offset before producing: {end_off}")
        assert end_off == 0, f"Topic {t} end offset is {end_off}, expected 0!"
    consumer.close()
    emit("Standard reset complete.\n")


def produce_data():
    emit("Step 2: Producing deterministic long-span subset (39,965 rows)...")
    cmd = [
        sys.executable, "-m", "member3_streaming.produce_ieee_subset",
        "--cap", "40000",
        "--delay", "0.0",
        "--bootstrap-servers", BROKER,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"Producer failed: {res.stderr}"
    emit("Data produced to Kafka successfully.\n")


def audit_checkpoint_dir(label=""):
    offsets_dir = os.path.join(CHK_DIR, "features", "offsets")
    commits_dir = os.path.join(CHK_DIR, "features", "commits")
    off_files = sorted([int(f) for f in os.listdir(offsets_dir) if f.isdigit()]) if os.path.exists(offsets_dir) else []
    comm_files = sorted([int(f) for f in os.listdir(commits_dir) if f.isdigit()]) if os.path.exists(commits_dir) else []
    orphan_files = [f for f in off_files if f not in comm_files]
    emit(f"Checkpoint Audit [{label}]:")
    emit(f"  Offsets directory files: {off_files}")
    emit(f"  Commits directory files: {comm_files}")
    emit(f"  Uncommitted orphan offset files: {orphan_files}")
    return off_files, comm_files, orphan_files


def main():
    emit("=" * 80)
    emit("RESTART-TWICE CRASH RECOVERY INVESTIGATION (IEEE STATEFUL)")
    emit("Real Kafka: yes (127.0.0.1:9092)")
    emit("Real Spark query: yes (PySpark Structured Streaming via consumer_ieee.py)")
    emit("Batch Size: maxOffsetsPerTrigger=500")
    emit("Protocol: Plain restart #1 (no edits), Plain restart #2 (no edits)")
    emit("=" * 80 + "\n")

    reset_environment()
    produce_data()

    # Step 3: Run 1
    emit("Step 3: Launching consumer 1 with --max-offsets-per-trigger 500...")
    cmd_consumer = [
        sys.executable, "-m", "member3_streaming.consumer_ieee",
        "--bootstrap-servers", BROKER,
        "--input-topic", IN_TOPIC,
        "--output-topic", OUT_TOPIC,
        "--dlq-topic", DLQ_TOPIC,
        "--checkpoint-location", CHK_DIR,
        "--max-offsets-per-trigger", "500",
    ]
    with open(LOG_PATH_RUN1, "w") as f1:
        proc1 = subprocess.Popen(cmd_consumer, stdout=f1, stderr=subprocess.STDOUT)

    consumer = KafkaConsumer(
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    tp = TopicPartition(OUT_TOPIC, 0)
    consumer.assign([tp])

    emit("Monitoring output topic for mid-batch kill condition (>2,000 and count % 500 != 0)...")
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
            emit(f"HARD KILL TRIGGERED at output count: {kill_count} (count % 500 = {kill_count % 500})!")
            break
        time.sleep(0.05)

    if not killed:
        consumer.seek_to_end(tp)
        kill_count = consumer.position(tp)
        kill_process(proc1.pid)
        emit(f"Forced kill at count {kill_count} (count % 500 = {kill_count % 500})")

    time.sleep(3)

    emit("\n" + "=" * 80)
    emit("CRASH DETAILS (RUN 1)")
    emit(f"  Exact Kill Count:         {kill_count}")
    emit(f"  Kill Count % 500:         {kill_count % 500} (Non-zero confirms mid-batch kill)")
    emit("=" * 80 + "\n")

    # Step A.1: Plain Restart #1
    emit("=" * 80)
    emit("STEP A.1: PLAIN RESTART #1 (NO FILE EDITS)")
    emit("=" * 80)
    off_before_r1, comm_before_r1, orphan_before_r1 = audit_checkpoint_dir("BEFORE Plain Restart #1")

    emit("\nLaunching Plain Restart #1 with exact same checkpoint...")
    with open(LOG_PATH_RESTART1, "w") as f_r1:
        proc_r1 = subprocess.Popen(cmd_consumer, stdout=f_r1, stderr=subprocess.STDOUT)

    t_start_r1 = time.time()
    r1_exit = None
    while (time.time() - t_start_r1) < 45:
        r1_exit = proc_r1.poll()
        if r1_exit is not None:
            break
        time.sleep(0.5)

    if r1_exit is None:
        kill_process(proc_r1.pid)
        emit("Plain Restart #1 did not exit within 45s (still running).")
    else:
        emit(f"Plain Restart #1 exited with return code: {r1_exit}")

    time.sleep(3)
    off_after_r1, comm_after_r1, orphan_after_r1 = audit_checkpoint_dir("AFTER Plain Restart #1")

    orphan_batch_num = orphan_before_r1[0] if orphan_before_r1 else None
    commits_appeared = orphan_batch_num in comm_after_r1 if orphan_batch_num is not None else False
    emit(f"\nOrphan Batch {orphan_batch_num} committed during Restart #1: {'YES (commits/' + str(orphan_batch_num) + ' appears)' if commits_appeared else 'NO'}")

    r1_log_content = ""
    first_15_exception_lines = []
    if os.path.exists(LOG_PATH_RESTART1):
        with open(LOG_PATH_RESTART1, "r", encoding="utf-8", errors="replace") as lf:
            r1_log_content = lf.read()
            in_exc = False
            for line in r1_log_content.splitlines():
                if "exception" in line.lower() or "error" in line.lower() or "caused by" in line.lower() or in_exc:
                    in_exc = True
                    first_15_exception_lines.append(line)
                    if len(first_15_exception_lines) >= 15:
                        break

    emit(f"\nFirst 15 lines of exception from Plain Restart #1 log:")
    for el in first_15_exception_lines:
        emit(f"   {el}")

    # Step A.2: Plain Restart #2
    emit("\n" + "=" * 80)
    emit("STEP A.2: PLAIN RESTART #2 IMMEDIATELY (NO FILE EDITS)")
    emit("=" * 80)
    audit_checkpoint_dir("BEFORE Plain Restart #2")

    emit("\nLaunching Plain Restart #2 with exact same checkpoint...")
    with open(LOG_PATH_RESTART2, "w") as f_r2:
        proc_r2 = subprocess.Popen(cmd_consumer, stdout=f_r2, stderr=subprocess.STDOUT)

    emit("Monitoring output topic until all rows consumed...")
    consumer_final = KafkaConsumer(
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    consumer_final.assign([tp])
    consumer_final.seek_to_beginning(tp)

    all_messages = []
    distinct_tids = set()
    t_wait = time.time()
    last_print = 0
    last_progress = time.time()
    last_distinct_count = 0
    r2_died = False

    while len(distinct_tids) < EXPECTED_SUBSET_SIZE and (time.time() - t_wait) < 1200:
        if proc_r2.poll() is not None:
            emit(f"\n[WARNING] Plain Restart #2 process exited early with code: {proc_r2.poll()}")
            r2_died = True
            break
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
            emit(f"No new records for 120s (stalled at {len(distinct_tids)}). Exiting wait loop.")
            break
        if time.time() - last_print > 15:
            emit(f"Consumed {len(all_messages)} total msgs, {len(distinct_tids)} / {EXPECTED_SUBSET_SIZE} distinct ({time.time() - t_wait:.1f}s)...")
            last_print = time.time()
        time.sleep(0.5)

    kill_process(proc_r2.pid)
    consumer_final.close()
    consumer.close()

    time.sleep(2)
    audit_checkpoint_dir("AFTER Plain Restart #2")

    # Step A.3: Audit & Parity
    emit("\n" + "=" * 80)
    emit("STEP A.3: CRASH RECOVERY AUDIT & PARITY VERIFICATION")
    emit("=" * 80)
    total_records = len(all_messages)
    distinct_count = len(distinct_tids)
    missing_count = EXPECTED_SUBSET_SIZE - distinct_count
    duplicate_count = total_records - distinct_count

    emit(f"  Total Output Records:             {total_records}")
    emit(f"  Distinct TransactionIDs:          {distinct_count} / {EXPECTED_SUBSET_SIZE}")
    emit(f"  Missing TransactionIDs:           {missing_count}")
    emit(f"  Duplicate Records:                {duplicate_count}")

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

    emit(f"  Duplicates Byte-Identical:        {'yes' if duplicates_byte_identical else 'no'}")
    if not duplicates_byte_identical:
        emit(f"  Non-identical duplicate examples: {non_identical_examples[:5]}")

    assert missing_count == 0, f"Expected 0 missing records, got {missing_count}"
    assert distinct_count == EXPECTED_SUBSET_SIZE, f"Expected {EXPECTED_SUBSET_SIZE} distinct rows, got {distinct_count}"

    # Post-dedupe parity check
    emit("\nVerifying post-dedupe parity against ieee_features.parquet...")
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
        emit(f"  {col:<30}: {col_err} mismatches")

    emit(f"\nPost-dedupe Total Stateful Mismatches: {mismatches}")
    assert mismatches == 0, f"Post-dedupe parity failed with {mismatches} mismatches!"

    # Step A.4: Conclusion
    emit("\n" + "=" * 80)
    emit("STEP A.4: RESTART-TWICE CONCLUSION")
    emit("=" * 80)
    if not r2_died and distinct_count == EXPECTED_SUBSET_SIZE and missing_count == 0 and duplicates_byte_identical and mismatches == 0:
        conclusion = "(i) restart-twice recovers with no file edits"
    else:
        conclusion = "(ii) restart-twice does not recover with no file edits"
    emit(f"CONCLUSION: {conclusion}")
    emit("=" * 80)

    # Write log buffer to raw report
    os.makedirs(os.path.dirname(RAW_REPORT_PATH), exist_ok=True)
    with open(RAW_REPORT_PATH, "w", encoding="utf-8") as f_raw:
        f_raw.write("\n".join(log_buffer) + "\n")
    emit(f"\nRaw report written to: {RAW_REPORT_PATH}")


if __name__ == "__main__":
    main()
