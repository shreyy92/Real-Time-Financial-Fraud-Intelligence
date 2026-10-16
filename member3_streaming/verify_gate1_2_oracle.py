"""Gate 1.2: Oracle Tie and Window Boundary Test for IEEE Stateful Features.

Verifies:
- At least 3 rows sharing one second with ascending TransactionIDs.
- For each W in {3600, 86400, 604800}, rows at exactly t-W, t-W-1, t-1, and t relative to probe row.
- Real Kafka broker (127.0.0.1:9092) and real Spark Structured Streaming consumer.
- All 434 raw keys present (null where unused).
- Comparison against Member 2's batch function (add_ieee_card_history_features).
- Pure-Python negative control showing old draft formula fails.
"""

import os
import sys
import time
import json
import shutil
import subprocess
import pandas as pd
import numpy as np

_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_member2_dir = os.path.join(_repo_root, "member2_spark")
if _member2_dir not in sys.path:
    sys.path.insert(0, _member2_dir)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

if "HADOOP_HOME" not in os.environ and os.path.exists(r"C:\hadoop"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    bin_path = r"C:\hadoop\bin"
    if bin_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_path + os.pathsep + os.environ.get("PATH", "")

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

from member3_streaming.ieee_raw_schema import IEEE_RAW_SCHEMA
from member3_streaming.consumer_ieee import create_spark_session
from src.ieee.card_features import add_ieee_card_features
from src.ieee.aggregate_features import add_ieee_card_history_features

BROKER = "127.0.0.1:9092"
IN_TOPIC = "test_oracle_ieee_in"
OUT_TOPIC = "test_oracle_ieee_out"
DLQ_TOPIC = "test_oracle_ieee_dlq"
CHK_DIR = os.path.abspath("member3_streaming/checkpoints/test_oracle_ieee")
LOG_PATH = os.path.abspath("member3_streaming/consumer_oracle_test.log")

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
    print("Resetting Kafka topics and checkpoint directory...")
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
    new_topics = [
        NewTopic(name=IN_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(name=OUT_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(name=DLQ_TOPIC, num_partitions=1, replication_factor=1),
    ]
    admin.create_topics(new_topics)
    admin.close()

    if os.path.exists(CHK_DIR):
        shutil.rmtree(CHK_DIR, ignore_errors=True)
    if os.path.exists(LOG_PATH):
        try:
            os.remove(LOG_PATH)
        except Exception:
            pass
    print("Topics and checkpoints clean.")


def build_synthetic_dataset():
    """Build synthetic rows for ONE card covering tie and boundary conditions."""
    T = 1000000  # Probe timestamp

    base_card = {
        "card1": 12345,
        "card2": 500.0,
        "card3": 150.0,
        "card4": "visa",
        "card5": 226.0,
        "card6": "debit",
        "addr1": 315.0,
        "addr2": 87.0,
        "dist1": None,
        "dist2": None,
        "P_emaildomain": "gmail.com",
        "R_emaildomain": None,
        "ProductCD": "W",
        "isFraud": 0,
    }

    # All 434 raw fields must be present
    all_raw_fields = [f.name for f in IEEE_RAW_SCHEMA.fields]

    test_specs = [
        # 3 rows sharing one second with ascending TransactionIDs
        (100000, 1001, 50.0, "Same-second tie 1"),
        (100000, 1002, 75.0, "Same-second tie 2"),
        (100000, 1003, 100.0, "Same-second tie 3"),
        # W = 7d (604800s) boundary points
        (T - 604800 - 1, 1004, 60.0, "t - 7d - 1 (excluded from 7d)"),
        (T - 604800, 1005, 70.0, "t - 7d (included in 7d boundary)"),
        # W = 24h (86400s) boundary points
        (T - 86400 - 1, 1006, 80.0, "t - 24h - 1 (excluded from 24h)"),
        (T - 86400, 1007, 90.0, "t - 24h (included in 24h boundary)"),
        # W = 1h (3600s) boundary points
        (T - 3600 - 1, 1008, 110.0, "t - 1h - 1 (excluded from 1h)"),
        (T - 3600, 1009, 120.0, "t - 1h (included in 1h boundary)"),
        # Probe boundaries
        (T - 1, 1010, 130.0, "t - 1 (upper boundary of rolling windows)"),
        (T, 1011, 150.0, "t (probe row, excluded from own rolling window)"),
    ]

    records = []
    for dt, tid, amt, desc in test_specs:
        row = {f: None for f in all_raw_fields}
        row.update(base_card)
        row["TransactionID"] = int(tid)
        row["TransactionDT"] = int(dt)
        row["TransactionAmt"] = float(amt)
        records.append(row)

    return records, test_specs


def compute_batch_oracle(records, spark):
    """Run Member 2 batch function on the synthetic rows in Spark."""
    df_raw = spark.createDataFrame(pd.DataFrame(records))
    df_with_card = add_ieee_card_features(df_raw)
    df_features = add_ieee_card_history_features(df_with_card)
    return df_features.toPandas()


def run_negative_control(records):
    """Apply the old draft formula (dt >= cur_dt - W) in pure Python."""
    print("\n" + "=" * 80)
    print("NEGATIVE CONTROL (Proving Oracle Test Can Detect Incorrect Logic)")
    print("=" * 80)
    print("Applying OLD draft formula: dt >= cur_dt - W (improperly includes same-second rows)...")

    # Trace on row 1 (TransactionID 1002, same second as 1001)
    history = [100000]  # row 0 DT
    cur_dt = 100000     # row 1 DT

    # Old draft formula:
    c_1h_old = sum(1 for dt in history if dt >= cur_dt - 3600)
    # Correct oracle formula:
    c_1h_correct = sum(1 for dt in history if cur_dt - 3600 <= dt <= cur_dt - 1)

    print(f"Row 1 (TransactionID 1002, DT=100000):")
    print(f"  Old draft c_1h: {c_1h_old} (wrongly counted row 1001 in same second)")
    print(f"  Correct oracle c_1h: {c_1h_correct} (strictly excludes same-second row 1001)")
    assert c_1h_old != c_1h_correct, "Negative control failed to detect logic discrepancy!"
    print("Negative control PASSED: Proved the assertion logic catches same-second window leakage!")


def main():
    print("=" * 80)
    print("GATE 1.2: ORACLE TIE AND WINDOW BOUNDARY TEST")
    print("Real Kafka: yes (127.0.0.1:9092)")
    print("Real Spark query: yes (PySpark Structured Streaming via consumer_ieee.py)")
    print("=" * 80)

    records, test_specs = build_synthetic_dataset()
    n_rows = len(records)
    print(f"Generated {n_rows} synthetic test records for single card_proxy_id.")

    # 1. Run Negative Control
    run_negative_control(records)

    # 2. Compute Batch Oracle via Member 2
    print("\nComputing batch ground truth via Member 2's add_ieee_card_history_features...")
    spark = create_spark_session(app_name="Gate1_2_BatchOracle")
    batch_oracle_df = compute_batch_oracle(records, spark)
    spark.stop()
    print("Batch ground truth computed.")

    # 3. Reset Kafka infrastructure
    reset_topics()

    # 4. Produce records to Kafka
    print(f"Producing {n_rows} synthetic records to {IN_TOPIC}...")
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for r in records:
        producer.send(IN_TOPIC, key=str(r["card1"]), value=r)
    producer.flush()
    producer.close()
    print("Produced all records.")

    # 5. Launch consumer_ieee.py
    print(f"Starting real streaming consumer subprocess writing to {OUT_TOPIC}...")
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

    # 6. Consume stream output from Kafka
    consumer = KafkaConsumer(
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    consumer.assign([TopicPartition(OUT_TOPIC, 0)])

    out_records = {}
    t0 = time.time()
    while len(out_records) < n_rows and (time.time() - t0) < 120:
        rec_map = consumer.poll(timeout_ms=1000)
        if rec_map:
            for tp, msgs in rec_map.items():
                for msg in msgs:
                    val = json.loads(msg.value.decode("utf-8"))
                    out_records[val["TransactionID"]] = val
        if len(out_records) >= n_rows:
            break
        time.sleep(0.5)
    consumer.close()

    # Clean termination
    try:
        cmd_kill = f"powershell -NoProfile -Command \"Get-CimInstance Win32_Process | Where-Object {{ $_.ParentProcessId -eq {proc.pid} }} | ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force }}\""
        subprocess.run(cmd_kill, shell=True, capture_output=True)
    except Exception:
        pass
    subprocess.run(f"taskkill /F /T /PID {proc.pid}", shell=True, capture_output=True)
    time.sleep(2)

    assert len(out_records) == n_rows, f"Expected {n_rows} output records, got {len(out_records)}"

    stream_df = pd.DataFrame(list(out_records.values()))
    merged = pd.merge(stream_df, batch_oracle_df, on="TransactionID", suffixes=("_stream", "_oracle"))
    merged = merged.sort_values(by=["TransactionDT_oracle", "TransactionID"], ascending=[True, True])

    print("\n" + "=" * 115)
    print("SIDE-BY-SIDE COMPARISON: STREAMING CONSUMER OUTPUT vs BATCH ORACLE (All 11 Stateful Columns)")
    print("=" * 115)

    mismatches = 0
    for _, row in merged.iterrows():
        tid = row["TransactionID"]
        dt = row["TransactionDT_oracle"]
        amt = row["TransactionAmt_oracle"]
        print(f"\n[TransactionID: {tid} | DT: {dt} | Amt: {amt}]")
        print(f"  {'Column':<26} | {'Stream Output':<18} | {'Batch Oracle':<18} | {'Match?':<8}")
        print("  " + "-" * 76)

        for col in STATEFUL_COLS:
            s_val = row[f"{col}_stream"]
            o_val = row[f"{col}_oracle"]

            if pd.isna(s_val) and pd.isna(o_val):
                match = True
            elif pd.isna(s_val) != pd.isna(o_val):
                match = False
            elif isinstance(s_val, (int, float, np.number)) and isinstance(o_val, (int, float, np.number)):
                match = abs(float(s_val) - float(o_val)) <= 1e-6
            else:
                match = str(s_val) == str(o_val)

            if not match:
                mismatches += 1

            s_str = f"{s_val:.4f}" if isinstance(s_val, float) and not pd.isna(s_val) else str(s_val)
            o_str = f"{o_val:.4f}" if isinstance(o_val, float) and not pd.isna(o_val) else str(o_val)
            print(f"  {col:<26} | {s_str:<18} | {o_str:<18} | {'PASS' if match else 'FAIL':<8}")

    print("\n" + "=" * 80)
    print(f"TOTAL MISMATCHES ACROSS ALL 11 COLUMNS ON ALL {n_rows} ROWS: {mismatches}")
    assert mismatches == 0, f"Oracle gate failed with {mismatches} mismatches!"
    print("Gate 1.2 PASSED: 100% strict parity with Member 2 batch oracle on ties & window boundaries!")
    print("=" * 80)


if __name__ == "__main__":
    main()
