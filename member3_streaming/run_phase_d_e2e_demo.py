"""Phase D clean end-to-end demo for PaySim and IEEE pipelines.

Demonstrates:
1. Standard reset of demo topics and checkpoints.
2. Producer feeding valid transactions to Kafka input topic.
3. PySpark Structured Streaming consumer calculating features in real time.
4. Member 4 style downstream consumer reading from Kafka output topic with client-side deduplication.
5. Contract compliance (PaySim: 42 columns, IEEE: 472 columns).
Outputs raw log to member3_streaming/reports/raw/PD_e2e_demo.txt.
"""
import os
import sys
import json
import time
import shutil
import subprocess
import uuid
import pandas as pd
import pyarrow.parquet as pq
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

sys.path.insert(0, os.path.abspath("."))
sys.path.insert(0, os.path.abspath("member2_spark"))

from member3_streaming.producer_ieee import sanitize_record, json_serializer
from member3_streaming.consumer_paysim import PAYSIM_OUTPUT_SCHEMA
from member3_streaming.ieee_output_schema import IEEE_OUTPUT_SCHEMA

BROKER = "127.0.0.1:9092"
RAW_OUT = os.path.abspath("member3_streaming/reports/raw/PD_e2e_demo.txt")

def _kill_process(proc):
    if proc and proc.poll() is None:
        try:
            cmd = f'powershell -NoProfile -Command "Stop-Process -Id (Get-CimInstance Win32_Process | Where-Object ParentProcessId -eq {proc.pid}).ProcessId -Force -ErrorAction SilentlyContinue"'
            subprocess.run(cmd, shell=True, capture_output=True)
        except Exception:
            pass
        subprocess.run(f"taskkill /F /T /PID {proc.pid}", shell=True, capture_output=True)
        try:
            proc.wait(timeout=10)
        except Exception:
            pass
        time.sleep(8)

def main():
    log_lines = []
    def log(msg):
        print(msg, flush=True)
        log_lines.append(str(msg))

    log("=====================================================================")
    log("PHASE D: END-TO-END DEMO FOR PAYSIM AND IEEE STREAMING PIPELINES")
    log("=====================================================================")

    uid = uuid.uuid4().hex[:6]
    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)

    # ---------------------------------------------------------
    # PART 1: PaySim End-to-End Demo
    # ---------------------------------------------------------
    log("\n---------------------------------------------------------------------")
    log("PART 1: PaySim End-to-End Demo")
    log("---------------------------------------------------------------------")
    ps_in = f"demo_ps_in_{uid}"
    ps_out = f"demo_ps_out_{uid}"
    ps_dlq = f"demo_ps_dlq_{uid}"
    ps_chk = os.path.abspath(f"member3_streaming/checkpoints/demo_ps_{uid}")
    ps_log = os.path.abspath(f"member3_streaming/scratch_demo_ps_{uid}.log")

    admin.create_topics([
        NewTopic(name=ps_in, num_partitions=1, replication_factor=1),
        NewTopic(name=ps_out, num_partitions=1, replication_factor=1),
        NewTopic(name=ps_dlq, num_partitions=1, replication_factor=1),
    ])
    log(f"Created ephemeral topics: {ps_in}, {ps_out}, {ps_dlq}")

    ps_proc = None
    try:
        # Load 25 clean transactions from paysim_clean.parquet
        ps_clean_path = "data/processed/paysim_clean.parquet"
        ps_table = pq.read_table(ps_clean_path)
        ps_df = ps_table.slice(0, 25).to_pandas()
        raw_cols = [
            "step", "type", "amount", "nameOrig", "oldbalanceOrg",
            "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud"
        ]

        log(f"Producing {len(ps_df)} transactions to {ps_in}...")
        ps_producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        for _, row in ps_df[raw_cols].iterrows():
            ps_producer.send(ps_in, value=row.to_dict())
        ps_producer.flush()
        ps_producer.close()
        log("Producer finished producing PaySim input transactions.")

        # Start PaySim Structured Streaming Consumer
        log(f"Starting PySpark consumer_paysim with checkpoint {ps_chk}...")
        ps_cmd = [
            sys.executable, "-m", "member3_streaming.consumer_paysim",
            "--bootstrap-servers", BROKER,
            "--input-topic", ps_in,
            "--output-topic", ps_out,
            "--dlq-topic", ps_dlq,
            "--checkpoint-location", ps_chk,
        ]
        with open(ps_log, "w") as f:
            ps_proc = subprocess.Popen(ps_cmd, stdout=f, stderr=subprocess.STDOUT)

        # Member 4 Downstream Inference Consumer with Deduplication
        log(f"Starting Member 4 dedupe consumer reading from {ps_out}...")
        m4_ps_consumer = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        m4_ps_consumer.assign([TopicPartition(ps_out, 0)])

        seen_ps_txns = {}
        t0 = time.time()
        while len(seen_ps_txns) < 25 and (time.time() - t0) < 120:
            rec_map = m4_ps_consumer.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        record = json.loads(msg.value.decode("utf-8"))
                        tid = record["txn_id"]
                        if tid not in seen_ps_txns:
                            seen_ps_txns[tid] = record
                            log(f"  [MEMBER 4 PAYSIM CONSUMER] Ingested & deduped: txn_id={tid}, step={record['step']}, amount={record['amount']}")
            time.sleep(0.5)
        m4_ps_consumer.close()

        log(f"Total unique PaySim records ingested: {len(seen_ps_txns)}")
        assert len(seen_ps_txns) == 25, f"Expected 25 PaySim records, got {len(seen_ps_txns)}"

        # Validate column schema (42 columns per contract)
        first_rec = next(iter(seen_ps_txns.values()))
        expected_cols = [f.name for f in PAYSIM_OUTPUT_SCHEMA.fields]
        actual_cols = list(first_rec.keys())
        assert len(actual_cols) == 42, f"Expected 42 columns, got {len(actual_cols)}"
        assert sorted(actual_cols) == sorted(expected_cols), "PaySim columns do not match contract schema!"
        log("SUCCESS: PaySim output matches 42-column schema contract exactly.")

    finally:
        _kill_process(ps_proc)
        try:
            admin.delete_topics([ps_in, ps_out, ps_dlq])
        except Exception:
            pass
        if os.path.exists(ps_chk):
            shutil.rmtree(ps_chk, ignore_errors=True)
        if os.path.exists(ps_log):
            try:
                os.remove(ps_log)
            except Exception:
                pass

    # ---------------------------------------------------------
    # PART 2: IEEE End-to-End Demo
    # ---------------------------------------------------------
    log("\n---------------------------------------------------------------------")
    log("PART 2: IEEE End-to-End Demo")
    log("---------------------------------------------------------------------")
    ieee_in = f"demo_ieee_in_{uid}"
    ieee_out = f"demo_ieee_out_{uid}"
    ieee_dlq = f"demo_ieee_dlq_{uid}"
    ieee_chk = os.path.abspath(f"member3_streaming/checkpoints/demo_ieee_{uid}")
    ieee_log = os.path.abspath(f"member3_streaming/scratch_demo_ieee_{uid}.log")

    admin.create_topics([
        NewTopic(name=ieee_in, num_partitions=1, replication_factor=1),
        NewTopic(name=ieee_out, num_partitions=1, replication_factor=1),
        NewTopic(name=ieee_dlq, num_partitions=1, replication_factor=1),
    ])
    log(f"Created ephemeral topics: {ieee_in}, {ieee_out}, {ieee_dlq}")

    ieee_proc = None
    try:
        # Load 25 clean transactions from ieee_clean.parquet
        ieee_clean_path = "data/processed/ieee_clean.parquet"
        ieee_table = pq.read_table(ieee_clean_path)
        ieee_df = ieee_table.slice(0, 25).to_pandas().sort_values(
            by=["TransactionDT", "TransactionID"], ascending=[True, True], kind="stable"
        )

        log(f"Producing {len(ieee_df)} transactions to {ieee_in}...")
        ieee_producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
            value_serializer=json_serializer,
        )
        for rec in ieee_df.to_dict(orient="records"):
            msg = sanitize_record(rec)
            ieee_producer.send(ieee_in, key=str(msg["card1"]), value=msg)
        ieee_producer.flush()
        ieee_producer.close()
        log("Producer finished producing IEEE input transactions.")

        # Start IEEE Structured Streaming Consumer
        log(f"Starting PySpark consumer_ieee with checkpoint {ieee_chk}...")
        ieee_cmd = [
            sys.executable, "-m", "member3_streaming.consumer_ieee",
            "--bootstrap-servers", BROKER,
            "--input-topic", ieee_in,
            "--output-topic", ieee_out,
            "--dlq-topic", ieee_dlq,
            "--checkpoint-location", ieee_chk,
        ]
        with open(ieee_log, "w") as f:
            ieee_proc = subprocess.Popen(ieee_cmd, stdout=f, stderr=subprocess.STDOUT)

        # Member 4 Downstream Inference Consumer with Deduplication on TransactionID
        log(f"Starting Member 4 dedupe consumer reading from {ieee_out}...")
        m4_ieee_consumer = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        m4_ieee_consumer.assign([TopicPartition(ieee_out, 0)])

        seen_ieee_txns = {}
        t0 = time.time()
        while len(seen_ieee_txns) < 25 and (time.time() - t0) < 150:
            rec_map = m4_ieee_consumer.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        record = json.loads(msg.value.decode("utf-8"))
                        tid = record["TransactionID"]
                        if tid not in seen_ieee_txns:
                            seen_ieee_txns[tid] = record
                            log(f"  [MEMBER 4 IEEE CONSUMER] Ingested & deduped: TransactionID={tid}, DT={record['TransactionDT']}, Amt={record['TransactionAmt']}")
            time.sleep(0.5)
        m4_ieee_consumer.close()

        log(f"Total unique IEEE records ingested: {len(seen_ieee_txns)}")
        assert len(seen_ieee_txns) == 25, f"Expected 25 IEEE records, got {len(seen_ieee_txns)}"

        # Validate column schema (483 columns per stateful contract: 472 base + 11 stateful)
        first_ieee = next(iter(seen_ieee_txns.values()))
        expected_ieee_cols = [f.name for f in IEEE_OUTPUT_SCHEMA.fields]
        actual_ieee_cols = list(first_ieee.keys())
        assert len(actual_ieee_cols) == 483, f"Expected 483 columns, got {len(actual_ieee_cols)}"
        assert sorted(actual_ieee_cols) == sorted(expected_ieee_cols), "IEEE columns do not match contract schema!"
        log("SUCCESS: IEEE output matches 483-column schema contract exactly.")

    finally:
        _kill_process(ieee_proc)
        try:
            admin.delete_topics([ieee_in, ieee_out, ieee_dlq])
        except Exception:
            pass
        if os.path.exists(ieee_chk):
            shutil.rmtree(ieee_chk, ignore_errors=True)
        if os.path.exists(ieee_log):
            try:
                os.remove(ieee_log)
            except Exception:
                pass
        admin.close()

    log("\n=====================================================================")
    log("END-TO-END DEMO FOR BOTH DATASETS COMPLETED SUCCESSFULLY!")
    log("=====================================================================")

    os.makedirs(os.path.dirname(RAW_OUT), exist_ok=True)
    with open(RAW_OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines) + "\n")
    print(f"\nRaw demo output written to {RAW_OUT}")

if __name__ == "__main__":
    main()
