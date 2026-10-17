"""End-to-end integration test for IEEE LATE_EVENT routing per Guidelines Phase A.1 Task 4.

Executes on real Kafka (127.0.0.1:9092) and real PySpark Structured Streaming.
Uses ephemeral topics and checkpoint directories.
Test scenario:
1. Seed event: DT=1000, TID=5 -> Valid output record #1.
2. Late events:
   - DT=900, TID=3 (DT < committed DT 1000) -> DLQ reason LATE_EVENT #1.
   - DT=1000, TID=4 (DT == committed DT 1000 but TID <= committed TID 5) -> DLQ reason LATE_EVENT #2.
3. Negative control:
   - DT=1001, TID=6 (DT > committed DT 1000) -> Valid output record #2.
"""

import os
import sys
import time
import json
import uuid
import shutil
import subprocess
import pyarrow.parquet as pq
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

BROKER = "127.0.0.1:9092"

def main():
    test_id = str(uuid.uuid4())[:8]
    in_topic = f"in_late_{test_id}"
    out_topic = f"out_late_{test_id}"
    dlq_topic = f"dlq_late_{test_id}"
    chk_dir = os.path.abspath(f"member3_streaming/checkpoints/test_late_{test_id}")
    raw_path = os.path.abspath("member3_streaming/reports/raw/PA_late_event.txt")

    print(f"=== IEEE LATE_EVENT End-to-End Integration Test ===")
    print(f"Real Kafka: yes ({BROKER})")
    print(f"Real Spark query: yes (PySpark Structured Streaming via consumer_ieee.py)")
    print(f"Ephemeral in_topic:  {in_topic}")
    print(f"Ephemeral out_topic: {out_topic}")
    print(f"Ephemeral dlq_topic: {dlq_topic}")
    print(f"Checkpoint dir:      {chk_dir}")

    # 1. Create ephemeral topics
    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    admin.create_topics([
        NewTopic(name=in_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=out_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=dlq_topic, num_partitions=1, replication_factor=1),
    ])
    admin.close()

    # Load a template raw record from ieee_clean.parquet to have all 434 valid columns
    clean_path = "data/processed/ieee_clean.parquet"
    table = pq.read_table(clean_path)
    sample_dict = {col: table[col][0].as_py() for col in table.column_names}
    # Ensure required fields are valid
    sample_dict["card1"] = 10001
    sample_dict["card2"] = 111.0
    sample_dict["card3"] = 150.0
    sample_dict["card4"] = "visa"
    sample_dict["card5"] = 226.0
    sample_dict["card6"] = "debit"
    sample_dict["ProductCD"] = "W"
    sample_dict["isFraud"] = 0
    sample_dict["TransactionAmt"] = 100.0

    def make_event(dt, tid, amt=100.0):
        rec = dict(sample_dict)
        rec["TransactionDT"] = dt
        rec["TransactionID"] = tid
        rec["TransactionAmt"] = amt
        return rec

    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )

    proc = None
    try:
        # 2. Launch consumer_ieee.py on ephemeral topics
        env = os.environ.copy()
        if "HADOOP_HOME" not in env and os.path.exists(r"C:\hadoop"):
            env["HADOOP_HOME"] = r"C:\hadoop"
        env["IEEE_DLQ_TOPIC"] = dlq_topic
        env["KAFKA_BOOTSTRAP_SERVERS"] = BROKER

        cmd = [
            sys.executable, "-u", "-m", "member3_streaming.consumer_ieee",
            "--bootstrap-servers", BROKER,
            "--input-topic", in_topic,
            "--output-topic", out_topic,
            "--dlq-topic", dlq_topic,
            "--checkpoint-location", chk_dir,
            "--max-offsets-per-trigger", "10",
        ]
        log_file = os.path.abspath(f"member3_streaming/scratch_late_event_{test_id}.log")
        f_log = open(log_file, "w")
        proc = subprocess.Popen(cmd, stdout=f_log, stderr=subprocess.STDOUT, env=env)
        print(f"Consumer started with PID {proc.pid}. Waiting 15s for Spark query initialization...")
        time.sleep(15)

        consumer_out = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        tp_out = TopicPartition(out_topic, 0)
        consumer_out.assign([tp_out])

        consumer_dlq = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        tp_dlq = TopicPartition(dlq_topic, 0)
        consumer_dlq.assign([tp_dlq])

        # Step 1: Send seed event DT=1000, TID=5
        print("\nStep 1: Sending seed event (DT=1000, TID=5)...")
        e1 = make_event(1000, 5, 50.0)
        producer.send(in_topic, key="seed", value=e1)
        producer.flush()

        # Wait for seed event in output
        out_records = []
        t0 = time.time()
        while len(out_records) < 1 and (time.time() - t0) < 45:
            rec_map = consumer_out.poll(timeout_ms=1000)
            if rec_map:
                for msgs in rec_map.values():
                    for m in msgs:
                        out_records.append(json.loads(m.value.decode("utf-8")))
            time.sleep(0.5)

        print(f"Received {len(out_records)} output records for seed event.")
        assert len(out_records) == 1, f"Expected 1 seed output record, got {len(out_records)}"
        assert out_records[0]["TransactionID"] == 5
        assert out_records[0]["TransactionDT"] == 1000
        print(f"Seed event successfully committed: DT={out_records[0]['TransactionDT']}, TID={out_records[0]['TransactionID']}.")

        # Step 2: Send late events: DT=900 TID=3 and DT=1000 TID=4
        print("\nStep 2: Sending late events (DT=900 TID=3 and DT=1000 TID=4)...")
        e_late1 = make_event(900, 3, 30.0)
        e_late2 = make_event(1000, 4, 40.0)
        producer.send(in_topic, key="late1", value=e_late1)
        producer.send(in_topic, key="late2", value=e_late2)
        producer.flush()

        # Wait for DLQ records
        dlq_records = []
        t0 = time.time()
        while len(dlq_records) < 2 and (time.time() - t0) < 45:
            rec_map = consumer_dlq.poll(timeout_ms=1000)
            if rec_map:
                for msgs in rec_map.values():
                    for m in msgs:
                        dlq_records.append(json.loads(m.value.decode("utf-8")))
            time.sleep(0.5)

        print(f"Received {len(dlq_records)} DLQ records for late events.")
        assert len(dlq_records) == 2, f"Expected 2 DLQ records, got {len(dlq_records)}"
        for idx, rec in enumerate(dlq_records):
            print(f"  DLQ Record #{idx+1}: reason_code='{rec.get('reason_code')}', ingested_at='{rec.get('ingested_at')}'")
            assert rec.get("reason_code") == "LATE_EVENT", f"Expected reason_code LATE_EVENT, got {rec.get('reason_code')}"
            raw_obj = json.loads(rec.get("raw_value"))
            assert raw_obj.get("TransactionID") in [3, 4], f"Unexpected TID in raw_value: {raw_obj.get('TransactionID')}"
            assert "card_proxy_id" in raw_obj, "Expected enriched row in raw_value"

        # Step 3: Negative control - send later event DT=1001, TID=6 (must be accepted as valid)
        print("\nStep 3: Sending negative control valid event (DT=1001, TID=6)...")
        e_valid = make_event(1001, 6, 60.0)
        producer.send(in_topic, key="valid_future", value=e_valid)
        producer.flush()

        t0 = time.time()
        while len(out_records) < 2 and (time.time() - t0) < 45:
            rec_map = consumer_out.poll(timeout_ms=1000)
            if rec_map:
                for msgs in rec_map.values():
                    for m in msgs:
                        out_records.append(json.loads(m.value.decode("utf-8")))
            time.sleep(0.5)

        print(f"Received total {len(out_records)} output records.")
        assert len(out_records) == 2, f"Expected 2 valid output records, got {len(out_records)}"
        assert out_records[1]["TransactionID"] == 6
        assert out_records[1]["TransactionDT"] == 1001
        print("Negative control PASSED: DT=1001, TID=6 was accepted into valid output.")

        # Ensure no extra DLQ records arrived
        extra_dlq = consumer_dlq.poll(timeout_ms=2000)
        extra_count = sum(len(msgs) for msgs in extra_dlq.values()) if extra_dlq else 0
        assert extra_count == 0, f"Unexpected extra DLQ records: {extra_count}"

        print("\n" + "=" * 80)
        print("IEEE LATE_EVENT INTEGRATION TEST PASSED!")
        print("  Valid Output Records: 2 (TID 5, TID 6)")
        print("  DLQ Records:          2 (TID 3: LATE_EVENT, TID 4: LATE_EVENT)")
        print("  Negative Control:     PASSED (TID 6 accepted)")
        print("=" * 80)

        # Write raw output
        with open(raw_path, "w", encoding="utf-8") as f_raw:
            f_raw.write("=== IEEE LATE_EVENT End-to-End Integration Test ===\n")
            f_raw.write(f"Real Kafka: yes ({BROKER})\n")
            f_raw.write("Real Spark query: yes (PySpark Structured Streaming via consumer_ieee.py)\n")
            f_raw.write(f"Ephemeral in_topic:  {in_topic}\n")
            f_raw.write(f"Ephemeral out_topic: {out_topic}\n")
            f_raw.write(f"Ephemeral dlq_topic: {dlq_topic}\n")
            f_raw.write("Valid output records received: 2\n")
            f_raw.write(f"  Record 1: TID={out_records[0]['TransactionID']}, DT={out_records[0]['TransactionDT']}\n")
            f_raw.write(f"  Record 2: TID={out_records[1]['TransactionID']}, DT={out_records[1]['TransactionDT']}\n")
            f_raw.write("DLQ records received: 2\n")
            for idx, r in enumerate(dlq_records):
                f_raw.write(f"  DLQ Record {idx+1}: reason_code={r.get('reason_code')}, TID={json.loads(r.get('raw_value')).get('TransactionID')}\n")
            f_raw.write("Negative control (TID=6, DT=1001): PASSED (accepted into valid output)\n")
            f_raw.write("Result: ALL LATE_EVENT ASSERTIONS PASSED WITH REAL KAFKA AND SPARK.\n")

    finally:
        if proc:
            subprocess.run(f"taskkill /F /T /PID {proc.pid}", shell=True, capture_output=True)
            try:
                f_log.close()
            except Exception:
                pass
            if os.path.exists(log_file):
                try:
                    os.remove(log_file)
                except Exception:
                    pass

        # Cleanup ephemeral topics and checkpoint
        admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
        try:
            admin.delete_topics([in_topic, out_topic, dlq_topic])
        except Exception:
            pass
        admin.close()

        if os.path.exists(chk_dir):
            shutil.rmtree(chk_dir, ignore_errors=True)

if __name__ == "__main__":
    main()
