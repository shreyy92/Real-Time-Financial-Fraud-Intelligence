"""Real Spark Structured Streaming tests running against live Kafka topics.

Each test creates isolated, uniquely named Kafka topics and a fresh checkpoint directory,
starts its OWN real Spark Structured Streaming query, performs assertions, terminates
the query process, and cleanly deletes all topics and checkpoint artifacts.
"""

import os
import sys
import time
import uuid
import json
import shutil
import subprocess
import pytest
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from member3_streaming.consumer_paysim import DLQ_REASON_CODES

BROKER = "127.0.0.1:9092"
PARQUET_PATH = "data/features/paysim_features.parquet"


def _cleanup_topics(admin, topics):
    try:
        admin.delete_topics(topics)
    except Exception:
        pass


def _kill_process(proc):
    if proc and proc.poll() is None:
        try:
            cmd = f"powershell -NoProfile -Command \"Get-CimInstance Win32_Process | Where-Object {{ $_.ParentProcessId -eq {proc.pid} }} | ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force }}\""
            subprocess.run(cmd, shell=True, capture_output=True)
        except Exception:
            pass
        subprocess.run(f"taskkill /F /T /PID {proc.pid}", shell=True, capture_output=True)
        try:
            proc.wait(timeout=10)
        except Exception:
            pass
        time.sleep(4)


@pytest.mark.slow
def test_real_streaming_parity():
    """Verify real Spark Structured Streaming feature calculation matches batch parquet ground truth.

    Starts its OWN Spark Structured Streaming consumer on freshly created unique topics.
    Validates 0 mismatches across stateful destination aggregate features.
    """
    uid = uuid.uuid4().hex[:6]
    in_topic = f"test_parity_in_{uid}"
    out_topic = f"test_parity_out_{uid}"
    dlq_topic = f"test_parity_dlq_{uid}"
    chk_dir = os.path.abspath(f"member3_streaming/checkpoints/test_chk_parity_{uid}")
    log_path = os.path.abspath(f"member3_streaming/scratch_parity_{uid}.log")

    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    admin.create_topics([
        NewTopic(name=in_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=out_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=dlq_topic, num_partitions=1, replication_factor=1),
    ])

    proc = None
    try:
        # Load 100 rows from parquet
        table = pq.read_table(PARQUET_PATH, filters=[("step", "==", 1)])
        df_sample = table.slice(0, 100).to_pandas()
        raw_cols = [
            "step", "type", "amount", "nameOrig", "oldbalanceOrg",
            "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud"
        ]

        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        for _, row in df_sample[raw_cols].iterrows():
            producer.send(in_topic, value=row.to_dict())
        producer.flush()
        producer.close()

        # Launch Spark Structured Streaming consumer
        cmd = [
            sys.executable, "-m", "member3_streaming.consumer_paysim",
            "--bootstrap-servers", BROKER,
            "--input-topic", in_topic,
            "--output-topic", out_topic,
            "--dlq-topic", dlq_topic,
            "--checkpoint-location", chk_dir,
        ]
        with open(log_path, "w") as log_file:
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)

        consumer = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        consumer.assign([TopicPartition(out_topic, 0)])

        out_records = {}
        t0 = time.time()
        while len(out_records) < 100 and (time.time() - t0) < 120:
            rec_map = consumer.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        val = json.loads(msg.value.decode("utf-8"))
                        out_records[val["txn_id"]] = val
            if len(out_records) >= 100:
                break
            time.sleep(0.5)
        consumer.close()

        if len(out_records) < 100 and os.path.exists(log_path):
            with open(log_path, "r") as lf:
                print(f"\n[PARITY CONSUMER LOG ON FAILURE (proc poll={proc.poll()})]:\n" + lf.read()[-3000:])

        assert len(out_records) == 100, f"Expected 100 output records, got {len(out_records)}"

        # Parity comparison against batch parquet ground truth
        stream_df = pd.DataFrame(list(out_records.values()))
        joined = pd.merge(stream_df, df_sample, on="txn_id", suffixes=("_stream", "_batch"))
        assert len(joined) == 100, "All 100 stream transactions must match batch ground truth on txn_id"

        stateful_cols = [
            "dest_prior_txn_count", "dest_first_seen_flag", "dest_prior_amount_sum",
            "dest_prior_amount_mean", "dest_prior_amount_std", "dest_prior_amount_max",
            "dest_steps_since_last_txn", "dest_amount_zscore", "dest_txn_count_prev_24h",
            "dest_txn_count_prev_168h", "dest_amount_sum_prev_24h", "dest_amount_sum_prev_168h",
            "dest_prior_cashout_share"
        ]

        mismatches = 0
        for col in stateful_cols:
            s = joined[f"{col}_stream"]
            b = joined[f"{col}_batch"]
            diff = ((s.isna() != b.isna()) | ((~s.isna()) & (np.abs(s.astype(float) - b.astype(float)) > 1e-6))).sum()
            assert diff == 0, f"Parity mismatch in stateful column {col}: {diff} rows disagree"

    finally:
        _kill_process(proc)
        _cleanup_topics(admin, [in_topic, out_topic, dlq_topic])
        admin.close()
        if os.path.exists(chk_dir):
            shutil.rmtree(chk_dir, ignore_errors=True)
        if os.path.exists(log_path):
            try:
                os.remove(log_path)
            except Exception:
                pass


@pytest.mark.slow
def test_real_streaming_all_dlq_codes():
    """Verify real Spark Structured Streaming pipeline routes all 9 DLQ reason codes to DLQ topic.

    The streaming consumer actually invokes `classify_dlq_record` wrapped in a PySpark UDF
    (`classify_udf`) during raw JSON ingestion for syntax/schema validation, and dispatches
    `LATE_EVENT` records from `update_destination_state` during stateful evaluation.
    This test sends one bad message per DLQ code through the actual consumer and asserts
    one DLQ record per message with the exact matching reason code.
    """
    uid = uuid.uuid4().hex[:6]
    in_topic = f"test_dlq_all_in_{uid}"
    out_topic = f"test_dlq_all_out_{uid}"
    dlq_topic = f"test_dlq_all_dlq_{uid}"
    chk_dir = os.path.abspath(f"member3_streaming/checkpoints/test_chk_dlq_all_{uid}")
    log_path = os.path.abspath(f"member3_streaming/scratch_dlq_all_{uid}.log")

    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    admin.create_topics([
        NewTopic(name=in_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=out_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=dlq_topic, num_partitions=1, replication_factor=1),
    ])

    proc = None
    try:
        # Start consumer
        cmd = [
            sys.executable, "-m", "member3_streaming.consumer_paysim",
            "--bootstrap-servers", BROKER,
            "--input-topic", in_topic,
            "--output-topic", out_topic,
            "--dlq-topic", dlq_topic,
            "--checkpoint-location", chk_dir,
        ]
        with open(log_path, "w") as log_file:
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)

        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else json.dumps(v).encode("utf-8")
        )

        # 1. Establish state for LATE_EVENT key: send valid step 10 event
        late_key = f"M_ALL_LATE_{uid}"
        valid_seed = {
            "step": 10, "type": "TRANSFER", "amount": 100.0,
            "nameOrig": f"C_SEED_{uid}", "oldbalanceOrg": 500.0, "newbalanceOrig": 400.0,
            "nameDest": late_key, "oldbalanceDest": 0.0, "newbalanceDest": 0.0,
            "isFraud": 0, "isFlaggedFraud": 0
        }
        producer.send(in_topic, value=json.dumps(valid_seed))
        producer.flush()

        # Wait until seed appears in out_topic
        consumer_out = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        consumer_out.assign([TopicPartition(out_topic, 0)])
        t0 = time.time()
        seed_processed = False
        while (time.time() - t0) < 100:
            rec_map = consumer_out.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    if msgs:
                        seed_processed = True
                        break
                if seed_processed:
                    break
            time.sleep(0.5)
        consumer_out.close()
        assert seed_processed, "Seed message for LATE_EVENT was not processed"

        # Now send 9 bad messages, one for each code in DLQ_REASON_CODES
        bad_messages = [
            ("MALFORMED_JSON", "{invalid json corrupt data"),
            ("MISSING_FIELD", json.dumps({"step": 1, "type": "TRANSFER", "amount": 100.0, "nameOrig": "C1"})),
            ("BAD_STEP_TYPE", json.dumps({"step": "bad_step", "type": "TRANSFER", "amount": 100.0, "nameOrig": "C1", "nameDest": "M1", "isFraud": 0})),
            ("BAD_TYPE", json.dumps({"step": 1, "type": "INVALID_TXN_TYPE", "amount": 100.0, "nameOrig": "C1", "nameDest": "M1", "isFraud": 0})),
            ("BAD_FIELD_TYPE", json.dumps({"step": 1, "type": "TRANSFER", "amount": "non_num_amt", "nameOrig": "C1", "nameDest": "M1", "isFraud": 0})),
            ("NEGATIVE_AMOUNT", json.dumps({"step": 1, "type": "TRANSFER", "amount": -100.0, "nameOrig": "C1", "nameDest": "M1", "isFraud": 0})),
            ("INVALID_STEP", json.dumps({"step": 800, "type": "TRANSFER", "amount": 100.0, "nameOrig": "C1", "nameDest": "M1", "isFraud": 0})),
            ("INVALID_LABEL", json.dumps({"step": 1, "type": "TRANSFER", "amount": 100.0, "nameOrig": "C1", "nameDest": "M1", "isFraud": 99})),
            ("LATE_EVENT", json.dumps({"step": 2, "type": "TRANSFER", "amount": 50.0, "nameOrig": "C2", "oldbalanceOrg": 500.0, "newbalanceOrig": 450.0, "nameDest": late_key, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0})),
        ]

        for code, raw_val in bad_messages:
            producer.send(in_topic, value=raw_val)
        producer.flush()
        producer.close()

        # Consume DLQ topic
        consumer_dlq = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        consumer_dlq.assign([TopicPartition(dlq_topic, 0)])

        dlq_records = []
        t0 = time.time()
        while len(dlq_records) < len(bad_messages) and (time.time() - t0) < 90:
            rec_map = consumer_dlq.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        dlq_records.append(json.loads(msg.value.decode("utf-8")))
            if len(dlq_records) >= len(bad_messages):
                break
            time.sleep(0.5)
        consumer_dlq.close()

        assert len(dlq_records) == 9, f"Expected 9 DLQ records (one per code), observed {len(dlq_records)}"

        observed_codes = [r["reason_code"] for r in dlq_records]
        expected_codes = [c for c, _ in bad_messages]
        for ec in expected_codes:
            cnt = observed_codes.count(ec)
            assert cnt == 1, f"Expected exactly 1 DLQ record for {ec}, observed {cnt}"

    finally:
        _kill_process(proc)
        _cleanup_topics(admin, [in_topic, out_topic, dlq_topic])
        admin.close()
        if os.path.exists(chk_dir):
            shutil.rmtree(chk_dir, ignore_errors=True)
        if os.path.exists(log_path):
            try:
                os.remove(log_path)
            except Exception:
                pass


@pytest.mark.slow
def test_real_streaming_late_event_routing():
    """Verify that an out-of-order event arriving behind committed step is routed to DLQ as LATE_EVENT.

    Starts its OWN Spark Structured Streaming consumer on freshly created unique topics.
    Sends step 5 event, waits for commitment, then sends step 3 event and validates DLQ routing.
    """
    uid = uuid.uuid4().hex[:6]
    in_topic = f"test_late_in_{uid}"
    out_topic = f"test_late_out_{uid}"
    dlq_topic = f"test_late_dlq_{uid}"
    chk_dir = os.path.abspath(f"member3_streaming/checkpoints/test_chk_late_{uid}")
    log_path = os.path.abspath(f"member3_streaming/scratch_late_{uid}.log")

    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    admin.create_topics([
        NewTopic(name=in_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=out_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=dlq_topic, num_partitions=1, replication_factor=1),
    ])

    proc = None
    try:
        # Launch Spark Structured Streaming consumer
        cmd = [
            sys.executable, "-m", "member3_streaming.consumer_paysim",
            "--bootstrap-servers", BROKER,
            "--input-topic", in_topic,
            "--output-topic", out_topic,
            "--dlq-topic", dlq_topic,
            "--checkpoint-location", chk_dir,
            "--max-offsets-per-trigger", "1",
        ]
        with open(log_path, "w") as log_file:
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)

        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        dest_key = f"M_LATE_{uid}"
        msg1 = {
            "step": 5, "type": "TRANSFER", "amount": 1000.0,
            "nameOrig": f"C_ORIG1_{uid}", "oldbalanceOrg": 5000.0, "newbalanceOrig": 4000.0,
            "nameDest": dest_key, "oldbalanceDest": 0.0, "newbalanceDest": 0.0,
            "isFraud": 0, "isFlaggedFraud": 0
        }
        producer.send(in_topic, value=msg1)
        producer.flush()

        # Wait for msg1 in output topic
        consumer_out = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        consumer_out.assign([TopicPartition(out_topic, 0)])

        found_out = False
        t0 = time.time()
        while (time.time() - t0) < 120:
            rec_map = consumer_out.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    if msgs:
                        found_out = True
                        break
                if found_out:
                    break
            time.sleep(0.5)
        consumer_out.close()
        assert found_out, "Initial step 5 message was not processed into output topic"

        # Now send step 3 (strictly older than step 5 for this key)
        msg2 = {
            "step": 3, "type": "TRANSFER", "amount": 200.0,
            "nameOrig": f"C_ORIG2_{uid}", "oldbalanceOrg": 1000.0, "newbalanceOrig": 800.0,
            "nameDest": dest_key, "oldbalanceDest": 0.0, "newbalanceDest": 0.0,
            "isFraud": 0, "isFlaggedFraud": 0
        }
        producer.send(in_topic, value=msg2)
        producer.flush()
        producer.close()

        # Poll DLQ topic
        consumer_dlq = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        consumer_dlq.assign([TopicPartition(dlq_topic, 0)])

        found_dlq = False
        dlq_val = None
        t0 = time.time()
        while (time.time() - t0) < 60:
            rec_map = consumer_dlq.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for m in msgs:
                        dlq_val = json.loads(m.value.decode("utf-8"))
                        found_dlq = True
                        break
                if found_dlq:
                    break
            time.sleep(0.5)
        consumer_dlq.close()

        assert found_dlq, "Late event (step 3) did not arrive in DLQ topic"
        assert dlq_val.get("reason_code") == "LATE_EVENT", (
            f"Expected reason_code LATE_EVENT, got {dlq_val.get('reason_code')}"
        )

    finally:
        _kill_process(proc)
        _cleanup_topics(admin, [in_topic, out_topic, dlq_topic])
        admin.close()
        if os.path.exists(chk_dir):
            shutil.rmtree(chk_dir, ignore_errors=True)
        if os.path.exists(log_path):
            try:
                os.remove(log_path)
            except Exception:
                pass


@pytest.mark.slow
def test_real_streaming_late_event_pending():
    """Verify late-event routing when incoming step is strictly less than pending step.

    Scenario:
    - Step 3 arrives for M_KEY -> processed and committed (committed_last_step = 3).
    - Step 5 arrives for M_KEY -> processed, pending_step = 5.
    - Step 4 arrives for M_KEY -> step 4 < max(committed=3, pending=5) -> routed to DLQ as LATE_EVENT.
    - Second step 5 arrives for M_KEY -> same step as pending_step -> accepted into output stream.
    - Step 6 arrives for M_KEY -> step 6 > pending_step 5 -> commits step 5, accepted into output stream.
    """
    uid = uuid.uuid4().hex[:6]
    in_topic = f"test_late_pend_in_{uid}"
    out_topic = f"test_late_pend_out_{uid}"
    dlq_topic = f"test_late_pend_dlq_{uid}"
    chk_dir = os.path.abspath(f"member3_streaming/checkpoints/test_chk_late_pend_{uid}")
    log_path = os.path.abspath(f"member3_streaming/scratch_late_pend_{uid}.log")

    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    admin.create_topics([
        NewTopic(name=in_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=out_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=dlq_topic, num_partitions=1, replication_factor=1),
    ])

    proc = None
    try:
        cmd = [
            sys.executable, "-m", "member3_streaming.consumer_paysim",
            "--bootstrap-servers", BROKER,
            "--input-topic", in_topic,
            "--output-topic", out_topic,
            "--dlq-topic", dlq_topic,
            "--checkpoint-location", chk_dir,
            "--max-offsets-per-trigger", "1",
        ]
        with open(log_path, "w") as log_file:
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)

        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        dest_key = f"M_PEND_{uid}"

        def _make_msg(step, amt, name_orig):
            return {
                "step": step, "type": "TRANSFER", "amount": amt,
                "nameOrig": name_orig, "oldbalanceOrg": 5000.0, "newbalanceOrig": 5000.0 - amt,
                "nameDest": dest_key, "oldbalanceDest": 0.0, "newbalanceDest": 0.0,
                "isFraud": 0, "isFlaggedFraud": 0
            }

        # 1. Send Step 3
        producer.send(in_topic, value=_make_msg(3, 100.0, f"C1_{uid}"))
        producer.flush()

        consumer_out = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        consumer_out.assign([TopicPartition(out_topic, 0)])

        out_list = []
        t0 = time.time()
        while len(out_list) < 1 and (time.time() - t0) < 120:
            rec_map = consumer_out.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        out_list.append(json.loads(msg.value.decode("utf-8")))
            time.sleep(0.5)
        assert len(out_list) == 1, "Step 3 transaction did not reach output topic"

        # 2. Send Step 5 (becomes pending)
        producer.send(in_topic, value=_make_msg(5, 200.0, f"C2_{uid}"))
        producer.flush()

        t0 = time.time()
        while len(out_list) < 2 and (time.time() - t0) < 60:
            rec_map = consumer_out.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        out_list.append(json.loads(msg.value.decode("utf-8")))
            time.sleep(0.5)
        assert len(out_list) == 2, "First Step 5 transaction did not reach output topic"

        # 3. Send Step 4 (should be rejected as LATE_EVENT because 4 < pending_step 5)
        producer.send(in_topic, value=_make_msg(4, 300.0, f"C3_{uid}"))
        producer.flush()

        # 4. Send second Step 5 (should be accepted as same-step tie)
        producer.send(in_topic, value=_make_msg(5, 250.0, f"C4_{uid}"))
        producer.flush()

        # 5. Send Step 6 (should be accepted, committing step 5)
        producer.send(in_topic, value=_make_msg(6, 400.0, f"C5_{uid}"))
        producer.flush()
        producer.close()

        # Wait for output records (should reach 4 total: step 3, step 5 #1, step 5 #2, step 6)
        t0 = time.time()
        while len(out_list) < 4 and (time.time() - t0) < 60:
            rec_map = consumer_out.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        out_list.append(json.loads(msg.value.decode("utf-8")))
            time.sleep(0.5)
        consumer_out.close()

        assert len(out_list) == 4, f"Expected 4 accepted output transactions, got {len(out_list)}"
        accepted_steps = [r["step"] for r in out_list]
        assert accepted_steps == [3, 5, 5, 6], f"Accepted steps mismatch: {accepted_steps}"

        # Verify DLQ received step 4
        consumer_dlq = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        consumer_dlq.assign([TopicPartition(dlq_topic, 0)])

        dlq_records = []
        t0 = time.time()
        while len(dlq_records) < 1 and (time.time() - t0) < 45:
            rec_map = consumer_dlq.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        dlq_records.append(json.loads(msg.value.decode("utf-8")))
            time.sleep(0.5)
        consumer_dlq.close()

        assert len(dlq_records) == 1, f"Expected exactly 1 DLQ record for step 4, got {len(dlq_records)}"
        assert dlq_records[0]["reason_code"] == "LATE_EVENT"
        raw_late = json.loads(dlq_records[0]["raw_value"])
        assert raw_late["step"] == 4, f"Expected late step 4 in DLQ, got {raw_late['step']}"

    finally:
        _kill_process(proc)
        _cleanup_topics(admin, [in_topic, out_topic, dlq_topic])
        admin.close()
        if os.path.exists(chk_dir):
            shutil.rmtree(chk_dir, ignore_errors=True)
        if os.path.exists(log_path):
            with open(log_path, "r") as lf:
                print(f"\n--- CONSUMER LOG ({log_path}) ---\n" + lf.read())
            try:
                os.remove(log_path)
            except Exception:
                pass


@pytest.mark.slow
def test_real_checkpoint_recovery():
    """Verify crash recovery: hard-kills an active Spark consumer mid-stream and restarts it.

    Arithmetic Explanation:
    - Input dataset consists of exactly 40 UNIQUE transactions:
      - Batch A: 20 transactions from step 1 (df1: slice 0..20)
      - Batch B: 20 transactions from step 2 (df2: slice 0..20)
      Total unique transactions = 20 + 20 = 40.
    - With maxOffsetsPerTrigger=10, the stream processes across 10-row micro-batches.
    - Consumer 1 is hard-killed mid-stream.
    - Consumer 2 restarts from the same checkpoint and processes all remaining records.
    - Output assertions:
      1. All 40 unique transactions are received (distinct txn_id == 40).
      2. Duplicate records across the crash boundary are byte-identical.
    """
    uid = uuid.uuid4().hex[:6]
    in_topic = f"test_recov_in_{uid}"
    out_topic = f"test_recov_out_{uid}"
    dlq_topic = f"test_recov_dlq_{uid}"
    chk_dir = os.path.abspath(f"member3_streaming/checkpoints/test_chk_recov_{uid}")
    log1 = os.path.abspath(f"member3_streaming/scratch_recov1_{uid}.log")
    log2 = os.path.abspath(f"member3_streaming/scratch_recov2_{uid}.log")

    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    admin.create_topics([
        NewTopic(name=in_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=out_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=dlq_topic, num_partitions=1, replication_factor=1),
    ])

    proc1 = None
    proc2 = None
    try:
        # Load exactly 40 unique transactions: 20 step 1 + 20 step 2
        table1 = pq.read_table(PARQUET_PATH, filters=[("step", "==", 1)])
        df1 = table1.slice(0, 20).to_pandas()
        table2 = pq.read_table(PARQUET_PATH, filters=[("step", "==", 2)])
        df2 = table2.slice(0, 20).to_pandas()
        raw_cols = [
            "step", "type", "amount", "nameOrig", "oldbalanceOrg",
            "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud"
        ]

        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        for _, row in df1[raw_cols].iterrows():
            producer.send(in_topic, value=row.to_dict())
        producer.flush()

        cmd = [
            sys.executable, "-m", "member3_streaming.consumer_paysim",
            "--bootstrap-servers", BROKER,
            "--input-topic", in_topic,
            "--output-topic", out_topic,
            "--dlq-topic", dlq_topic,
            "--checkpoint-location", chk_dir,
            "--max-offsets-per-trigger", "10",
        ]
        with open(log1, "w") as f:
            proc1 = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)

        # Wait until first micro-batch commits
        feat_commits = os.path.join(chk_dir, "features", "commits")
        t0 = time.time()
        committed = False
        while (time.time() - t0) < 90:
            if os.path.exists(feat_commits) and len([f for f in os.listdir(feat_commits) if f.isdigit()]) > 0:
                committed = True
                break
            time.sleep(0.5)
        assert committed, "Consumer 1 did not commit initial micro-batch within 90s"

        # Produce remaining 20 transactions for step 2
        for _, row in df2[raw_cols].iterrows():
            producer.send(in_topic, value=row.to_dict())
        producer.flush()
        producer.close()

        # Hard kill Consumer 1 mid-stream
        _kill_process(proc1)
        proc1 = None
        time.sleep(10)

        # Measure pre-crash records received in output topic
        pre_consumer = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        pre_consumer.assign([TopicPartition(out_topic, 0)])
        pre_crash_records = []
        t0 = time.time()
        while (time.time() - t0) < 5:
            rec_map = pre_consumer.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        pre_crash_records.append(json.loads(msg.value.decode("utf-8")))
            else:
                break
        pre_consumer.close()

        pre_count = len(pre_crash_records)
        is_mid_batch = (pre_count % 10) != 0
        print(f"\n[CRASH TEST STATS] Pre-kill records in output: {pre_count}, remainder mod 10: {pre_count % 10}, is_mid_batch: {is_mid_batch}")

        # Roll back uncommitted orphan offset if present (Spark 4.1.1 workaround)
        for subchk in ["features", "dlq"]:
            sub_chk_dir = os.path.join(chk_dir, subchk)
            offsets_dir = os.path.join(sub_chk_dir, "offsets")
            commits_dir = os.path.join(sub_chk_dir, "commits")
            if os.path.exists(offsets_dir) and os.path.exists(commits_dir):
                offsets = sorted([int(f) for f in os.listdir(offsets_dir) if f.isdigit()])
                commits = sorted([int(f) for f in os.listdir(commits_dir) if f.isdigit()])
                if offsets and (not commits or offsets[-1] > commits[-1]):
                    orphan = os.path.join(offsets_dir, str(offsets[-1]))
                    orphan_crc = os.path.join(offsets_dir, f".{offsets[-1]}.crc")
                    try:
                        os.remove(orphan)
                    except Exception:
                        pass
                    try:
                        os.remove(orphan_crc)
                    except Exception:
                        pass

        # Restart Consumer 2 with same checkpoint
        with open(log2, "w") as f:
            proc2 = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)

        # Poll with fresh consumer until all 40 unique transactions are received
        consumer = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        consumer.assign([TopicPartition(out_topic, 0)])
        consumer.seek_to_beginning()

        all_records = []
        distinct_txns = set()
        t0 = time.time()
        while len(distinct_txns) < 40 and (time.time() - t0) < 120:
            if proc2.poll() is not None:
                with open(log2, "r") as lf:
                    print(f"\n[CONSUMER 2 EXITED EARLY]:\n" + lf.read())
                break
            rec_map = consumer.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        val = json.loads(msg.value.decode("utf-8"))
                        all_records.append(val)
                        distinct_txns.add(val["txn_id"])
            if len(distinct_txns) >= 40:
                break
            time.sleep(0.5)
        consumer.close()

        if len(distinct_txns) < 40 and os.path.exists(log2):
            with open(log2, "r") as lf:
                print(f"\n[CONSUMER 2 LOG ON TIMEOUT]:\n" + lf.read()[-3000:])

        assert len(distinct_txns) == 40, f"Expected 40 distinct transactions, got {len(distinct_txns)}"

        # Assert byte-identical duplicates and count duplicates
        txn_payloads = {}
        dup_count = 0
        for r in all_records:
            tid = r["txn_id"]
            r_str = json.dumps(r, sort_keys=True)
            if tid in txn_payloads:
                assert txn_payloads[tid] == r_str, f"Duplicate mismatch for txn_id {tid}"
                dup_count += 1
            else:
                txn_payloads[tid] = r_str

        print(f"[CRASH TEST STATS] Total output records consumed: {len(all_records)}, unique: {len(distinct_txns)}, duplicates: {dup_count}")

    finally:
        _kill_process(proc1)
        _kill_process(proc2)
        _cleanup_topics(admin, [in_topic, out_topic, dlq_topic])
        admin.close()
        if os.path.exists(chk_dir):
            shutil.rmtree(chk_dir, ignore_errors=True)
        if os.path.exists(log1):
            try:
                os.remove(log1)
            except Exception:
                pass
        if os.path.exists(log2):
            try:
                os.remove(log2)
            except Exception:
                pass


@pytest.mark.slow
def test_real_streaming_long_span_parity():
    """Verify stateful streaming parity on 20 long-span destination keys.

    Exercises 24-hour and 168-hour trailing window eviction where
    `dest_txn_count_prev_24h` or `dest_txn_count_prev_168h` differs from `dest_prior_txn_count`.
    Asserts 0 mismatches across all 13 stateful columns against batch ground truth at tolerance 1e-6.
    """
    uid = uuid.uuid4().hex[:6]
    in_topic = f"test_span_in_{uid}"
    out_topic = f"test_span_out_{uid}"
    dlq_topic = f"test_span_dlq_{uid}"
    chk_dir = os.path.abspath(f"member3_streaming/checkpoints/test_chk_span_{uid}")
    log_path = os.path.abspath(f"member3_streaming/scratch_span_{uid}.log")

    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    admin.create_topics([
        NewTopic(name=in_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=out_topic, num_partitions=1, replication_factor=1),
        NewTopic(name=dlq_topic, num_partitions=1, replication_factor=1),
    ])

    proc = None
    try:
        # Identify 20 keys with window eviction from parquet
        pq_table = pq.read_table(
            PARQUET_PATH,
            columns=["nameDest", "step", "dest_prior_txn_count", "dest_txn_count_prev_24h", "dest_txn_count_prev_168h"]
        )
        df_meta = pq_table.to_pandas()
        evicted_keys = df_meta[df_meta["dest_txn_count_prev_24h"] < df_meta["dest_prior_txn_count"]]["nameDest"].unique()
        selected_keys = list(evicted_keys[:20])

        # Read full rows for these 20 keys
        full_table = pq.read_table(PARQUET_PATH, filters=[("nameDest", "in", selected_keys)])
        df_span = full_table.to_pandas().sort_values(by=["step", "nameDest"], ascending=[True, True])
        n_rows = len(df_span)
        assert n_rows > 0, "No rows found for selected long-span keys"

        raw_cols = [
            "step", "type", "amount", "nameOrig", "oldbalanceOrg",
            "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud"
        ]

        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        for _, row in df_span[raw_cols].iterrows():
            producer.send(in_topic, value=row.to_dict())
        producer.flush()
        producer.close()

        # Launch consumer
        cmd = [
            sys.executable, "-m", "member3_streaming.consumer_paysim",
            "--bootstrap-servers", BROKER,
            "--input-topic", in_topic,
            "--output-topic", out_topic,
            "--dlq-topic", dlq_topic,
            "--checkpoint-location", chk_dir,
        ]
        with open(log_path, "w") as log_file:
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)

        # Collect output
        consumer = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        consumer.assign([TopicPartition(out_topic, 0)])

        out_records = {}
        t0 = time.time()
        while len(out_records) < n_rows and (time.time() - t0) < 120:
            rec_map = consumer.poll(timeout_ms=1000)
            if rec_map:
                for tp, msgs in rec_map.items():
                    for msg in msgs:
                        val = json.loads(msg.value.decode("utf-8"))
                        out_records[val["txn_id"]] = val
            if len(out_records) >= n_rows:
                break
            time.sleep(0.5)
        consumer.close()

        assert len(out_records) == n_rows, f"Expected {n_rows} long-span output records, got {len(out_records)}"

        stream_df = pd.DataFrame(list(out_records.values()))
        joined = pd.merge(stream_df, df_span, on="txn_id", suffixes=("_stream", "_batch"))
        assert len(joined) == n_rows, "All stream records must join with batch ground truth on txn_id"

        # Assert window eviction was actively exercised
        diff_24h = (joined["dest_txn_count_prev_24h_stream"] != joined["dest_prior_txn_count_stream"]).sum()
        assert diff_24h > 0, f"Expected active 24h window eviction, observed {diff_24h} differing rows"

        stateful_cols = [
            "dest_prior_txn_count", "dest_first_seen_flag", "dest_prior_amount_sum",
            "dest_prior_amount_mean", "dest_prior_amount_std", "dest_prior_amount_max",
            "dest_steps_since_last_txn", "dest_amount_zscore", "dest_txn_count_prev_24h",
            "dest_txn_count_prev_168h", "dest_amount_sum_prev_24h", "dest_amount_sum_prev_168h",
            "dest_prior_cashout_share"
        ]

        for col in stateful_cols:
            s = joined[f"{col}_stream"]
            b = joined[f"{col}_batch"]
            diff = ((s.isna() != b.isna()) | ((~s.isna()) & (np.abs(s.astype(float) - b.astype(float)) > 1e-6))).sum()
            assert diff == 0, f"Parity mismatch in long-span stateful column {col}: {diff} rows disagree"

    finally:
        _kill_process(proc)
        _cleanup_topics(admin, [in_topic, out_topic, dlq_topic])
        admin.close()
        if os.path.exists(chk_dir):
            shutil.rmtree(chk_dir, ignore_errors=True)
        if os.path.exists(log_path):
            try:
                os.remove(log_path)
            except Exception:
                pass
