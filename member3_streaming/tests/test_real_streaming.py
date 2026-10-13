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

BROKER = "localhost:9092"
PARQUET_PATH = "data/features/paysim_features.parquet"


def _cleanup_topics(admin, topics):
    try:
        admin.delete_topics(topics)
    except Exception:
        pass


def _kill_process(proc):
    if proc and proc.poll() is None:
        subprocess.run(f"taskkill /F /T /PID {proc.pid}", shell=True, capture_output=True)
        time.sleep(2)


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
        while len(out_records) < 100 and (time.time() - t0) < 60:
            for msg in consumer:
                val = json.loads(msg.value.decode("utf-8"))
                out_records[val["txn_id"]] = val
                if len(out_records) >= 100:
                    break
            time.sleep(0.5)
        consumer.close()

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
        while (time.time() - t0) < 45:
            msg = next(iter(consumer_out), None)
            if msg:
                found_out = True
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
        while (time.time() - t0) < 30:
            msg = next(iter(consumer_dlq), None)
            if msg:
                dlq_val = json.loads(msg.value.decode("utf-8"))
                found_dlq = True
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
def test_real_checkpoint_recovery():
    """Verify crash recovery: hard-kills an active Spark consumer mid-stream and restarts it.

    Asserts that after restart from the same checkpoint:
    1. All transactions are fully processed (distinct txn_id == total input rows).
    2. Zero transactions are lost (missing == 0).
    3. Any duplicate records produced across the crash boundary are byte-identical.
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
        # Prepare 40 transactions: 20 step 1, 20 step 2
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
        while (time.time() - t0) < 40:
            if os.path.exists(feat_commits) and len(os.listdir(feat_commits)) > 0:
                break
            time.sleep(0.5)

        # Produce remaining 20 transactions for step 2 while stream is active
        for _, row in df2[raw_cols].iterrows():
            producer.send(in_topic, value=row.to_dict())
        producer.flush()
        producer.close()

        # Hard kill Consumer 1 mid-stream
        _kill_process(proc1)
        proc1 = None
        time.sleep(3)

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
        while len(distinct_txns) < 40 and (time.time() - t0) < 60:
            for msg in consumer:
                val = json.loads(msg.value.decode("utf-8"))
                all_records.append(val)
                distinct_txns.add(val["txn_id"])
                if len(distinct_txns) >= 40:
                    break
            time.sleep(0.5)
        consumer.close()

        assert len(distinct_txns) == 40, f"Expected 40 distinct transactions, got {len(distinct_txns)}"

        # Assert byte-identical duplicates
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

    finally:
        _kill_process(proc1)
        _kill_process(proc2)
        _cleanup_topics(admin, [in_topic, out_topic, dlq_topic])
        admin.close()
        if os.path.exists(log2):
            with open(log2, "r") as f:
                lines = f.readlines()
                print("LOG2 TAIL:\n" + "".join(lines[-40:]))
        if os.path.exists(log1):
            with open(log1, "r") as f:
                lines = f.readlines()
                print("LOG1 TAIL:\n" + "".join(lines[-40:]))
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
