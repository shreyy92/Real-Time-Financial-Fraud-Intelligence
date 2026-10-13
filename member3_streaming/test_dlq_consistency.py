import os
import sys
import shutil
import time
import json
import subprocess
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

from member3_streaming.consumer_paysim import (
    create_spark_session,
    build_streaming_pipeline,
    DLQ_REASON_CODES,
)

def reset_topic(topic_name: str):
    cmd_del = f"docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic {topic_name}"
    subprocess.run(cmd_del, shell=True, capture_output=True)
    time.sleep(1)
    cmd_create = f"docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic {topic_name} --partitions 1 --replication-factor 1"
    subprocess.run(cmd_create, shell=True, capture_output=True)
    time.sleep(1)

def get_offset(topic_name: str) -> int:
    cmd = f"docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic {topic_name}"
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    out = res.stdout.strip()
    if not out:
        return 0
    parts = out.split(":")
    if len(parts) >= 3:
        return int(parts[2].strip())
    return 0

def run_dlq_consistency_test():
    print("=" * 80)
    print("STARTING DLQ CONSISTENCY TEST (Chunk 1R)")
    print("Real Kafka: yes | Real Spark query: yes")
    print("=" * 80)

    input_topic = "financial_transactions_paysim"
    output_topic = "financial_features_paysim"
    dlq_topic = "financial_transactions_paysim_dlq"
    chk_dir = os.path.abspath("member3_streaming/checkpoints/c1r_dlq_test")

    # 1. Standard Reset
    print("\n[Step 1] Resetting Kafka topics and checkpoint directory...")
    for t in [input_topic, output_topic, dlq_topic]:
        reset_topic(t)
        print(f"  Topic {t} offset: {get_offset(t)}")

    if os.path.exists(chk_dir):
        shutil.rmtree(chk_dir, ignore_errors=True)

    # 2. Start Real Spark Structured Streaming Query
    print("\n[Step 2] Initializing SparkSession and building streaming pipeline...")
    spark = create_spark_session("C1R_DLQ_Consistency")
    query = build_streaming_pipeline(
        spark=spark,
        bootstrap_servers="localhost:9092",
        input_topic=input_topic,
        output_topic=output_topic,
        dlq_topic=dlq_topic,
        checkpoint_location=chk_dir,
    )

    print(f"  Query started. Query ID: {query.id}, Run ID: {query.runId}")
    print(f"  Query active status: {query.isActive}")
    time.sleep(3)

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8") if isinstance(v, (dict, list)) else v.encode("utf-8"),
    )

    # 3. Message Sequence
    guard_key = "M_TEST_GUARD_1R"

    # Valid event 1 (Step 3)
    ev_val1 = {
        "step": 3, "type": "TRANSFER", "amount": 100.0, "nameOrig": "C_V1", "nameDest": guard_key,
        "oldbalanceOrg": 500.0, "newbalanceOrig": 400.0, "oldbalanceDest": 0.0, "newbalanceDest": 100.0,
        "isFraud": 0, "isFlaggedFraud": 0
    }
    # Valid event 2 (Step 5)
    ev_val2 = {
        "step": 5, "type": "TRANSFER", "amount": 200.0, "nameOrig": "C_V2", "nameDest": guard_key,
        "oldbalanceOrg": 500.0, "newbalanceOrig": 300.0, "oldbalanceDest": 100.0, "newbalanceDest": 300.0,
        "isFraud": 0, "isFlaggedFraud": 0
    }
    # Bad event: LATE_EVENT (Step 4 after 5 has arrived)
    ev_late = {
        "step": 4, "type": "PAYMENT", "amount": 50.0, "nameOrig": "C_LATE", "nameDest": guard_key,
        "oldbalanceOrg": 200.0, "newbalanceOrig": 150.0, "oldbalanceDest": 300.0, "newbalanceDest": 350.0,
        "isFraud": 0, "isFlaggedFraud": 0
    }
    # Valid event 3 (Step 5 same-step tie)
    ev_val3_tie = {
        "step": 5, "type": "PAYMENT", "amount": 75.0, "nameOrig": "C_V3", "nameDest": guard_key,
        "oldbalanceOrg": 200.0, "newbalanceOrig": 125.0, "oldbalanceDest": 300.0, "newbalanceDest": 375.0,
        "isFraud": 0, "isFlaggedFraud": 0
    }
    # Valid event 4 (Step 6 advancing and committing pending step 5)
    ev_val4_step6 = {
        "step": 6, "type": "TRANSFER", "amount": 300.0, "nameOrig": "C_V4", "nameDest": guard_key,
        "oldbalanceOrg": 1000.0, "newbalanceOrig": 700.0, "oldbalanceDest": 375.0, "newbalanceDest": 675.0,
        "isFraud": 0, "isFlaggedFraud": 0
    }

    # Stateless Bad Events
    bad_events = [
        ("MALFORMED_JSON", "CORRUPTED_NON_JSON_PAYLOAD_STRING{{{"),
        ("MISSING_FIELD", {
            "type": "TRANSFER", "amount": 1500.0, "nameOrig": "C_MISSING_1",
            "oldbalanceOrg": 5000.0, "newbalanceOrig": 3500.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0
        }),
        ("BAD_STEP_TYPE", {
            "step": "one_hundred", "type": "PAYMENT", "amount": 250.0, "nameOrig": "C_BADSTEP_1", "nameDest": "M_DEST1",
            "oldbalanceOrg": 500.0, "newbalanceOrig": 250.0, "oldbalanceDest": 0.0, "newbalanceDest": 250.0, "isFraud": 0
        }),
        ("BAD_TYPE", {
            "step": 10, "type": "TELEPORT", "amount": 250.0, "nameOrig": "C_BADTYPE_1", "nameDest": "M_DEST_TEL",
            "oldbalanceOrg": 500.0, "newbalanceOrig": 250.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0
        }),
        ("BAD_FIELD_TYPE", {
            "step": 10, "type": "PAYMENT", "amount": "abc", "nameOrig": "C_BADFIELD_1", "nameDest": "M_DEST_FIELD",
            "oldbalanceOrg": 500.0, "newbalanceOrig": 250.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0
        }),
        ("NEGATIVE_AMOUNT", {
            "step": 10, "type": "PAYMENT", "amount": -99.99, "nameOrig": "C_NEG_1", "nameDest": "M_DEST2",
            "oldbalanceOrg": 500.0, "newbalanceOrig": 500.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0
        }),
        ("INVALID_STEP", {
            "step": 9999, "type": "CASH_OUT", "amount": 300.0, "nameOrig": "C_STEP_9999", "nameDest": "M_DEST3",
            "oldbalanceOrg": 500.0, "newbalanceOrig": 200.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0
        }),
        ("INVALID_LABEL", {
            "step": 15, "type": "TRANSFER", "amount": 1000.0, "nameOrig": "C_FRAUD_2", "nameDest": "M_DEST4",
            "oldbalanceOrg": 1000.0, "newbalanceOrig": 0.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 2
        }),
    ]

    # Valid event 5 after all bad events
    ev_val5_post = {
        "step": 20, "type": "PAYMENT", "amount": 55.55, "nameOrig": "C_POST_1", "nameDest": "M_POST_DEST",
        "oldbalanceOrg": 500.0, "newbalanceOrig": 444.45, "oldbalanceDest": 0.0, "newbalanceDest": 55.55,
        "isFraud": 0, "isFlaggedFraud": 0
    }

    # Consumer to monitor output topic in real time
    consumer_mon = KafkaConsumer(
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    tp_out = TopicPartition(output_topic, 0)
    consumer_mon.assign([tp_out])

    seen_output = []
    def wait_for_output_step(target_step, name_dest, timeout_sec=40):
        for val in seen_output:
            if val.get("nameDest") == name_dest and val.get("step") == target_step:
                return val
        start = time.time()
        while time.time() - start < timeout_sec:
            records = consumer_mon.poll(timeout_ms=1000)
            for tp, msgs in records.items():
                for msg in msgs:
                    val = json.loads(msg.value.decode("utf-8"))
                    seen_output.append(val)
                    if val.get("nameDest") == name_dest and val.get("step") == target_step:
                        return val
            time.sleep(0.5)
        raise TimeoutError(f"Timed out waiting for step {target_step} on {name_dest}")

    print("\n[Step 3] Establishing committed state for key: step 3 then step 5...")
    # Send Step 3 and wait for commitment
    producer.send(input_topic, value=ev_val1)
    producer.flush()
    print("  -> Sent Step 3. Waiting for output...")
    res3 = wait_for_output_step(3, guard_key)
    print(f"  -> Step 3 committed: txn_id={res3['txn_id']}")

    # Send Step 5 and wait for commitment
    producer.send(input_topic, value=ev_val2)
    producer.flush()
    print("  -> Sent Step 5. Waiting for output...")
    res5 = wait_for_output_step(5, guard_key)
    print(f"  -> Step 5 committed: txn_id={res5['txn_id']}")

    # Now key has committed/pending step 5. Send Late Event (step 4)
    print("  -> Sending LATE_EVENT (step 4 for key with committed/pending step 5)")
    producer.send(input_topic, value=ev_late)

    # Send Step 5 tie
    print("  -> Sending Step 5 same-step tie (must be accepted)")
    producer.send(input_topic, value=ev_val3_tie)

    # Send Step 6
    print("  -> Sending Step 6 (advancing step and folding pending)")
    producer.send(input_topic, value=ev_val4_step6)

    # Send all 8 stateless bad events
    for code, payload in bad_events:
        print(f"  -> Sending bad event: {code}")
        producer.send(input_topic, value=payload)

    # Send post-validation valid event
    print("  -> Sending post-validation valid event (step 20)")
    producer.send(input_topic, value=ev_val5_post)
    producer.flush()

    print("  -> Waiting for step 6 to be committed in output...")
    res6 = wait_for_output_step(6, guard_key)
    print(f"  -> Step 6 committed: txn_id={res6['txn_id']}")

    print("  -> Waiting for step 20 to be committed in output...")
    res20 = wait_for_output_step(20, "M_POST_DEST")
    print(f"  -> Step 20 committed: txn_id={res20['txn_id']}")
    consumer_mon.close()

    print("\n[Step 4] Checking Spark streaming query health...")
    query_active_during = query.isActive
    print(f"  Query still active during processing: {query_active_during}")

    # Wait for processing triggers to complete
    print("  Waiting 10s for all micro-batches to commit...")
    time.sleep(10)

    query_active_after = query.isActive
    print(f"  Query still active after all batches: {query_active_after}")

    # Stop queries cleanly
    for q in spark.streams.active:
        q.stop()
    spark.stop()

    # 4. Consume from DLQ topic
    print("\n[Step 5] Consuming all records from DLQ topic...")
    consumer_dlq = KafkaConsumer(
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    tp_dlq = TopicPartition(dlq_topic, 0)
    consumer_dlq.assign([tp_dlq])
    consumer_dlq.seek_to_beginning()

    dlq_records = []
    while True:
        msgs = consumer_dlq.poll(timeout_ms=3000)
        if not msgs:
            break
        for tp, record_list in msgs.items():
            for m in record_list:
                try:
                    val = json.loads(m.value.decode("utf-8"))
                    dlq_records.append(val)
                except Exception as e:
                    dlq_records.append({"raw_bytes": str(m.value), "parse_error": str(e)})

    # 5. Consume from Output topic
    print("\n[Step 6] Consuming all records from output topic...")
    consumer_out = KafkaConsumer(
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    tp_out = TopicPartition(output_topic, 0)
    consumer_out.assign([tp_out])
    consumer_out.seek_to_beginning()

    output_records = []
    while True:
        msgs = consumer_out.poll(timeout_ms=3000)
        if not msgs:
            break
        for tp, record_list in msgs.items():
            for m in record_list:
                val = json.loads(m.value.decode("utf-8"))
                output_records.append(val)

    # 6. Print Raw Evidence
    print("\n" + "=" * 80)
    print("RAW DLQ RECORDS (COUNT = {})".format(len(dlq_records)))
    print("=" * 80)
    for idx, r in enumerate(dlq_records, 1):
        print(f"DLQ #{idx}:")
        print(f"  reason_code: {r.get('reason_code')}")
        print(f"  ingested_at: {r.get('ingested_at')}")
        print(f"  raw_value:   {r.get('raw_value')}")

    print("\n" + "=" * 80)
    print("RAW OUTPUT RECORDS (COUNT = {})".format(len(output_records)))
    print("=" * 80)
    for idx, r in enumerate(output_records, 1):
        print(f"OUT #{idx}: step={r.get('step')}, nameDest={r.get('nameDest')}, amount={r.get('amount')}, txn_id={r.get('txn_id')}, dest_prior_txn_count={r.get('dest_prior_txn_count')}, dest_prior_amount_sum={r.get('dest_prior_amount_sum')}, dest_steps_since_last_txn={r.get('dest_steps_since_last_txn')}, split={r.get('split')}")

    # 7. Verification Summary
    print("\n" + "=" * 80)
    print("DLQ REASON CODE VERIFICATION AUDIT")
    print("=" * 80)
    expected_codes = [
        "LATE_EVENT",
        "MALFORMED_JSON",
        "MISSING_FIELD",
        "BAD_STEP_TYPE",
        "BAD_TYPE",
        "BAD_FIELD_TYPE",
        "NEGATIVE_AMOUNT",
        "INVALID_STEP",
        "INVALID_LABEL",
    ]
    observed_codes = [r.get("reason_code") for r in dlq_records]

    code_counts = {code: observed_codes.count(code) for code in expected_codes}
    for code, cnt in code_counts.items():
        status = "PASS" if cnt == 1 else f"FAIL (count={cnt})"
        print(f"  {code:<20}: expected 1, observed {cnt:<2} -> {status}")

    all_codes_passed = all(cnt == 1 for cnt in code_counts.values()) and len(dlq_records) == len(expected_codes)
    valid_count_passed = (len(output_records) == 5)
    query_alive_passed = query_active_during and query_active_after

    print("\n" + "=" * 80)
    print(f"SUMMARY: DLQ Code Check: {'PASS' if all_codes_passed else 'FAIL'}")
    print(f"SUMMARY: Valid Count (5 expected): {'PASS' if valid_count_passed else 'FAIL'} (actual: {len(output_records)})")
    print(f"SUMMARY: Query Active Throughout: {'PASS' if query_alive_passed else 'FAIL'}")
    print("=" * 80)

    if not (all_codes_passed and valid_count_passed and query_alive_passed):
        print("OVERALL DLQ CONSISTENCY TEST: FAILED")
        sys.exit(1)
    else:
        print("OVERALL DLQ CONSISTENCY TEST: PASSED")
        sys.exit(0)

if __name__ == "__main__":
    run_dlq_consistency_test()
