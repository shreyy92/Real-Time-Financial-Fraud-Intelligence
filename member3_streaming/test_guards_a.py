import os
import shutil
import time
import json
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

from member3_streaming.consumer_paysim import (
    create_spark_session,
    build_streaming_pipeline,
)

def run_test():
    chk_dir = os.path.abspath("member3_streaming/checkpoints/guard_test")
    if os.path.exists(chk_dir):
        shutil.rmtree(chk_dir, ignore_errors=True)

    spark = create_spark_session("GuardVerificationA")

    query = build_streaming_pipeline(
        spark=spark,
        bootstrap_servers="localhost:9092",
        input_topic="financial_transactions_paysim",
        output_topic="financial_features_paysim",
        dlq_topic="financial_transactions_paysim_dlq",
        checkpoint_location=chk_dir,
    )

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8") if isinstance(v, (dict, list)) else v.encode("utf-8")
    )

    # Consumer to monitor output topic in real time
    consumer_out = KafkaConsumer(
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    tp_out = TopicPartition("financial_features_paysim", 0)
    consumer_out.assign([tp_out])

    def wait_for_output_step(target_step, name_dest, timeout_sec=30):
        start = time.time()
        while time.time() - start < timeout_sec:
            records = consumer_out.poll(timeout_ms=1000)
            for tp, msgs in records.items():
                for msg in msgs:
                    val = json.loads(msg.value.decode("utf-8"))
                    if val.get("nameDest") == name_dest and val.get("step") == target_step:
                        return val
            time.sleep(0.5)
        raise TimeoutError(f"Timed out waiting for step {target_step} on {name_dest}")

    fresh_key = "M_TEST_GUARD_A2"
    print(f"--- Establishing initial state for fresh key {fresh_key} ---")

    # Step 3 event
    ev1 = {
        "step": 3, "type": "TRANSFER", "amount": 100.0, "nameOrig": "C_A2_1", "nameDest": fresh_key,
        "oldbalanceOrg": 500.0, "newbalanceOrig": 400.0, "oldbalanceDest": 0.0, "newbalanceDest": 100.0,
        "isFraud": 0, "isFlaggedFraud": 0
    }
    producer.send("financial_transactions_paysim", value=ev1)
    producer.flush()
    print("Sent Step 3 event. Waiting for it to appear in output...")
    res1 = wait_for_output_step(3, fresh_key)
    print(f"Step 3 processed and committed in stream: {res1['txn_id']}")

    # Step 5 event
    ev2 = {
        "step": 5, "type": "TRANSFER", "amount": 200.0, "nameOrig": "C_A2_2", "nameDest": fresh_key,
        "oldbalanceOrg": 500.0, "newbalanceOrig": 300.0, "oldbalanceDest": 100.0, "newbalanceDest": 300.0,
        "isFraud": 0, "isFlaggedFraud": 0
    }
    producer.send("financial_transactions_paysim", value=ev2)
    producer.flush()
    print("Sent Step 5 event. Waiting for it to appear in output...")
    res2 = wait_for_output_step(5, fresh_key)
    print(f"Step 5 processed and committed in stream: {res2['txn_id']}")

    print("\nState successfully established: committed state at step 3, pending at step 5!")
    print("--- Sending test events ---")

    # Step 4 row (Must go to DLQ as LATE_EVENT)
    ev3_late = {
        "step": 4, "type": "PAYMENT", "amount": 50.0, "nameOrig": "C_A2_3", "nameDest": fresh_key,
        "oldbalanceOrg": 200.0, "newbalanceOrig": 150.0, "oldbalanceDest": 300.0, "newbalanceDest": 350.0,
        "isFraud": 0, "isFlaggedFraud": 0
    }
    producer.send("financial_transactions_paysim", value=ev3_late)

    # Step 5 row (Must be accepted: same-step tie)
    ev4_tie = {
        "step": 5, "type": "PAYMENT", "amount": 75.0, "nameOrig": "C_A2_4", "nameDest": fresh_key,
        "oldbalanceOrg": 200.0, "newbalanceOrig": 125.0, "oldbalanceDest": 300.0, "newbalanceDest": 375.0,
        "isFraud": 0, "isFlaggedFraud": 0
    }
    producer.send("financial_transactions_paysim", value=ev4_tie)

    # Step 6 row (Must be accepted and fold pending into committed)
    ev5_step6 = {
        "step": 6, "type": "TRANSFER", "amount": 300.0, "nameOrig": "C_A2_5", "nameDest": fresh_key,
        "oldbalanceOrg": 1000.0, "newbalanceOrig": 700.0, "oldbalanceDest": 375.0, "newbalanceDest": 675.0,
        "isFraud": 0, "isFlaggedFraud": 0
    }
    producer.send("financial_transactions_paysim", value=ev5_step6)

    # Row with type="TELEPORT" (Must go to DLQ as BAD_TYPE)
    ev6_teleport = {
        "step": 10, "type": "TELEPORT", "amount": 500.0, "nameOrig": "C_A3_TEL", "nameDest": "M_DEST_TEL",
        "oldbalanceOrg": 1000.0, "newbalanceOrig": 500.0, "oldbalanceDest": 0.0, "newbalanceDest": 500.0,
        "isFraud": 0, "isFlaggedFraud": 0
    }
    producer.send("financial_transactions_paysim", value=ev6_teleport)

    # Row with step as string (Must go to DLQ as BAD_STEP_TYPE)
    ev7_bad_step = {
        "step": "one_hundred", "type": "PAYMENT", "amount": 250.0, "nameOrig": "C_A3_STR", "nameDest": "M_DEST_STR",
        "oldbalanceOrg": 500.0, "newbalanceOrig": 250.0, "oldbalanceDest": 0.0, "newbalanceDest": 250.0,
        "isFraud": 0, "isFlaggedFraud": 0
    }
    producer.send("financial_transactions_paysim", value=ev7_bad_step)

    producer.flush()
    print("Sent test events. Waiting for step 6 to be processed in output...")
    res6 = wait_for_output_step(6, fresh_key)
    print(f"Step 6 processed and committed in stream: {res6['txn_id']}")

    print("Waiting 5s for all triggers and DLQ flushes to settle...")
    time.sleep(5)

    # Stop active streams cleanly
    for q in spark.streams.active:
        q.stop()
    spark.stop()

    print("\n" + "=" * 80)
    print("COLLECTING AND DISPLAYING ALL OUTPUT ROWS FROM financial_features_paysim")
    print("=" * 80)
    consumer_out.seek_to_beginning()
    output_rows = []
    while True:
        records = consumer_out.poll(timeout_ms=3000)
        if not records:
            break
        for tp, msgs in records.items():
            for msg in msgs:
                val = json.loads(msg.value.decode("utf-8"))
                output_rows.append(val)
                print(f"OUTPUT ROW: step={val.get('step')}, nameDest={val.get('nameDest')}, amount={val.get('amount')}, split={val.get('split')}, dest_prior_txn_count={val.get('dest_prior_txn_count')}, dest_prior_amount_sum={val.get('dest_prior_amount_sum')}, dest_steps_since_last_txn={val.get('dest_steps_since_last_txn')}, txn_id={val.get('txn_id')}")

    print(f"\nTotal Output Rows: {len(output_rows)}")

    print("\n" + "=" * 80)
    print("COLLECTING AND DISPLAYING ALL DLQ RECORDS FROM financial_transactions_paysim_dlq")
    print("=" * 80)
    consumer_dlq = KafkaConsumer(
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    tp_dlq = TopicPartition("financial_transactions_paysim_dlq", 0)
    consumer_dlq.assign([tp_dlq])
    consumer_dlq.seek_to_beginning()

    dlq_records = []
    while True:
        records = consumer_dlq.poll(timeout_ms=3000)
        if not records:
            break
        for tp, msgs in records.items():
            for msg in msgs:
                val = json.loads(msg.value.decode("utf-8"))
                dlq_records.append(val)
                print(f"DLQ RECORD: reason_code={val.get('reason_code')}, raw_value={val.get('raw_value')}")

    print(f"\nTotal DLQ Records: {len(dlq_records)}")

if __name__ == "__main__":
    run_test()
