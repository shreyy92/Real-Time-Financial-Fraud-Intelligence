"""
Verification script for IEEE-CIS Producer (Step 3).

Checks:
1. Message count (exactly 2000)
2. TransactionDT is non-decreasing (True)
3. Columns entirely null in the sample
4. Field count per message (identical across all messages)
5. JSON null count and columns in sample message
6. json.loads failures (0)
7. Exact TransactionID match against sorted parquet.
"""

import json
import pandas as pd
from kafka import KafkaConsumer, TopicPartition


def main():
    print("Connecting to KafkaConsumer...")
    consumer = KafkaConsumer(
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=10000,
    )

    tp = TopicPartition("financial_transactions_ieee", 0)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)

    messages = []
    keys = []
    json_failures = 0

    for msg in consumer:
        try:
            val = json.loads(msg.value.decode("utf-8"))
            messages.append(val)
            keys.append(msg.key.decode("utf-8") if msg.key else None)
        except Exception:
            json_failures += 1
        if len(messages) == 2000:
            break

    consumer.close()

    print("=== KAFKA CONSUMER CHECK RESULTS ===")
    print(f"1. Total consumed messages: {len(messages)}")
    print(f"2. json.loads failures: {json_failures}")

    dts = [m["TransactionDT"] for m in messages]
    is_non_decreasing = all(dts[i] <= dts[i + 1] for i in range(len(dts) - 1))
    print(f"3. TransactionDT is non-decreasing: {is_non_decreasing}")

    field_counts = set(len(m) for m in messages)
    print(f"4. Number of fields per message across all messages: {field_counts}")

    # Entirely null columns across all 2000 messages
    sample_df = pd.DataFrame(messages)
    entirely_null_cols = [col for col in sample_df.columns if sample_df[col].isna().all()]
    print(f"5. Columns entirely null in the 2000 sample: count={len(entirely_null_cols)}")
    print(f"   List of entirely null columns: {entirely_null_cols}")

    # Number of distinct keys in sample
    distinct_keys = len(set(keys))
    print(f"6. Distinct Kafka keys in sample: {distinct_keys}")

    # Sample message nulls analysis (message index 0)
    sample_msg = messages[0]
    null_cols_in_sample_0 = [k for k, v in sample_msg.items() if v is None]
    tx_id_0 = sample_msg["TransactionID"]
    print(f"7. Sample message 0 (TransactionID={tx_id_0}) null count: {len(null_cols_in_sample_0)}")
    print(f"   Columns null in sample message 0: {null_cols_in_sample_0}")

    # 8. Compare against parquet
    print("Loading parquet dataset to verify order against ground truth...")
    df_parquet = pd.read_parquet("data/processed/ieee_clean.parquet")
    df_parquet = df_parquet.sort_values(
        by=["TransactionDT", "TransactionID"], ascending=[True, True], kind="stable"
    ).head(2000)

    parquet_tx_ids = df_parquet["TransactionID"].tolist()
    kafka_tx_ids = [m["TransactionID"] for m in messages]

    mismatches = sum(p != k for p, k in zip(parquet_tx_ids, kafka_tx_ids))
    print(f"8. TransactionID order mismatches vs Parquet: {mismatches}")
    if mismatches == 0:
        print("   PERFECT MATCH: All 2000 TransactionIDs match in exact order.")


if __name__ == "__main__":
    main()
