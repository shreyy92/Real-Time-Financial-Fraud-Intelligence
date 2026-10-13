import json
import pyarrow.parquet as pq
import pandas as pd
from kafka import KafkaConsumer, TopicPartition

BROKER = "localhost:9092"
TOPIC = "financial_transactions_ieee"
PARQUET_PATH = "data/processed/ieee_clean.parquet"

print("Loading ground truth parquet and sorting by (TransactionDT, TransactionID)...")
df_parquet = pd.read_parquet(PARQUET_PATH)
df_sorted = df_parquet.sort_values(by=["TransactionDT", "TransactionID"], ascending=[True, True], kind="stable").head(2000)
expected_txn_ids = df_sorted["TransactionID"].tolist()

print(f"Consuming all 2000 messages from Kafka topic: {TOPIC}...")
consumer = KafkaConsumer(
    bootstrap_servers=[BROKER],
    auto_offset_reset="earliest",
    consumer_timeout_ms=5000,
)
tp = TopicPartition(TOPIC, 0)
consumer.assign([tp])
consumer.seek_to_beginning()

messages = []
json_failures = 0
nan_strings = 0
field_counts = []
transaction_dts = []
consumed_txn_ids = []

sample_message = None
sample_null_cols = []

for msg in consumer:
    raw_str = msg.value.decode("utf-8")
    if '"NaN"' in raw_str or '"nan"' in raw_str:
        nan_strings += 1
    try:
        data = json.loads(raw_str)
        messages.append(data)
        field_counts.append(len(data))
        transaction_dts.append(data["TransactionDT"])
        consumed_txn_ids.append(data["TransactionID"])
        if sample_message is None:
            sample_message = data
            sample_null_cols = [k for k, v in data.items() if v is None]
    except Exception as ex:
        json_failures += 1

consumer.close()

print(f"Consumed messages count: {len(messages)}")
print(f"Gate 7.4a: json.loads failures = {json_failures}")
assert json_failures == 0, f"Expected 0 json failures, got {json_failures}"

is_non_decreasing = all(transaction_dts[i] <= transaction_dts[i+1] for i in range(len(transaction_dts)-1))
print(f"Gate 7.4b: TransactionDT non-decreasing = {is_non_decreasing}")
assert is_non_decreasing is True, "TransactionDT is not non-decreasing!"

unique_field_counts = set(field_counts)
print(f"Gate 7.4c: Field count identical across all messages = {len(unique_field_counts) == 1} (Count: {unique_field_counts.pop()})")
assert len(unique_field_counts) == 0, "Field counts are not identical across messages!"

print(f"Gate 7.4d: Sample message (TransactionID={sample_message['TransactionID']}) has {len(sample_null_cols)} null fields.")
print(f"Sample null columns (first 10): {sample_null_cols[:10]}")

# 7.5 Order vs parquet
order_mismatches = sum(1 for e, c in zip(expected_txn_ids, consumed_txn_ids) if e != c)
print(f"Gate 7.5: First 2,000 TransactionIDs order mismatches vs parquet = {order_mismatches}")
assert order_mismatches == 0, f"Order mismatches: {order_mismatches}"

# 7.6 Types: No NaN strings
print(f"Gate 7.6: NaN strings found in payloads = {nan_strings}")
assert nan_strings == 0, f"Found {nan_strings} NaN strings in payloads!"

print("\nALL CHUNK 7 GATE CHECKS PASSED WITH 100% SUCCESS!")
