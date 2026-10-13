import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "financial_features_paysim",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="member4_inference_consumer",
    consumer_timeout_ms=5000,
)
seen_txn_ids = set()
for msg in consumer:
    record = json.loads(msg.value.decode("utf-8"))
    txn_id = record["txn_id"]
    if txn_id in seen_txn_ids:
        continue
    seen_txn_ids.add(txn_id)
    print(f"Processed unique txn_id: {txn_id} (step={record['step']}, amount={record['amount']})")
    if len(seen_txn_ids) >= 10:
        break
consumer.close()
print(f"Total unique records consumed and deduplicated: {len(seen_txn_ids)}")
