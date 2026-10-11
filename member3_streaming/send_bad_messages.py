import argparse
import json
import time
from kafka import KafkaProducer

def main():
    parser = argparse.ArgumentParser(description="Send malformed messages to trigger DLQ")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092")
    parser.add_argument("--topic", type=str, default="financial_transactions_paysim")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode("utf-8") if isinstance(v, (dict, list)) else v.encode("utf-8")
    )

    bad_samples = [
        {"step": None, "type": "TRANSFER", "amount": 1000.0, "nameOrig": "C_BAD1", "nameDest": "M_DEST1"},
        {"step": 1, "type": "PAYMENT", "amount": None, "nameOrig": "C_BAD2", "nameDest": "M_DEST2"},
        {"step": 1, "type": "CASH_OUT", "amount": 500.0, "nameOrig": "C_BAD3", "nameDest": None},
        "NOT_A_VALID_JSON_STRING_AT_ALL_CORRUPTED",
    ]

    print(f"Sending {len(bad_samples)} bad messages to {args.topic}...")
    for idx, sample in enumerate(bad_samples, 1):
        producer.send(args.topic, value=sample)
        print(f"  Sent bad message {idx}: {sample}")

    producer.flush()
    producer.close()
    print("Done sending bad messages.")

if __name__ == "__main__":
    main()
