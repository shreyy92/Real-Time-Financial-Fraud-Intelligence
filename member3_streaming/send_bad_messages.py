import argparse
import json
import time
from kafka import KafkaProducer

def main():
    parser = argparse.ArgumentParser(description="Send 6 categories of bad messages to test DLQ")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092")
    parser.add_argument("--topic", type=str, default="financial_transactions_paysim")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode("utf-8") if isinstance(v, (dict, list)) else v.encode("utf-8")
    )

    bad_samples = [
        # 1. Malformed JSON
        "CORRUPTED_NON_JSON_PAYLOAD_STRING{{{",
        # 2. Missing field (missing 'step' and 'nameDest')
        {"type": "TRANSFER", "amount": 1500.0, "nameOrig": "C_MISSING_1", "oldbalanceOrg": 5000.0, "newbalanceOrig": 3500.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0},
        # 3. Bad type (step is a string instead of int)
        {"step": "one_hundred", "type": "PAYMENT", "amount": 250.0, "nameOrig": "C_BADTYPE_1", "nameDest": "M_DEST1", "oldbalanceOrg": 500.0, "newbalanceOrig": 250.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0},
        # 4. Negative amount
        {"step": 10, "type": "PAYMENT", "amount": -99.99, "nameOrig": "C_NEG_1", "nameDest": "M_DEST2", "oldbalanceOrg": 500.0, "newbalanceOrig": 500.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0},
        # 5. Out-of-bounds step (step 9999)
        {"step": 9999, "type": "CASH_OUT", "amount": 300.0, "nameOrig": "C_STEP_9999", "nameDest": "M_DEST3", "oldbalanceOrg": 500.0, "newbalanceOrig": 200.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0},
        # 6. Invalid label (isFraud = 2)
        {"step": 15, "type": "TRANSFER", "amount": 1000.0, "nameOrig": "C_FRAUD_2", "nameDest": "M_DEST4", "oldbalanceOrg": 1000.0, "newbalanceOrig": 0.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 2},
    ]

    print(f"Sending all 6 bad message kinds to {args.topic}...")
    for idx, sample in enumerate(bad_samples, 1):
        producer.send(args.topic, value=sample)
        print(f"  Sent bad message #{idx}: {sample}")

    producer.flush()
    producer.close()
    print("Done sending bad messages.")

if __name__ == "__main__":
    main()
