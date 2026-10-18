"""
Real-Time IEEE-CIS Kafka Producer for Member 3.

Reads cleaned IEEE-CIS parquet dataset, stably sorts by (TransactionDT, TransactionID),
and streams raw transaction events to Kafka topic `financial_transactions_ieee`.
"""

import argparse
import time
import json
import math
import pandas as pd
import numpy as np
from kafka import KafkaProducer


class NumpyEncoder(json.JSONEncoder):
    """Fallback JSON encoder for numpy data types."""
    def default(self, obj):
        if hasattr(obj, "item"):
            val = obj.item()
            if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                return None
            return val
        if pd.isna(obj):
            return None
        return super().default(obj)


def sanitize_record(row_dict: dict) -> dict:
    """
    Sanitize row dictionary for strict JSON compliance:
    - Converts numpy scalars (int64, float32, bool) to native Python types.
    - Converts NaN, NaT, inf, and nulls to None (which serialize to JSON null).
    """
    cleaned = {}
    for k, v in row_dict.items():
        if pd.isna(v):
            cleaned[k] = None
        elif hasattr(v, "item"):
            val = v.item()
            if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                cleaned[k] = None
            else:
                cleaned[k] = val
        elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            cleaned[k] = None
        else:
            cleaned[k] = v
    return cleaned


def json_serializer(data: dict) -> bytes:
    """Serialize dictionary to JSON bytes with allow_nan=False to prevent invalid NaN leakage."""
    return json.dumps(data, cls=NumpyEncoder, allow_nan=False).encode("utf-8")


def main():
    parser = argparse.ArgumentParser(description="IEEE-CIS Kafka Producer")
    parser.add_argument("--limit", type=int, default=50000, help="Number of rows to send (0 = all)")
    parser.add_argument("--delay", type=float, default=0.05, help="Delay in seconds between messages")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument(
        "--key-field",
        type=str,
        default="card1",
        choices=["card1", "TransactionID"],
        help="Field to use as Kafka record key (default: card1)",
    )
    args = parser.parse_args()

    input_path = "data/processed/ieee_clean.parquet"
    print(f"Loading parquet data from {input_path}...")
    df = pd.read_parquet(input_path)

    print("Sorting by (TransactionDT, TransactionID) with stable sort...")
    sort_indices = np.lexsort((df["TransactionID"].values, df["TransactionDT"].values))
    if args.limit > 0:
        sort_indices = sort_indices[:args.limit]
    df = df.iloc[sort_indices]

    total = len(df)
    print(f"Prepared {total} events to produce (key: {args.key_field}).")

    print(f"Connecting to Kafka at {args.bootstrap_servers}...")
    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_servers],
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        value_serializer=json_serializer,
    )

    topic = "financial_transactions_ieee"
    print(f"Starting to produce messages to topic: {topic}")

    records = df.to_dict(orient="records")
    for i, record in enumerate(records, 1):
        clean_msg = sanitize_record(record)
        key = str(clean_msg[args.key_field])
        producer.send(topic, key=key, value=clean_msg)

        if i % 5000 == 0:
            print(f"Sent {i}/{total} messages...")

        if args.delay > 0:
            time.sleep(args.delay)

    producer.flush()
    producer.close()
    print(f"Done. Sent {total} messages to {topic}.")


if __name__ == "__main__":
    main()
