import argparse
import time
import json
import pandas as pd
from kafka import KafkaProducer

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'item'):
            return obj.item()
        return super().default(obj)

def json_serializer(data):
    return json.dumps(data, cls=NumpyEncoder, allow_nan=False).encode('utf-8')

def main():
    parser = argparse.ArgumentParser(description="PaySim Kafka Producer")
    parser.add_argument("--limit", type=int, default=50000, help="Number of rows to send (0 = all)")
    parser.add_argument("--delay", type=float, default=0.05, help="Delay in seconds between messages")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka bootstrap servers")
    args = parser.parse_args()

    print("Loading parquet data...")
    df = pd.read_parquet("data/processed/paysim_clean.parquet")
    
    print("Sorting by step...")
    df = df.sort_values(by="step", ascending=True, kind="stable")
    
    if args.limit > 0:
        df = df.head(args.limit)
        
    if "step_hour" in df.columns:
        df = df.drop(columns=["step_hour"])
        
    required_cols = [
        "step", "type", "amount", "nameOrig", "oldbalanceOrg", 
        "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest", 
        "isFraud", "isFlaggedFraud"
    ]
    df = df[required_cols]

    print(f"Connecting to Kafka at {args.bootstrap_servers}...")
    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_servers],
        value_serializer=json_serializer
    )
    
    topic = "financial_transactions_paysim"
    print(f"Starting to produce messages to topic: {topic}")
    
    total = len(df)
    
    for i, row in enumerate(df.itertuples(index=False), 1):
        message = row._asdict()
        producer.send(topic, value=message)
        
        if i % 1000 == 0:
            print(f"Sent {i}/{total} messages...")
            
        if args.delay > 0:
            time.sleep(args.delay)
            
    producer.flush()
    producer.close()
    print(f"Done. Sent {total} messages to {topic}.")

if __name__ == "__main__":
    main()
