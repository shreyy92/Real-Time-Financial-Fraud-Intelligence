import json
import time
from kafka import KafkaProducer

def send_smoke_message():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    test_msg = {
        "txn_id": "smoke_test_001",
        "step": 1,
        "type": "TRANSFER",
        "amount": 250.75,
        "nameOrig": "C123456789",
        "nameDest": "M987654321",
        "timestamp": time.time()
    }
    
    print(f"Sending smoke test message to 'financial_transactions_paysim': {test_msg}")
    future = producer.send("financial_transactions_paysim", test_msg)
    record_metadata = future.get(timeout=10)
    producer.flush()
    producer.close()
    print(f"Message successfully delivered to topic '{record_metadata.topic}' partition {record_metadata.partition} offset {record_metadata.offset}")

if __name__ == "__main__":
    send_smoke_message()
