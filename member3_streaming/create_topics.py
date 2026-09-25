import subprocess
import sys

TOPICS = ["financial_transactions_paysim", "financial_transactions_ieee"]

def create_topics():
    for topic in TOPICS:
        cmd = [
            "docker", "exec", "kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "localhost:9092",
            "--create", "--if-not-exists",
            "--topic", topic,
            "--partitions", "1",
            "--replication-factor", "1"
        ]
        print(f"Creating topic '{topic}'...")
        res = subprocess.run(cmd, capture_output=True, text=True)
        print(res.stdout or res.stderr)

def list_topics():
    cmd = [
        "docker", "exec", "kafka",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server", "localhost:9092",
        "--list"
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    print("Existing Kafka topics:")
    print(res.stdout)

if __name__ == "__main__":
    create_topics()
    list_topics()
