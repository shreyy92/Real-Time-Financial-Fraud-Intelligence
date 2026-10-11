# Member 3: Real-Time Streaming Feature Store (PaySim)

This module implements the Spark Structured Streaming consumer and Kafka producer for real-time transaction feature engineering.

## Architecture & Configuration
- **Kafka Topics**:
  - `financial_transactions_paysim`: Inbound transaction stream.
  - `financial_features_paysim`: Outbound enriched feature stream (Tier A contract).
  - `financial_transactions_paysim_dlq`: Dead Letter Queue for invalid/malformed/late events.
- **State Store Provider**:
  - Uses `org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider`.
  - RocksDB handles out-of-core state offloading for high cardinality keys (~2.7M entities in PaySim).
- **At-Least-Once Semantics**:
  - Spark's Kafka sink provides at-least-once delivery semantics across recovery restarts.
  - Downstream consumers reading from `financial_features_paysim` MUST deduplicate on `txn_id`.

## Demo & Local Execution
> **Important**: Demo reruns require deleting the checkpoint directory (`member3_streaming/checkpoints/`) and recreating Kafka topics to start fresh from offset 0:
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic financial_transactions_paysim
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic financial_features_paysim
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic financial_transactions_paysim_dlq

docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_transactions_paysim --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_features_paysim --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial_transactions_paysim_dlq --partitions 1 --replication-factor 1

rm -rf member3_streaming/checkpoints/
```

### Running the Consumer:
```bash
python -m member3_streaming.consumer_paysim --bootstrap-servers localhost:9092
```

### Running the Producer:
```bash
python member3_streaming/producer_paysim.py --limit 20000 --delay 0
```
