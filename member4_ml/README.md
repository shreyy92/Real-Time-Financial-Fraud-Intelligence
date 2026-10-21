# Member 4: Machine Learning Modeling, Evaluation & Real-Time Scoring

## Overview
Member 4 implements the fraud-detection machine learning intelligence layer for the **Real-Time Financial Fraud Intelligence Platform**. It consumes the ML-ready batch feature datasets produced by **Member 2** (`data/features/*.parquet`), enforces strict zero-leakage policies, establishes class imbalance evaluation metrics, and integrates directly with the real-time streaming feature topics engineered by **Member 3** (`financial_features_*`), emitting scored predictions to **Member 5** (`fraud_scores_*`).

---

## Architecture & Data Flow

```
+-------------------------------------------------------------+
| Member 2: Batch Feature Store (Parquet)                     |
| - data/features/paysim_features.parquet (48 cols)           |
| - data/features/ieee_features.parquet (483 cols)            |
+------------------------------+------------------------------+
                               | Offline Training (Time Splits)
                               v
+-------------------------------------------------------------+
| Member 4: ML Modeling & Evaluation                          |
| - Precomputed Time Splits: train / valid / test             |
| - Strict Tier A Isolation (Authorization-Time Features)     |
| - Tier B Simulator Artifact Ablation Study                  |
| - Imbalance Evaluation: PR-AUC, ROC-AUC, Precision, Recall  |
| - Model Artifact Serialization (models/*.joblib)            |
+------------------------------+------------------------------+
                               | Live Feature Streaming (Kafka)
                               v
+-------------------------------------------------------------+
| Member 3: Kafka Streaming Topics                            |
| - financial_features_paysim (42 cols, key: txn_id)          |
| - financial_features_ieee (483 cols, key: TransactionID)    |
+------------------------------+------------------------------+
                               | Real-Time Ingest & Deduplication
                               v
+-------------------------------------------------------------+
| Member 4: Kafka Scoring Consumer (scoring_consumer.py)      |
| - Bounded LRU Deduplication Cache (txn_id / TransactionID)  |
| - Strips Metadata/Labels, Formats Model Vector              |
| - Real-Time Inference (model.predict_proba)                 |
+------------------------------+------------------------------+
                               | Scored Prediction Alerts
                               v
+-------------------------------------------------------------+
| Downstream: Storage & API Alerts (Member 5)                 |
| - fraud_scores_paysim (txn_id, fraud_score, scored_at)      |
| - fraud_scores_ieee (TransactionID, fraud_score, scored_at) |
+-------------------------------------------------------------+
```

---

## CLI Usage Guide (`run_pipeline.py`)

Member 4 provides a unified command-line runner supporting four execution modes:

### 1. Model Training
Train a fraud classification model (LightGBM or Baseline Logistic Regression) using precomputed `train` and `valid` splits:
```powershell
# Train LightGBM model on PaySim (Tier A only)
.\.venv\Scripts\python.exe member4_ml/run_pipeline.py --mode train --dataset paysim --model lightgbm

# Train Baseline Logistic Regression on IEEE-CIS
.\.venv\Scripts\python.exe member4_ml/run_pipeline.py --mode train --dataset ieee --model baseline
```

### 2. Model Evaluation
Evaluate a saved model artifact on validation or held-out test data:
```powershell
.\.venv\Scripts\python.exe member4_ml/run_pipeline.py `
    --mode evaluate `
    --dataset paysim `
    --model-artifact member4_ml/models/paysim_lightgbm.joblib `
    --split test
```

### 3. PaySim Simulator Artifact Ablation
Compare Tier A (32 authorization-time features) vs Tier A+B (37 features including post-transaction balance deltas):
```powershell
.\.venv\Scripts\python.exe member4_ml/run_pipeline.py --mode ablate --dataset paysim
```

### 4. Real-Time Kafka Scoring Consumer
Start the streaming consumer daemon to score live transaction features arriving from Member 3:
```powershell
.\.venv\Scripts\python.exe member4_ml/run_pipeline.py `
    --mode consume `
    --dataset paysim `
    --model-artifact member4_ml/models/paysim_lightgbm.joblib `
    --bootstrap-servers localhost:9092
```

---

## Deduplication & Streaming Semantics

1. **At-Least-Once Delivery**: Spark Structured Streaming with RocksDB state stores guarantees at-least-once delivery. Duplicate messages emitted upon recovery are 100% byte-identical.
2. **In-Memory LRU Cache**: `BoundedDeduplicationCache` suppresses duplicate arrivals within a rolling window of 100,000 transactions (configurable via `--cache-capacity`).
3. **Restart Semantics Note**: The in-memory cache is non-durable across process crashes. For mission-critical persistent exactly-once scoring across supervisor restarts, an external persistent key-value store (e.g. Redis or RocksDB) would be used.
4. **Offset Commit**: In `KafkaScoringConsumer`, offsets are manually committed **only after** predictions are successfully formatted and accepted by the output producer.

---

## Automated Test Suite
Run all Member 4 unit and integration tests:
```powershell
.\.venv\Scripts\python.exe -m pytest member4_ml/tests -v
```
*(Tests cover schema checks, non-leakage, split boundaries, metrics, threshold optimization, baseline preprocessing, LightGBM native null handling, ablation, deduplication cache eviction, and mock Kafka scoring)*.
