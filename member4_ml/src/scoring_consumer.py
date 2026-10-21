"""
Real-Time Kafka Fraud Scoring Consumer for Member 4.

Consumes enriched feature records emitted by Member 3:
- PaySim topic: `financial_features_paysim` (Key: txn_id)
- IEEE topic: `financial_features_ieee` (Key: TransactionID)

Publishes scored fraud predictions to Member 5:
- PaySim topic: `fraud_scores_paysim`
- IEEE topic: `fraud_scores_ieee`

Features:
1. Bounded LRU in-memory deduplication cache on primary transaction key.
   (Handles Spark Structured Streaming / Kafka at-least-once replay duplicates).
2. Explicit artifact loading: requires an existing fitted model and metadata JSON.
   Never silently trains models or substitutes synthetic data.
3. Strict non-leakage & column order enforcement: strips labels, IDs, metadata,
   and aligns live feature vector to training schema.
4. Error handling & DLQ/logging for malformed JSON, missing features, and output dispatch failures.
5. Emits downstream schema matching Member 5 contract:
   {
       "txn_id": "...",
       "fraud_score": 0.8421,
       "model_version": "v1.0",
       "scored_at": "2026-10-22T00:00:00.000000Z"
   }
"""

from typing import Dict, Any, Optional, List, Tuple, Union
import os
import json
import logging
from collections import OrderedDict
from datetime import datetime, timezone
import pandas as pd
import numpy as np

from member4_ml.src.feature_selection import load_config
from member4_ml.src.train import ModelTrainer

logger = logging.getLogger("Member4ScoringConsumer")


class BoundedDeduplicationCache:
    """
    In-memory bounded LRU cache for deduplicating transaction IDs.

    NOTE: This cache is in-memory and not durable across process restarts.
    It protects against at-least-once streaming duplicates within a rolling window,
    but does not guarantee global exactly-once delivery across crashes.
    """

    def __init__(self, capacity: int = 100000):
        if capacity <= 0:
            raise ValueError(f"Cache capacity must be positive, got {capacity}")
        self.capacity = capacity
        self._cache: OrderedDict = OrderedDict()

    def check_and_add(self, key: Any) -> bool:
        """
        Check if key has been seen. If seen, returns True (duplicate).
        If new, adds to cache and returns False (not duplicate).
        """
        if key in self._cache:
            # Move to end (most recently seen)
            self._cache.move_to_end(key)
            return True

        self._cache[key] = True
        if len(self._cache) > self.capacity:
            # Evict oldest entry (FIFO / LRU)
            self._cache.popitem(last=False)
        return False

    def __len__(self) -> int:
        return len(self._cache)

    def contains(self, key: Any) -> bool:
        return key in self._cache


class FraudScoringEngine:
    """
    Decoupled scoring engine that processes feature dictionaries/records
    using a pre-trained model artifact.
    """

    def __init__(
        self,
        dataset: str,
        model_artifact_path: str,
        cache_capacity: int = 100000,
        model_version: str = "v1.0",
        config: Optional[Dict[str, Any]] = None,
    ):
        self.dataset = dataset.lower()
        if self.dataset not in ("paysim", "ieee"):
            raise ValueError(f"Unknown dataset '{dataset}'. Must be 'paysim' or 'ieee'.")

        self.config = config or load_config()
        self.ds_config = self.config[self.dataset]
        self.primary_key = self.ds_config["primary_key"]
        self.target_col = self.ds_config["target_col"]
        self.model_version = model_version

        # Explicit artifact loading: Fail fast if artifact does not exist
        if not os.path.exists(model_artifact_path):
            raise FileNotFoundError(
                f"Model artifact not found at '{model_artifact_path}'.\n"
                f"Member 4 consumer requires an existing trained model artifact.\n"
                f"Train a model first using: python member4_ml/run_pipeline.py --mode train --dataset {self.dataset}"
            )

        self.model = ModelTrainer.load_artifact(model_artifact_path)
        self.expected_features = self.model.feature_names
        self.dedup_cache = BoundedDeduplicationCache(capacity=cache_capacity)

    def process_record(self, raw_record: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        """
        Process a single raw transaction record from Kafka.

        Returns
        -------
        score_payload : Optional[Dict[str, Any]]
            Formatted scored alert dict if processed, or None if skipped/invalid.
        status : str
            One of: 'SCORED', 'DUPLICATE', 'MALFORMED', 'MISSING_KEY', 'MISSING_FEATURES'
        """
        if not isinstance(raw_record, dict):
            return None, "MALFORMED"

        # 1. Primary key validation
        if self.primary_key not in raw_record:
            return None, "MISSING_KEY"

        key_val = raw_record[self.primary_key]
        if key_val is None or str(key_val).strip() == "":
            return None, "MISSING_KEY"

        # 2. Idempotent Deduplication Check
        if self.dedup_cache.check_and_add(key_val):
            return None, "DUPLICATE"

        # 3. Feature extraction and validation
        missing_features = [f for f in self.expected_features if f not in raw_record]
        if missing_features:
            logger.warning(
                f"Record {key_val} missing {len(missing_features)} features: {missing_features[:3]}..."
            )
            return None, "MISSING_FEATURES"

        # 4. Construct single-row DataFrame aligned strictly to training feature order
        feature_dict = {f: [raw_record[f]] for f in self.expected_features}
        df_row = pd.DataFrame(feature_dict)

        # 5. Model Inference (predict probability of fraud)
        try:
            prob = float(self.model.predict_proba(df_row)[0])
        except Exception as e:
            logger.error(f"Inference failure on transaction {key_val}: {e}")
            raise

        if not (0.0 <= prob <= 1.0) or np.isnan(prob):
            raise ValueError(f"Invalid model prediction probability: {prob}")

        # 6. Format output payload matching Member 5 contract
        # Use ISO-8601 UTC timestamp
        scored_at = datetime.now(timezone.utc).isoformat()

        score_payload = {
            self.primary_key: key_val,
            "fraud_score": round(prob, 4),
            "model_version": self.model_version,
            "scored_at": scored_at,
        }

        return score_payload, "SCORED"


class KafkaScoringConsumer:
    """
    Kafka consumer daemon that polls the streaming feature topic and publishes
    scored alerts to the downstream scoring topic.
    """

    def __init__(
        self,
        dataset: str,
        model_artifact_path: str,
        bootstrap_servers: str = "localhost:9092",
        group_id: Optional[str] = None,
        input_topic: Optional[str] = None,
        output_topic: Optional[str] = None,
        cache_capacity: int = 100000,
        model_version: str = "v1.0",
        config: Optional[Dict[str, Any]] = None,
    ):
        self.dataset = dataset.lower()
        self.config = config or load_config()
        self.ds_config = self.config[self.dataset]

        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic or self.ds_config["streaming_topic"]
        self.output_topic = output_topic or self.ds_config["scoring_topic"]
        self.group_id = group_id or f"member4_scoring_consumer_{self.dataset}"

        self.engine = FraudScoringEngine(
            dataset=self.dataset,
            model_artifact_path=model_artifact_path,
            cache_capacity=cache_capacity,
            model_version=model_version,
            config=self.config,
        )
        self.consumer = None
        self.producer = None

    def start(self, max_messages: Optional[int] = None, timeout_ms: int = 5000) -> Dict[str, int]:
        """
        Start consumer poll loop.

        Parameters
        ----------
        max_messages : Optional[int]
            If provided, stops after consuming this many records (for testing).
        timeout_ms : int
            Kafka consumer timeout in milliseconds.

        Returns
        -------
        stats : Dict[str, int]
            Summary count of processed, duplicate, malformed, and published records.
        """
        from kafka import KafkaConsumer, KafkaProducer

        logger.info(
            f"Initializing Kafka Consumer: Input='{self.input_topic}', Output='{self.output_topic}', "
            f"Brokers='{self.bootstrap_servers}'"
        )

        self.consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=self.bootstrap_servers.split(","),
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # Manual commit after successful processing
            group_id=self.group_id,
            consumer_timeout_ms=timeout_ms,
        )

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
        )

        stats = {"processed": 0, "scored": 0, "duplicates": 0, "errors": 0}

        try:
            for msg in self.consumer:
                stats["processed"] += 1
                try:
                    payload = json.loads(msg.value.decode("utf-8"))
                except Exception as e:
                    logger.warning(f"Failed to parse JSON payload at offset {msg.offset}: {e}")
                    stats["errors"] += 1
                    continue

                score_payload, status = self.engine.process_record(payload)

                if status == "SCORED":
                    key_val = score_payload[self.engine.primary_key]
                    try:
                        future = self.producer.send(
                            self.output_topic,
                            key=key_val,
                            value=score_payload,
                        )
                        # Flush or verify delivery
                        future.get(timeout=10)
                        stats["scored"] += 1
                    except Exception as e:
                        logger.error(f"Failed to publish scored alert for {key_val}: {e}")
                        stats["errors"] += 1
                        raise RuntimeError(f"Downstream publish failure on key {key_val}") from e

                elif status == "DUPLICATE":
                    stats["duplicates"] += 1
                else:
                    stats["errors"] += 1

                # Commit offset only after successful processing and publishing
                self.consumer.commit()

                if max_messages and stats["processed"] >= max_messages:
                    logger.info(f"Reached max_messages={max_messages}. Stopping.")
                    break

        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
            if self.consumer:
                self.consumer.close()

        return stats
