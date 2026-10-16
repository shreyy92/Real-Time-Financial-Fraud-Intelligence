"""Real-Time IEEE-CIS Spark Structured Streaming Consumer for Member 3.

Computes all Tier A stateless and stateful features for IEEE-CIS transactions:
- Validates raw input payloads and routes malformed/invalid records to DLQ.
- Computes deterministic card proxy hashes (card_proxy_id, card_addr_proxy_id).
- Computes relative temporal and cyclical encodings.
- Computes amount transforms and currency conversion fingerprints.
- Computes missingness block indicators and identity coverage metrics.
- Computes masked family aggregations (M, C, D).
- Drops 14 uninformative near-constants.
- Maintains stateful card behavioral metrics per card_proxy_id via applyInPandasWithState:
    * card_prior_txn_count (int32)
    * card_first_seen_flag (int8)
    * card_prior_amt_sum (double)
    * card_prior_amt_mean (double)
    * card_prior_amt_std (double, sample standard deviation via Welford algorithm)
    * card_prior_amt_max (double)
    * amt_zscore_vs_card (double, evaluated when count >= 3 and std > 1e-5)
    * card_secs_since_last_txn (int32)
    * card_txn_count_1h (int32, rangeBetween -3600 to -1)
    * card_txn_count_24h (int32, rangeBetween -86400 to -1)
    * card_txn_count_7d (int32, rangeBetween -604800 to -1)
- Writes enriched 483-column feature vectors to Kafka with explicit null preservation.
"""

import os
import sys
import json
import math
import bisect
import time
import argparse
import datetime
from typing import Tuple, Optional, List, Iterator, Dict, Any

import pandas as pd
import numpy as np

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ByteType,
    BooleanType,
    LongType,
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_member2_dir = os.path.join(_repo_root, "member2_spark")
if _member2_dir not in sys.path:
    sys.path.insert(0, _member2_dir)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

if "HADOOP_HOME" not in os.environ and os.path.exists(r"C:\hadoop"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    bin_path = r"C:\hadoop\bin"
    if bin_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_path + os.pathsep + os.environ.get("PATH", "")

from member3_streaming.ieee_raw_schema import IEEE_RAW_SCHEMA
from member3_streaming.ieee_output_schema import IEEE_OUTPUT_SCHEMA
from src.ieee.card_features import add_ieee_card_features
from src.ieee.temporal_features import add_ieee_temporal_features
from src.ieee.missingness_features import add_ieee_missingness_features
from src.ieee.aggregate_features import (
    add_ieee_amount_features,
    add_ieee_masked_aggregates,
)
from src.ieee.build_features import UNINFORMATIVE_NEAR_CONSTANTS

# State schema stored per card proxy entity
IEEE_STATE_SCHEMA = StructType([
    StructField("count", IntegerType(), True),
    StructField("amount_sum", DoubleType(), True),
    StructField("amount_m2", DoubleType(), True),
    StructField("amount_max", DoubleType(), True),
    StructField("last_dt", IntegerType(), True),
    StructField("last_tid", IntegerType(), True),
    StructField("history_json", StringType(), True),
])

# Required core fields according to IEEE contract
IEEE_REQUIRED_FIELDS = [
    "TransactionID",
    "isFraud",
    "TransactionDT",
    "TransactionAmt",
    "ProductCD",
    "card1",
]

DLQ_EVAL_SCHEMA = StructType([
    StructField("reason_code", StringType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("is_valid", BooleanType(), True),
])

DLQ_RECORD_SCHEMA = StructType([
    StructField("raw_value", StringType(), True),
    StructField("reason_code", StringType(), True),
    StructField("ingested_at", StringType(), True),
])

DLQ_REASON_CODES = {
    "MALFORMED_JSON": "Payload is not valid JSON or not a JSON object",
    "MISSING_FIELD": "One or more required fields are missing or null",
    "BAD_FIELD_TYPE": "A required field has the wrong JSON type",
    "NEGATIVE_AMOUNT": "TransactionAmt is negative (< 0.0)",
    "INVALID_LABEL": "Target label 'isFraud' is not in {0, 1}",
    "INVALID_TRANSACTIONDT": "TransactionDT is not positive (> 0)",
    "LATE_EVENT": "Transaction timestamp/ID is strictly older than key's committed latest (DT, TID)",
}

# Global tracking of maximum velocity history buffer length
MAX_HISTORY_LEN_SEEN = 0


def classify_dlq_record_ieee(raw_val: Optional[str]) -> Tuple[Optional[str], str, bool]:
    """Validate raw JSON IEEE record across DLQ error categories."""
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    if raw_val is None:
        return ("MALFORMED_JSON", now, False)

    try:
        obj = json.loads(raw_val)
        if not isinstance(obj, dict):
            return ("MALFORMED_JSON", now, False)
    except Exception:
        return ("MALFORMED_JSON", now, False)

    # 1. Missing required fields
    for field in IEEE_REQUIRED_FIELDS:
        if field not in obj or obj[field] is None:
            return ("MISSING_FIELD", now, False)

    # 2. Type validation
    try:
        dt = int(obj["TransactionDT"])
        amt = float(obj["TransactionAmt"])
        is_fraud = int(obj["isFraud"])
        tid = int(obj["TransactionID"])
    except (ValueError, TypeError):
        return ("BAD_FIELD_TYPE", now, False)

    if not isinstance(obj["ProductCD"], str):
        return ("BAD_FIELD_TYPE", now, False)

    # 3. Value bounds
    if amt < 0.0:
        return ("NEGATIVE_AMOUNT", now, False)
    if is_fraud not in (0, 1):
        return ("INVALID_LABEL", now, False)
    if dt <= 0:
        return ("INVALID_TRANSACTIONDT", now, False)

    return (None, now, True)


def create_spark_session(app_name: str = "Member3-IEEE-Streaming") -> SparkSession:
    """Create SparkSession configured for RocksDB state store and Kafka streaming."""
    scala_version = "2.13"
    spark_version = "4.1.1"
    kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", kafka_pkg)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config("spark.sql.streaming.stateStore.rocksdb.formatVersion", "5")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.port", "0")
        .config("spark.blockManager.port", "0")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )
    spark = builder.getOrCreate()
    jvm = spark.sparkContext._gateway.jvm
    if os.path.exists(r"C:\hadoop\bin\hadoop.dll"):
        try:
            jvm.java.lang.System.load(r"C:\hadoop\bin\hadoop.dll")
        except Exception:
            pass
    return spark


def add_stateless_features_ieee(df: DataFrame, train_max_day: float = 130.0) -> DataFrame:
    """Apply all stateless feature engineering transformations to IEEE streaming DataFrame."""
    # 1. Missingness & identity features
    df = add_ieee_missingness_features(df)

    # 2. Card proxy entity generation (card_proxy_id, card_addr_proxy_id)
    df = add_ieee_card_features(df)

    # 3. Relative temporal features (hour, sin/cos, day index, weekday, warmup flag)
    df = add_ieee_temporal_features(df)

    # 4. Time-based split assignment
    train_max_seconds = train_max_day * 86400.0
    df = df.withColumn(
        "split",
        F.when(F.col("TransactionDT") <= train_max_seconds, "train").otherwise("valid"),
    )

    # 5. Amount features and currency decimal fingerprints
    df = add_ieee_amount_features(df)

    # 6. Masked column aggregates (M, C, D)
    df = add_ieee_masked_aggregates(df)

    # 7. Drop the 14 low-information near-constants
    cols_to_drop = [c for c in UNINFORMATIVE_NEAR_CONSTANTS if c in df.columns]
    if cols_to_drop:
        df = df.drop(*cols_to_drop)

    # 8. Feature version tag
    df = df.withColumn("feature_version", F.lit("v1.0"))

    return df


_cached_ieee_dlq_producer = None


def get_dlq_producer(bootstrap_servers: str = "127.0.0.1:9092"):
    global _cached_ieee_dlq_producer
    if _cached_ieee_dlq_producer is None:
        from kafka import KafkaProducer
        _cached_ieee_dlq_producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode("utf-8") if isinstance(v, (dict, list)) else v,
        )
    return _cached_ieee_dlq_producer


def update_card_state(
    key: Tuple[str],
    pdf_iter: Iterator[pd.DataFrame],
    state: GroupState,
) -> Iterator[pd.DataFrame]:
    """Stateful micro-batch processor for card_proxy_id using RocksDB-backed GroupState.

    Enforces:
    1. Vectorized computation using numpy arrays (no iterrows).
    2. Strict (DT, TID) monotonicity: rows with (DT, TID) <= last (DT, TID) are routed to DLQ with LATE_EVENT.
    3. Welford state for mean and sample std (n - 1), null when count < 2.
    4. Z-score evaluated only when prior_count >= 3 and prior_std > 1e-5.
    5. Rolling velocity window counts: [cur_dt - W, cur_dt - 1] via bisect.
    6. Prunes history timestamps strictly below cur_dt - 604800.
    7. Nulls preserved via pandas nullable dtypes (Int32, Int8, Float64).
    """
    global MAX_HISTORY_LEN_SEEN

    card_id = key[0]

    if state.exists:
        st = state.get
        s_count = int(st[0])
        s_amount_sum = float(st[1])
        s_amount_m2 = float(st[2])
        s_amount_max = float(st[3]) if st[3] is not None else None
        s_last_dt = int(st[4]) if st[4] is not None else None
        s_last_tid = int(st[5]) if st[5] is not None else None
        history: List[int] = json.loads(st[6]) if st[6] else []
    else:
        s_count = 0
        s_amount_sum = 0.0
        s_amount_m2 = 0.0
        s_amount_max = None
        s_last_dt = None
        s_last_tid = None
        history = []

    for pdf in pdf_iter:
        if pdf.empty:
            continue

        # Sort incoming micro-batch deterministically by (TransactionDT, TransactionID)
        pdf = pdf.sort_values(by=["TransactionDT", "TransactionID"], ascending=[True, True], kind="stable")

        dt_arr = pdf["TransactionDT"].to_numpy(dtype=np.int64)
        tid_arr = pdf["TransactionID"].to_numpy(dtype=np.int64)
        amt_arr = pdf["TransactionAmt"].to_numpy(dtype=np.float64)

        # Monotonicity check against committed key state
        if s_last_dt is not None:
            late_mask = (dt_arr < s_last_dt) | ((dt_arr == s_last_dt) & (tid_arr <= s_last_tid))
            if np.any(late_mask):
                late_rows = pdf[late_mask]
                try:
                    target_dlq = os.environ.get("IEEE_DLQ_TOPIC", "financial_transactions_ieee_dlq")
                    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
                    producer = get_dlq_producer(bootstrap)
                    for r in late_rows.to_dict(orient="records"):
                        dlq_rec = {
                            "raw_value": json.dumps(r, default=str),
                            "reason_code": "LATE_EVENT",
                            "ingested_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        }
                        producer.send(target_dlq, key=str(r.get("TransactionID", "")).encode("utf-8"), value=dlq_rec)
                    producer.flush()
                except Exception as ex:
                    print(f"Warning: Failed to route IEEE late event to DLQ: {ex}")

                pdf = pdf[~late_mask]
                if pdf.empty:
                    continue
                dt_arr = pdf["TransactionDT"].to_numpy(dtype=np.int64)
                tid_arr = pdf["TransactionID"].to_numpy(dtype=np.int64)
                amt_arr = pdf["TransactionAmt"].to_numpy(dtype=np.float64)

        n = len(pdf)
        card_prior_txn_count = np.empty(n, dtype=np.int32)
        card_first_seen_flag = np.empty(n, dtype=np.int8)
        card_prior_amt_sum = np.empty(n, dtype=np.float64)
        card_prior_amt_mean = [None] * n
        card_prior_amt_std = [None] * n
        card_prior_amt_max = [None] * n
        amt_zscore_vs_card = [None] * n
        card_secs_since_last_txn = [None] * n
        card_txn_count_1h = np.empty(n, dtype=np.int32)
        card_txn_count_24h = np.empty(n, dtype=np.int32)
        card_txn_count_7d = np.empty(n, dtype=np.int32)

        for i in range(n):
            cur_dt = int(dt_arr[i])
            cur_tid = int(tid_arr[i])
            amt = float(amt_arr[i])

            card_prior_txn_count[i] = s_count
            card_first_seen_flag[i] = 1 if s_count == 0 else 0
            card_prior_amt_sum[i] = s_amount_sum

            if s_count > 0:
                mean_val = s_amount_sum / s_count
                card_prior_amt_mean[i] = mean_val
                card_prior_amt_max[i] = s_amount_max
                if s_last_dt is not None:
                    card_secs_since_last_txn[i] = cur_dt - s_last_dt

            if s_count >= 2:
                var = s_amount_m2 / (s_count - 1)
                std_val = math.sqrt(max(0.0, var))
                card_prior_amt_std[i] = std_val
                if s_count >= 3 and std_val > 1e-5:
                    amt_zscore_vs_card[i] = (amt - mean_val) / std_val

            # Rolling velocity window counts: [cur_dt - W, cur_dt - 1] via bisect
            idx_r = bisect.bisect_right(history, cur_dt - 1)
            card_txn_count_1h[i] = idx_r - bisect.bisect_left(history, cur_dt - 3600)
            card_txn_count_24h[i] = idx_r - bisect.bisect_left(history, cur_dt - 86400)
            card_txn_count_7d[i] = idx_r - bisect.bisect_left(history, cur_dt - 604800)

            # Update running state via Welford algorithm
            s_count += 1
            if s_count == 1:
                s_amount_sum = amt
                s_amount_m2 = 0.0
                s_amount_max = amt
            else:
                old_mean = s_amount_sum / (s_count - 1)
                s_amount_sum += amt
                new_mean = s_amount_sum / s_count
                s_amount_m2 += (amt - old_mean) * (amt - new_mean)
                if amt > s_amount_max:
                    s_amount_max = amt

            s_last_dt = cur_dt
            s_last_tid = cur_tid

            history.append(cur_dt)
            prune_threshold = cur_dt - 604800
            p_idx = bisect.bisect_left(history, prune_threshold)
            if p_idx > 0:
                history = history[p_idx:]

            if len(history) > MAX_HISTORY_LEN_SEEN:
                MAX_HISTORY_LEN_SEEN = len(history)

        # Assign 11 stateful feature columns to pdf preserving nullable types
        new_cols_df = pd.DataFrame(
            {
                "card_prior_txn_count": pd.Series(card_prior_txn_count, index=pdf.index, dtype="Int32"),
                "card_first_seen_flag": pd.Series(card_first_seen_flag, index=pdf.index, dtype="Int8"),
                "card_prior_amt_sum": pd.Series(card_prior_amt_sum, index=pdf.index, dtype="Float64"),
                "card_prior_amt_mean": pd.Series(card_prior_amt_mean, index=pdf.index, dtype="Float64"),
                "card_prior_amt_std": pd.Series(card_prior_amt_std, index=pdf.index, dtype="Float64"),
                "card_prior_amt_max": pd.Series(card_prior_amt_max, index=pdf.index, dtype="Float64"),
                "amt_zscore_vs_card": pd.Series(amt_zscore_vs_card, index=pdf.index, dtype="Float64"),
                "card_secs_since_last_txn": pd.Series(card_secs_since_last_txn, index=pdf.index, dtype="Int32"),
                "card_txn_count_1h": pd.Series(card_txn_count_1h, index=pdf.index, dtype="Int32"),
                "card_txn_count_24h": pd.Series(card_txn_count_24h, index=pdf.index, dtype="Int32"),
                "card_txn_count_7d": pd.Series(card_txn_count_7d, index=pdf.index, dtype="Int32"),
            },
            index=pdf.index,
        )
        pdf = pd.concat([pdf, new_cols_df], axis=1)

        # Update RocksDB state store
        new_state = (
            int(s_count),
            float(s_amount_sum),
            float(s_amount_m2),
            float(s_amount_max) if s_amount_max is not None else None,
            int(s_last_dt) if s_last_dt is not None else None,
            int(s_last_tid) if s_last_tid is not None else None,
            json.dumps(history),
        )
        state.update(new_state)

        # Reorder columns to strictly match IEEE_OUTPUT_SCHEMA
        out_df = pdf[[field.name for field in IEEE_OUTPUT_SCHEMA.fields]]

        # Optional test hook for trickle sleep or mid-batch crash
        trickle_sleep = os.environ.get("TRICKLE_BATCH_SLEEP", "0")
        if float(trickle_sleep) > 0:
            time.sleep(float(trickle_sleep))

        yield out_df


def build_streaming_pipeline_ieee(
    spark: SparkSession,
    bootstrap_servers: str = "127.0.0.1:9092",
    input_topic: str = "financial_transactions_ieee",
    output_topic: Optional[str] = "financial_features_ieee",
    dlq_topic: Optional[str] = "financial_transactions_ieee_dlq",
    checkpoint_location: Optional[str] = "member3_streaming/checkpoints/ieee_stateful",
    max_offsets_per_trigger: Optional[int] = None,
    starting_offsets: str = "earliest",
):
    """Construct and start end-to-end stateful Spark Structured Streaming query for IEEE features."""
    classify_udf = F.udf(classify_dlq_record_ieee, DLQ_EVAL_SCHEMA)

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
    )
    if max_offsets_per_trigger is not None:
        reader = reader.option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))

    raw_stream = reader.load()

    eval_stream = (
        raw_stream.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
        )
        .withColumn("eval", classify_udf(F.col("raw_json")))
    )

    # Branch 1: Malformed / invalid records -> route to DLQ
    malformed_stream = (
        eval_stream.filter(F.col("eval.is_valid") == False)
        .select(
            F.col("kafka_key").alias("key"),
            F.to_json(
                F.struct(
                    F.col("raw_json").alias("raw_value"),
                    F.col("eval.reason_code").alias("reason_code"),
                    F.col("eval.ingested_at").alias("ingested_at"),
                ),
                {"ignoreNullFields": "false"},
            ).alias("value"),
        )
    )

    if dlq_topic and checkpoint_location:
        dlq_chk = os.path.join(checkpoint_location, "dlq")
        malformed_stream.writeStream.format("kafka").option(
            "kafka.bootstrap.servers", bootstrap_servers
        ).option("topic", dlq_topic).option(
            "checkpointLocation", dlq_chk
        ).outputMode("append").start()

    # Branch 2: Valid records -> stateless features
    valid_stream = (
        eval_stream.filter(F.col("eval.is_valid") == True)
        .select(F.from_json(F.col("raw_json"), IEEE_RAW_SCHEMA).alias("data"))
        .select("data.*")
    )

    stateless_df = add_stateless_features_ieee(valid_stream)

    # Branch 2b: Stateful features via applyInPandasWithState
    stateful_df = (
        stateless_df.groupBy("card_proxy_id")
        .applyInPandasWithState(
            func=update_card_state,
            outputStructType=IEEE_OUTPUT_SCHEMA,
            stateStructType=IEEE_STATE_SCHEMA,
            outputMode="Update",
            timeoutConf=GroupStateTimeout.NoTimeout,
        )
    )

    if output_topic and checkpoint_location:
        kafka_payload = stateful_df.select(
            F.col("TransactionID").cast("string").alias("key"),
            F.to_json(
                F.struct([F.col(c) for c in stateful_df.columns]),
                {"ignoreNullFields": "false"},
            ).alias("value"),
        )
        features_chk = os.path.join(checkpoint_location, "features")
        query = (
            kafka_payload.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("topic", output_topic)
            .option("checkpointLocation", features_chk)
            .outputMode("update")
            .start()
        )
        return query

    return stateful_df


def build_stateless_pipeline_ieee(
    spark: SparkSession,
    bootstrap_servers: str = "127.0.0.1:9092",
    input_topic: str = "financial_transactions_ieee",
    output_topic: Optional[str] = "financial_features_ieee",
    dlq_topic: Optional[str] = "financial_transactions_ieee_dlq",
    checkpoint_location: Optional[str] = None,
    max_offsets_per_trigger: Optional[int] = None,
    starting_offsets: str = "earliest",
):
    """Construct and start stateless IEEE streaming pipeline (for Chunk 8 verification and --stateless-only)."""
    classify_udf = F.udf(classify_dlq_record_ieee, DLQ_EVAL_SCHEMA)

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
    )
    if max_offsets_per_trigger is not None:
        reader = reader.option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))

    raw_stream = reader.load()

    eval_stream = (
        raw_stream.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
        )
        .withColumn("eval", classify_udf(F.col("raw_json")))
    )

    malformed_stream = (
        eval_stream.filter(F.col("eval.is_valid") == False)
        .select(
            F.col("kafka_key").alias("key"),
            F.to_json(
                F.struct(
                    F.col("raw_json").alias("raw_value"),
                    F.col("eval.reason_code").alias("reason_code"),
                    F.col("eval.ingested_at").alias("ingested_at"),
                ),
                {"ignoreNullFields": "false"},
            ).alias("value"),
        )
    )

    if dlq_topic and checkpoint_location:
        dlq_chk = os.path.join(checkpoint_location, "dlq")
        malformed_stream.writeStream.format("kafka").option(
            "kafka.bootstrap.servers", bootstrap_servers
        ).option("topic", dlq_topic).option(
            "checkpointLocation", dlq_chk
        ).outputMode("append").start()

    valid_stream = (
        eval_stream.filter(F.col("eval.is_valid") == True)
        .select(F.from_json(F.col("raw_json"), IEEE_RAW_SCHEMA).alias("data"))
        .select("data.*")
    )

    features_df = add_stateless_features_ieee(valid_stream)

    if output_topic and checkpoint_location:
        kafka_payload = features_df.select(
            F.col("TransactionID").cast("string").alias("key"),
            F.to_json(
                F.struct([F.col(c) for c in features_df.columns]),
                {"ignoreNullFields": "false"},
            ).alias("value"),
        )
        features_chk = os.path.join(checkpoint_location, "features")
        return (
            kafka_payload.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("topic", output_topic)
            .option("checkpointLocation", features_chk)
            .outputMode("append")
            .start()
        )

    return features_df


def main():
    parser = argparse.ArgumentParser(description="IEEE Structured Streaming Consumer")
    parser.add_argument("--bootstrap-servers", type=str, default="127.0.0.1:9092")
    parser.add_argument("--input-topic", type=str, default="financial_transactions_ieee")
    parser.add_argument("--output-topic", type=str, default="financial_features_ieee")
    parser.add_argument("--dlq-topic", type=str, default="financial_transactions_ieee_dlq")
    parser.add_argument("--checkpoint-location", type=str, default="member3_streaming/checkpoints/ieee_stateful")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=None)
    parser.add_argument("--stateless-only", action="store_true", help="Run stateless features only")
    args = parser.parse_args()

    if args.dlq_topic:
        os.environ["IEEE_DLQ_TOPIC"] = args.dlq_topic
    if args.bootstrap_servers:
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = args.bootstrap_servers

    spark = create_spark_session()
    print("SparkSession initialized. Starting IEEE streaming consumer...")
    if args.stateless_only:
        query = build_stateless_pipeline_ieee(
            spark=spark,
            bootstrap_servers=args.bootstrap_servers,
            input_topic=args.input_topic,
            output_topic=args.output_topic,
            dlq_topic=args.dlq_topic,
            checkpoint_location=args.checkpoint_location,
            max_offsets_per_trigger=args.max_offsets_per_trigger,
        )
    else:
        query = build_streaming_pipeline_ieee(
            spark=spark,
            bootstrap_servers=args.bootstrap_servers,
            input_topic=args.input_topic,
            output_topic=args.output_topic,
            dlq_topic=args.dlq_topic,
            checkpoint_location=args.checkpoint_location,
            max_offsets_per_trigger=args.max_offsets_per_trigger,
        )
    print(f"Streaming query started to topic: {args.output_topic}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
