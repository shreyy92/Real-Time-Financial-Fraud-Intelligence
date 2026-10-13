"""Real-Time IEEE-CIS Spark Structured Streaming Consumer for Member 3.

Computes all Tier A stateless features for IEEE-CIS transactions:
- Validates raw input payloads and routes malformed/invalid records to DLQ.
- Computes deterministic card proxy hashes (card_proxy_id, card_addr_proxy_id).
- Computes relative temporal and cyclical encodings.
- Computes amount transforms and currency conversion fingerprints.
- Computes missingness block indicators and identity coverage metrics.
- Computes masked family aggregations (M, C, D).
- Drops 14 uninformative near-constants.
- Writes enriched feature vectors to Kafka with explicit null preservation.
"""

import os
import sys
import json
import math
import argparse
import datetime
from typing import Tuple, Optional, List

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
)

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
from src.ieee.card_features import add_ieee_card_features
from src.ieee.temporal_features import add_ieee_temporal_features
from src.ieee.missingness_features import add_ieee_missingness_features
from src.ieee.aggregate_features import (
    add_ieee_amount_features,
    add_ieee_masked_aggregates,
)
from src.ieee.build_features import UNINFORMATIVE_NEAR_CONSTANTS

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
    "INVALID_LABEL": "Target label isFraud is not in {0, 1}",
    "INVALID_TRANSACTIONDT": "TransactionDT is negative or out of plausible bounds",
    "LATE_EVENT": "Transaction timestamp is strictly older than key's committed state",
}


def classify_dlq_record_ieee(raw_str: str) -> Tuple[Optional[str], str, bool]:
    """Validate raw JSON payload and classify DLQ reason code for IEEE transactions."""
    now_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    if not raw_str or not isinstance(raw_str, str):
        return ("MALFORMED_JSON", now_ts, False)

    try:
        data = json.loads(raw_str)
    except Exception:
        return ("MALFORMED_JSON", now_ts, False)

    if not isinstance(data, dict):
        return ("MALFORMED_JSON", now_ts, False)

    # 1. Missing required fields check
    for req in IEEE_REQUIRED_FIELDS:
        if req not in data or data[req] is None:
            return ("MISSING_FIELD", now_ts, False)

    # 2. Field type checks
    tid = data["TransactionID"]
    fraud = data["isFraud"]
    dt = data["TransactionDT"]
    amt = data["TransactionAmt"]
    prod = data["ProductCD"]
    c1 = data["card1"]

    # Boolean is a subclass of int in Python, so exclude bool explicitly
    if isinstance(tid, bool) or not isinstance(tid, int):
        return ("BAD_FIELD_TYPE", now_ts, False)
    if isinstance(fraud, bool) or not isinstance(fraud, int):
        return ("BAD_FIELD_TYPE", now_ts, False)
    if isinstance(dt, bool) or not isinstance(dt, int):
        return ("BAD_FIELD_TYPE", now_ts, False)
    if isinstance(amt, bool) or not isinstance(amt, (int, float)):
        return ("BAD_FIELD_TYPE", now_ts, False)
    if not isinstance(prod, str):
        return ("BAD_FIELD_TYPE", now_ts, False)
    if isinstance(c1, bool) or not isinstance(c1, int):
        return ("BAD_FIELD_TYPE", now_ts, False)

    # 3. Negative amount check
    if amt < 0.0:
        return ("NEGATIVE_AMOUNT", now_ts, False)

    # 4. Invalid label check
    if fraud not in (0, 1):
        return ("INVALID_LABEL", now_ts, False)

    # 5. Invalid TransactionDT check
    if dt < 0 or dt > 100_000_000:
        return ("INVALID_TRANSACTIONDT", now_ts, False)

    return (None, now_ts, True)


def create_spark_session(app_name: str = "Member3-IEEE-Streaming") -> SparkSession:
    """Create optimized SparkSession for IEEE Structured Streaming."""
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
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "localhost")
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


def build_stateless_pipeline_ieee(
    spark: SparkSession,
    bootstrap_servers: str = "localhost:9092",
    input_topic: str = "financial_transactions_ieee",
    output_topic: Optional[str] = "financial_features_ieee",
    dlq_topic: Optional[str] = "financial_transactions_ieee_dlq",
    checkpoint_location: Optional[str] = None,
    max_offsets_per_trigger: Optional[int] = None,
    starting_offsets: str = "earliest",
):
    """Construct and start end-to-end Spark Structured Streaming query for stateless IEEE features."""
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

    # Parse and evaluate records
    eval_stream = (
        raw_stream.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
        )
        .withColumn("eval", classify_udf(F.col("raw_json")))
    )

    # Branch 1: DLQ for invalid / malformed records
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

    dlq_query = None
    if dlq_topic and checkpoint_location:
        dlq_chk = os.path.join(checkpoint_location, "dlq")
        dlq_query = (
            malformed_stream.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("topic", dlq_topic)
            .option("checkpointLocation", dlq_chk)
            .outputMode("append")
            .start()
        )

    # Branch 2: Valid records -> parse schema and compute stateless features
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
        query = (
            kafka_payload.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("topic", output_topic)
            .option("checkpointLocation", features_chk)
            .outputMode("append")
            .start()
        )
        return query

    return features_df


def main():
    parser = argparse.ArgumentParser(description="IEEE Structured Streaming Consumer")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092")
    parser.add_argument("--input-topic", type=str, default="financial_transactions_ieee")
    parser.add_argument("--output-topic", type=str, default="financial_features_ieee")
    parser.add_argument("--dlq-topic", type=str, default="financial_transactions_ieee_dlq")
    parser.add_argument("--checkpoint-location", type=str, default="member3_streaming/checkpoints/ieee")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=None)
    args = parser.parse_args()

    spark = create_spark_session()
    print("SparkSession initialized. Starting IEEE stateless streaming consumer...")
    query = build_stateless_pipeline_ieee(
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
