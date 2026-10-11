"""
Real-Time PaySim Spark Structured Streaming Consumer with Stateful Destination Tracking.

Reads from Kafka topic `financial_transactions_paysim`, validates incoming records,
routes malformed records to DLQ `financial_transactions_paysim_dlq`, computes stateless
Tier A features, and statefully computes historical destination features using
`applyInPandasWithState` with bounded sliding windows and state timeout eviction.
"""

import os
import sys
import math
import json
import argparse
import datetime
from typing import Iterator, Tuple, List, Dict, Any, Optional

import pandas as pd
import numpy as np

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    ByteType,
    BooleanType,
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

# Exact PaySim input schema (raw transaction grain)
PAYSIM_RAW_SCHEMA = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
])

# Complete PaySim Tier A Output Schema verbatim from Member 2's OUTPUT_CONTRACT.md
# Note: newbalanceOrig and newbalanceDest are removed (Tier B leakage).
# isFraud is retained exclusively as the target label column.
PAYSIM_OUTPUT_SCHEMA = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("step_hour", IntegerType(), True),
    StructField("txn_id", StringType(), True),
    StructField("hour_sin", DoubleType(), True),
    StructField("hour_cos", DoubleType(), True),
    StructField("sim_day", IntegerType(), True),
    StructField("history_window_complete_flag", ByteType(), True),
    StructField("type_transfer", ByteType(), True),
    StructField("type_cash_out", ByteType(), True),
    StructField("type_payment", ByteType(), True),
    StructField("type_cash_in", ByteType(), True),
    StructField("type_debit", ByteType(), True),
    StructField("amount_log1p", DoubleType(), True),
    StructField("amount_zero_flag", ByteType(), True),
    StructField("amt_to_oldbalOrg", DoubleType(), True),
    StructField("amt_to_oldbalDest", DoubleType(), True),
    StructField("orig_drain_flag", ByteType(), True),
    StructField("amount_exceeds_orig_balance_flag", ByteType(), True),
    StructField("orig_balance_zero_flag", ByteType(), True),
    StructField("dest_balance_zero_flag", ByteType(), True),
    StructField("dest_prior_txn_count", IntegerType(), True),
    StructField("dest_first_seen_flag", ByteType(), True),
    StructField("dest_prior_amount_sum", DoubleType(), True),
    StructField("dest_prior_amount_mean", DoubleType(), True),
    StructField("dest_prior_amount_std", DoubleType(), True),
    StructField("dest_prior_amount_max", DoubleType(), True),
    StructField("dest_steps_since_last_txn", IntegerType(), True),
    StructField("dest_amount_zscore", DoubleType(), True),
    StructField("dest_txn_count_prev_24h", IntegerType(), True),
    StructField("dest_txn_count_prev_168h", IntegerType(), True),
    StructField("dest_amount_sum_prev_24h", DoubleType(), True),
    StructField("dest_amount_sum_prev_168h", DoubleType(), True),
    StructField("dest_prior_cashout_share", DoubleType(), True),
    StructField("feature_version", StringType(), True),
])

# State schema stored per destination account
# Running aggregates for committed prior steps, pending buffer for current step,
# plus bounded sliding window history JSON string
PAYSIM_STATE_SCHEMA = StructType([
    StructField("count", IntegerType(), True),
    StructField("amount_sum", DoubleType(), True),
    StructField("amount_sq_sum", DoubleType(), True),
    StructField("amount_max", DoubleType(), True),
    StructField("last_step", IntegerType(), True),
    StructField("cashout_count", IntegerType(), True),
    StructField("history_json", StringType(), True),
    StructField("pending_step", IntegerType(), True),
    StructField("pending_count", IntegerType(), True),
    StructField("pending_amount_sum", DoubleType(), True),
    StructField("pending_amount_sq_sum", DoubleType(), True),
    StructField("pending_amount_max", DoubleType(), True),
    StructField("pending_cashout_count", IntegerType(), True),
])

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


def classify_dlq_record(raw_val: Optional[str]) -> Tuple[Optional[str], str, bool]:
    """
    Validate raw JSON transaction string across the 6 failure categories.
    Returns (reason_code, ingested_at, is_valid).
    """
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    if raw_val is None:
        return ("MALFORMED_JSON", now, False)

    try:
        obj = json.loads(raw_val)
        if not isinstance(obj, dict):
            return ("MALFORMED_JSON", now, False)
    except Exception:
        return ("MALFORMED_JSON", now, False)

    # 1. Missing required field
    required_keys = ["step", "type", "amount", "nameOrig", "nameDest", "isFraud"]
    for k in required_keys:
        if k not in obj or obj[k] is None:
            return ("MISSING_FIELD", now, False)

    # 2. Bad data type
    if not isinstance(obj["step"], int) or isinstance(obj["step"], bool):
        return ("BAD_TYPE", now, False)
    if not isinstance(obj["amount"], (int, float)) or isinstance(obj["amount"], bool):
        return ("BAD_TYPE", now, False)
    if not isinstance(obj["type"], str) or not isinstance(obj["nameOrig"], str) or not isinstance(obj["nameDest"], str):
        return ("BAD_TYPE", now, False)
    if not isinstance(obj["isFraud"], int) or isinstance(obj["isFraud"], bool):
        return ("BAD_TYPE", now, False)

    # 3. Negative transaction amount
    if float(obj["amount"]) < 0.0:
        return ("NEGATIVE_AMOUNT", now, False)

    # 4. Out-of-bounds simulation step (step == 9999 or not in 1..743)
    step_num = int(obj["step"])
    if step_num == 9999 or step_num < 1 or step_num > 743:
        return ("INVALID_STEP", now, False)

    # 5. Invalid target label (isFraud not in {0, 1})
    if int(obj["isFraud"]) not in (0, 1):
        return ("INVALID_LABEL", now, False)

    return (None, now, True)



def add_stateless_features(df: DataFrame) -> DataFrame:
    """Compute all stateless Tier A PaySim features verbatim from FEATURE_DICTIONARY.md."""
    df = (
        df.withColumn("step_hour", (F.col("step") % 24).cast("integer"))
        .withColumn(
            "txn_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("step"),
                    F.col("type"),
                    F.col("amount"),
                    F.col("nameOrig"),
                    F.col("nameDest"),
                ),
                256,
            ),
        )
        .withColumn(
            "hour_sin",
            F.sin(2.0 * math.pi * F.col("step_hour") / 24.0).cast("double"),
        )
        .withColumn(
            "hour_cos",
            F.cos(2.0 * math.pi * F.col("step_hour") / 24.0).cast("double"),
        )
        .withColumn(
            "sim_day",
            F.floor((F.col("step") - 1) / 24).cast("integer"),
        )
        .withColumn(
            "history_window_complete_flag",
            F.when(F.col("step") >= 169, 1).otherwise(0).cast("tinyint"),
        )
        .withColumn("type_transfer", F.when(F.col("type") == "TRANSFER", 1).otherwise(0).cast("tinyint"))
        .withColumn("type_cash_out", F.when(F.col("type") == "CASH_OUT", 1).otherwise(0).cast("tinyint"))
        .withColumn("type_payment", F.when(F.col("type") == "PAYMENT", 1).otherwise(0).cast("tinyint"))
        .withColumn("type_cash_in", F.when(F.col("type") == "CASH_IN", 1).otherwise(0).cast("tinyint"))
        .withColumn("type_debit", F.when(F.col("type") == "DEBIT", 1).otherwise(0).cast("tinyint"))
        .withColumn(
            "amount_log1p",
            F.log1p(F.greatest(F.lit(0.0), F.col("amount"))).cast("double"),
        )
        .withColumn(
            "amount_zero_flag",
            F.when(F.col("amount") == 0.0, 1).otherwise(0).cast("tinyint"),
        )
        .withColumn(
            "amt_to_oldbalOrg",
            (F.col("amount") / (F.col("oldbalanceOrg") + 1.0)).cast("double"),
        )
        .withColumn(
            "amt_to_oldbalDest",
            (F.col("amount") / (F.col("oldbalanceDest") + 1.0)).cast("double"),
        )
        .withColumn(
            "orig_drain_flag",
            F.when(
                (F.col("oldbalanceOrg") > 0.0)
                & (F.abs(F.col("amount") - F.col("oldbalanceOrg")) < 0.005),
                1,
            )
            .otherwise(0)
            .cast("tinyint"),
        )
        .withColumn(
            "amount_exceeds_orig_balance_flag",
            F.when(F.col("amount") > F.col("oldbalanceOrg"), 1).otherwise(0).cast("tinyint"),
        )
        .withColumn(
            "orig_balance_zero_flag",
            F.when(F.col("oldbalanceOrg") == 0.0, 1).otherwise(0).cast("tinyint"),
        )
        .withColumn(
            "dest_balance_zero_flag",
            F.when(
                (F.col("oldbalanceDest") == 0.0) & (F.col("type") != "PAYMENT"),
                1,
            )
            .otherwise(0)
            .cast("tinyint"),
        )
    )
    return df


def update_destination_state(
    key: Tuple[str],
    pdf_iter: Iterator[pd.DataFrame],
    state: GroupState,
) -> Iterator[pd.DataFrame]:
    """
    Process streaming batches per `nameDest` with state maintenance.

    Window Semantics (Replicating Member 2's Exact Window Rules):
    - Strictly prior transactions: step < current_step (rangeBetween Window.unboundedPreceding to -1).
    - Same-step ties: Transactions in the same step are excluded from each other's prior history
      and share the exact same prior state. They are incorporated for future steps once a strictly higher
      step arrives, even when a step is split across micro-batch boundaries.
    - Standard Deviation: Sample standard deviation with n - 1 denominator when count >= 2.
    - Z-Score: Evaluated only when prior count >= 3 and prior std > 1e-5; null otherwise.
    - Rolling Windows (24h, 168h): Evaluated on `[step - 24, step - 1]` and `[step - 168, step - 1]`.
    - Buffer pruning: Rolling buffer keeps entries within `latest_step - 168`.
    - Late-Event Guard: If an incoming row's step is lower than the key's committed latest step,
      it is not applied to state and is routed directly to the DLQ with reason_code LATE_EVENT.
    """
    if state.hasTimedOut:
        state.remove()
        return

    # Restore state or initialize clean accumulator
    if state.exists:
        st_vals = state.get
        s_count = st_vals[0]
        s_amount_sum = st_vals[1]
        s_amount_sq_sum = st_vals[2]
        s_amount_max = st_vals[3]
        s_last_step = st_vals[4]
        s_cashout_count = st_vals[5]
        s_history_json = st_vals[6]
        history = json.loads(s_history_json) if s_history_json else []

        pending_step = st_vals[7]
        pending_cnt = st_vals[8]
        pending_sum = st_vals[9]
        pending_sq = st_vals[10]
        pending_max = st_vals[11]
        pending_cashout = st_vals[12]
    else:
        s_count = 0
        s_amount_sum = 0.0
        s_amount_sq_sum = 0.0
        s_amount_max = None
        s_last_step = None
        s_cashout_count = 0
        history = []

        pending_step = None
        pending_cnt = 0
        pending_sum = 0.0
        pending_sq = 0.0
        pending_max = None
        pending_cashout = 0

    out_dfs: List[pd.DataFrame] = []
    dlq_producer = None

    for pdf in pdf_iter:
        if pdf.empty:
            continue

        # Check for late events (incoming row step < committed s_last_step)
        if s_last_step is not None:
            late_mask = pdf["step"] < s_last_step
            if late_mask.any():
                late_rows = pdf[late_mask]
                try:
                    from kafka import KafkaProducer
                    if dlq_producer is None:
                        dlq_producer = KafkaProducer(
                            bootstrap_servers=["localhost:9092"],
                            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                        )
                    for r in late_rows.to_dict(orient="records"):
                        dlq_record = {
                            "raw_value": json.dumps(r),
                            "reason_code": "LATE_EVENT",
                            "ingested_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        }
                        dlq_producer.send("financial_transactions_paysim_dlq", value=dlq_record)
                    dlq_producer.flush()
                except Exception as ex:
                    print(f"Warning: Failed to route late event to DLQ: {ex}")

                pdf = pdf[~late_mask]
                if pdf.empty:
                    continue

        # Sort incoming mini-batch chunk by step to ensure deterministic chronological replay
        pdf = pdf.sort_values(by="step", ascending=True, kind="stable")

        # Group rows by step to properly handle same-step ties
        step_groups = pdf.groupby("step", sort=False)
        processed_chunks: List[pd.DataFrame] = []

        for step_val, group in step_groups:
            rows = group.copy()
            n_rows = len(rows)

            # If we transition to a strictly higher step, commit any pending aggregates
            if pending_step is not None and step_val > pending_step:
                s_count += pending_cnt
                s_amount_sum += pending_sum
                s_amount_sq_sum += pending_sq
                s_amount_max = pending_max if s_amount_max is None else max(s_amount_max, pending_max)
                s_last_step = int(pending_step)
                s_cashout_count += pending_cashout

                # Reset pending accumulator
                pending_step = None
                pending_cnt = 0
                pending_sum = 0.0
                pending_sq = 0.0
                pending_max = None
                pending_cashout = 0

            # Prior state seen by all transactions in this step (strictly step < current_step)
            prior_count = s_count
            prior_sum = s_amount_sum
            prior_mean = (prior_sum / prior_count) if prior_count > 0 else None

            if prior_count >= 2:
                var = (s_amount_sq_sum - (prior_sum ** 2) / prior_count) / (prior_count - 1)
                prior_std = math.sqrt(max(0.0, var))
            else:
                prior_std = None

            prior_max = s_amount_max
            prior_last_step = s_last_step
            prior_cashout_share = (s_cashout_count / prior_count) if prior_count > 0 else None

            # Vectorized / list evaluation for rows in this step
            first_seen = 1 if prior_count == 0 else 0
            steps_since = int(step_val - prior_last_step) if prior_count > 0 else None

            # 24h and 168h window filtering from history buffer (strictly prior steps: <= step_val - 1)
            cnt_24 = sum(1 for (s, a) in history if step_val - 24 <= s <= step_val - 1)
            sum_24 = float(sum(a for (s, a) in history if step_val - 24 <= s <= step_val - 1))
            cnt_168 = sum(1 for (s, a) in history if step_val - 168 <= s <= step_val - 1)
            sum_168 = float(sum(a for (s, a) in history if step_val - 168 <= s <= step_val - 1))

            zscores = []
            for amt in rows["amount"]:
                if prior_count >= 3 and prior_std is not None and prior_std > 1e-5:
                    zscores.append((amt - prior_mean) / prior_std)
                else:
                    zscores.append(None)

            rows["dest_prior_txn_count"] = pd.Series([prior_count] * n_rows, index=rows.index, dtype="int32")
            rows["dest_first_seen_flag"] = pd.Series([first_seen] * n_rows, index=rows.index, dtype="int8")
            rows["dest_prior_amount_sum"] = pd.Series([prior_sum] * n_rows, index=rows.index, dtype="float64")
            rows["dest_prior_amount_mean"] = pd.Series([prior_mean] * n_rows, index=rows.index, dtype="float64")
            rows["dest_prior_amount_std"] = pd.Series([prior_std] * n_rows, index=rows.index, dtype="float64")
            rows["dest_prior_amount_max"] = pd.Series([prior_max] * n_rows, index=rows.index, dtype="float64")
            rows["dest_steps_since_last_txn"] = pd.Series([steps_since] * n_rows, index=rows.index, dtype="Int32")
            rows["dest_amount_zscore"] = pd.Series(zscores, index=rows.index, dtype="float64")
            rows["dest_txn_count_prev_24h"] = pd.Series([cnt_24] * n_rows, index=rows.index, dtype="int32")
            rows["dest_txn_count_prev_168h"] = pd.Series([cnt_168] * n_rows, index=rows.index, dtype="int32")
            rows["dest_amount_sum_prev_24h"] = pd.Series([sum_24] * n_rows, index=rows.index, dtype="float64")
            rows["dest_amount_sum_prev_168h"] = pd.Series([sum_168] * n_rows, index=rows.index, dtype="float64")
            rows["dest_prior_cashout_share"] = pd.Series([prior_cashout_share] * n_rows, index=rows.index, dtype="float64")

            processed_chunks.append(rows)

            # Accumulate this step into pending buffer
            pending_step = int(step_val)
            for r in rows.itertuples():
                pending_cnt += 1
                pending_sum += float(r.amount)
                pending_sq += float(r.amount * r.amount)
                pending_max = float(r.amount) if pending_max is None else max(pending_max, float(r.amount))
                if r.type == "CASH_OUT":
                    pending_cashout += 1
                history.append((int(step_val), float(r.amount)))

            # Prune sliding window buffer beyond 168 steps
            history = [(s, a) for (s, a) in history if s >= int(step_val) - 168]

        if processed_chunks:
            out_pdf = pd.concat(processed_chunks, ignore_index=True)
            out_pdf["feature_version"] = "v1.0"

            # Ensure exact column order matching Tier A contract
            cols = [f.name for f in PAYSIM_OUTPUT_SCHEMA.fields]
            out_dfs.append(out_pdf[cols])

    # Persist updated state with indefinite retention (NoTimeout)
    state.update((
        int(s_count),
        float(s_amount_sum),
        float(s_amount_sq_sum),
        float(s_amount_max) if s_amount_max is not None else None,
        int(s_last_step) if s_last_step is not None else None,
        int(s_cashout_count),
        json.dumps(history),
        int(pending_step) if pending_step is not None else None,
        int(pending_cnt),
        float(pending_sum),
        float(pending_sq),
        float(pending_max) if pending_max is not None else None,
        int(pending_cashout),
    ))

    if dlq_producer is not None:
        try:
            dlq_producer.close()
        except Exception:
            pass

    for df in out_dfs:
        yield df


def create_spark_session(app_name: str = "PaySimStreamingConsumer") -> SparkSession:
    """Initialize SparkSession configured for local streaming execution."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "localhost")
        .getOrCreate()
    )

    jvm = spark.sparkContext._gateway.jvm
    if os.path.exists(r"C:\hadoop\bin\hadoop.dll"):
        jvm.java.lang.System.load(r"C:\hadoop\bin\hadoop.dll")

    return spark


def build_streaming_pipeline(
    spark: SparkSession,
    bootstrap_servers: str = "localhost:9092",
    input_topic: str = "financial_transactions_paysim",
    output_topic: Optional[str] = None,
    dlq_topic: str = "financial_transactions_paysim_dlq",
    checkpoint_location: Optional[str] = None,
    max_offsets_per_trigger: Optional[int] = None,
) -> Any:
    """Build the end-to-end streaming query with DLQ routing, stateless & stateful features."""
    stream_reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", "earliest")
    )
    if max_offsets_per_trigger is not None:
        stream_reader = stream_reader.option("maxOffsetsPerTrigger", max_offsets_per_trigger)

    raw_stream = stream_reader.load()

    # Define classification UDF for DLQ routing
    classify_udf = F.udf(classify_dlq_record, DLQ_EVAL_SCHEMA)

    eval_stream = raw_stream.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        classify_udf(F.col("value").cast("string")).alias("eval"),
    )

    # Branch 1: Invalid / malformed records -> DLQ topic
    malformed_stream = eval_stream.filter(F.col("eval.is_valid") == False).select(
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

    # Branch 2: Valid records -> Parse JSON and compute Tier A features
    valid_stream = (
        eval_stream.filter(F.col("eval.is_valid") == True)
        .select(F.from_json(F.col("raw_json"), PAYSIM_RAW_SCHEMA).alias("data"))
        .select("data.*")
    )

    # If DLQ topic is enabled and checkpoint location provided, launch DLQ stream
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

    # 1. Stateless feature engineering
    stateless_df = add_stateless_features(valid_stream)

    # 2. Stateful feature engineering with applyInPandasWithState
    stateful_df = (
        stateless_df.groupBy("nameDest")
        .applyInPandasWithState(
            func=update_destination_state,
            outputStructType=PAYSIM_OUTPUT_SCHEMA,
            stateStructType=PAYSIM_STATE_SCHEMA,
            outputMode="Append",
            timeoutConf=GroupStateTimeout.NoTimeout,
        )
    )

    if output_topic and checkpoint_location:
        # Write to Kafka output topic with explicit nulls (ignoreNullFields=false)
        kafka_payload = stateful_df.select(
            F.col("txn_id").alias("key"),
            F.to_json(
                F.struct([F.col(c) for c in stateful_df.columns]),
                {"ignoreNullFields": "false"},
            ).alias("value"),
        )
        main_chk = os.path.join(checkpoint_location, "features")
        query = (
            kafka_payload.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("topic", output_topic)
            .option("checkpointLocation", main_chk)
            .outputMode("append")
            .start()
        )
        return query

    return stateful_df


def main():
    parser = argparse.ArgumentParser(description="PaySim Structured Streaming Consumer")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092")
    parser.add_argument("--input-topic", type=str, default="financial_transactions_paysim")
    parser.add_argument("--output-topic", type=str, default="financial_features_paysim")
    parser.add_argument("--dlq-topic", type=str, default="financial_transactions_paysim_dlq")
    parser.add_argument("--checkpoint-location", type=str, default="member3_streaming/checkpoints/paysim")
    args = parser.parse_args()

    spark = create_spark_session()
    print("SparkSession initialized. Starting PaySim streaming consumer...")
    query = build_streaming_pipeline(
        spark=spark,
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        dlq_topic=args.dlq_topic,
        checkpoint_location=args.checkpoint_location,
    )
    print(f"Streaming query started to topic: {args.output_topic}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
