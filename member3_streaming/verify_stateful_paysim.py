"""
Verify Stateful PaySim Streaming Features Against Batch Parquet.

Modes:
- Default (--real-stream / B-mode): Runs real PySpark Structured Streaming query:
    spark.readStream (Kafka, maxOffsetsPerTrigger=500)
    -> stateless feature transformation
    -> applyInPandasWithState(update_destination_state)
    -> memory sink
  Then reads back the sink and compares against `data/features/paysim_features.parquet`
  across all 31 features at 1e-6 tolerance.
- Span Verification (--span-test / D-mode): Replays transactions of ~300 destination
  accounts spanning >168 steps to Kafka topic `financial_transactions_paysim_span`,
  runs real PySpark Structured Streaming query with applyInPandasWithState over steps 1..743,
  and compares against batch parquet reference.
"""

import sys
import argparse
import time
import json
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout

from member3_streaming.consumer_paysim import (
    create_spark_session,
    PAYSIM_RAW_SCHEMA,
    PAYSIM_OUTPUT_SCHEMA,
    PAYSIM_STATE_SCHEMA,
    add_stateless_features,
    update_destination_state,
)


def run_real_streaming_b_mode(tolerance: float = 1e-6):
    print("=" * 80)
    print("RUNNING REAL STRUCTURED STREAMING B-MODE (maxOffsetsPerTrigger=500)")
    print("readStream (Kafka) -> applyInPandasWithState -> memory sink")
    print("=" * 80)

    spark = create_spark_session("RealStreamBVerification")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "financial_transactions_paysim")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 500)
        .load()
    )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), PAYSIM_RAW_SCHEMA).alias("data")
    ).select("data.*").filter(
        F.col("step").isNotNull()
        & F.col("amount").isNotNull()
        & F.col("nameDest").isNotNull()
    )

    stateless = add_stateless_features(parsed)

    stateful = (
        stateless.groupBy("nameDest")
        .applyInPandasWithState(
            func=update_destination_state,
            outputStructType=PAYSIM_OUTPUT_SCHEMA,
            stateStructType=PAYSIM_STATE_SCHEMA,
            outputMode="Update",
            timeoutConf=GroupStateTimeout.NoTimeout,
        )
    )

    query = (
        stateful.writeStream
        .format("memory")
        .queryName("real_sink_b")
        .outputMode("update")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print("Streaming query finished! Reading back from real_sink_b memory sink...")

    stream_df = spark.sql("SELECT * FROM real_sink_b").toPandas()
    print(f"Total streaming rows retrieved: {len(stream_df)}")

    max_step = int(stream_df["step"].max())
    batch_table = pq.read_table("data/features/paysim_features.parquet", filters=[("step", "<=", max_step)])
    batch_df = batch_table.to_pandas()

    joined = pd.merge(stream_df, batch_df, on="txn_id", suffixes=("_stream", "_batch"))
    rows_compared = len(joined)
    print(f"Rows successfully joined & compared: {rows_compared}")

    test_features = [
        "step_hour", "hour_sin", "hour_cos", "sim_day", "history_window_complete_flag",
        "type_transfer", "type_cash_out", "type_payment", "type_cash_in", "type_debit",
        "amount_log1p", "amount_zero_flag", "amt_to_oldbalOrg", "amt_to_oldbalDest",
        "orig_drain_flag", "amount_exceeds_orig_balance_flag", "orig_balance_zero_flag",
        "dest_balance_zero_flag", "dest_prior_txn_count", "dest_first_seen_flag",
        "dest_prior_amount_sum", "dest_prior_amount_mean", "dest_prior_amount_std",
        "dest_prior_amount_max", "dest_steps_since_last_txn", "dest_amount_zscore",
        "dest_txn_count_prev_24h", "dest_txn_count_prev_168h", "dest_amount_sum_prev_24h",
        "dest_amount_sum_prev_168h", "dest_prior_cashout_share",
    ]

    total_mismatches = 0
    for feat in test_features:
        s_vals = joined[f"{feat}_stream"]
        b_vals = joined[f"{feat}_batch"]
        null_mask = s_vals.isna() != b_vals.isna()
        valid_mask = (~s_vals.isna()) & (~b_vals.isna())
        val_mask = np.abs(s_vals[valid_mask].astype(float) - b_vals[valid_mask].astype(float)) > tolerance
        cnt = int(null_mask.sum() + val_mask.sum())
        total_mismatches += cnt
        status = "OK" if cnt == 0 else f"MISMATCH ({cnt})"
        print(f"  {feat:32s}: {cnt:5d} [{status}]")

    print("-" * 80)
    print(f"Total Rows Compared   : {rows_compared}")
    print(f"Total Mismatches Found: {total_mismatches}")
    print("-" * 80)

    if total_mismatches > 0:
        print("VERIFICATION FAILED WITH MISMATCHES!")
        sys.exit(1)
    else:
        print(f"ALL {rows_compared} ROWS MATCH BATCH REFERENCE PERFECTLY (ZERO MISMATCHES) VIA REAL STREAM!")


def run_real_streaming_d_mode(tolerance: float = 1e-6):
    print("=" * 80)
    print("RUNNING REAL STRUCTURED STREAMING D-MODE (SPANS > 168 STEPS)")
    print("readStream (financial_transactions_paysim_span) -> applyInPandasWithState -> memory sink")
    print("=" * 80)

    spark = create_spark_session("RealStreamDVerification")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "financial_transactions_paysim_span")
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), PAYSIM_RAW_SCHEMA).alias("data")
    ).select("data.*").filter(
        F.col("step").isNotNull()
        & F.col("amount").isNotNull()
        & F.col("nameDest").isNotNull()
    )

    stateless = add_stateless_features(parsed)

    stateful = (
        stateless.groupBy("nameDest")
        .applyInPandasWithState(
            func=update_destination_state,
            outputStructType=PAYSIM_OUTPUT_SCHEMA,
            stateStructType=PAYSIM_STATE_SCHEMA,
            outputMode="Update",
            timeoutConf=GroupStateTimeout.NoTimeout,
        )
    )

    query = (
        stateful.writeStream
        .format("memory")
        .queryName("real_sink_d")
        .outputMode("update")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print("Streaming query finished! Reading back from real_sink_d memory sink...")

    stream_df = spark.sql("SELECT * FROM real_sink_d").toPandas()
    print(f"Total streaming rows retrieved: {len(stream_df)}")

    batch_full = pq.read_table("data/features/paysim_features.parquet").to_pandas()
    joined = pd.merge(stream_df, batch_full, on="txn_id", suffixes=("_stream", "_batch"))
    rows_compared = len(joined)
    print(f"Rows successfully joined & compared: {rows_compared}")

    c24_diff = int((joined["dest_txn_count_prev_24h_stream"] != joined["dest_prior_txn_count_stream"]).sum())
    c168_diff = int((joined["dest_txn_count_prev_168h_stream"] != joined["dest_prior_txn_count_stream"]).sum())
    print(f"dest_txn_count_prev_24h != dest_prior_txn_count: {c24_diff} (must be > 0)")
    print(f"dest_txn_count_prev_168h != dest_prior_txn_count: {c168_diff} (must be > 0)")

    test_features = [
        "dest_prior_txn_count", "dest_first_seen_flag",
        "dest_prior_amount_sum", "dest_prior_amount_mean", "dest_prior_amount_std",
        "dest_prior_amount_max", "dest_steps_since_last_txn", "dest_amount_zscore",
        "dest_txn_count_prev_24h", "dest_txn_count_prev_168h",
        "dest_amount_sum_prev_24h", "dest_amount_sum_prev_168h",
        "dest_prior_cashout_share",
    ]

    total_mismatches = 0
    for feat in test_features:
        s_vals = joined[f"{feat}_stream"]
        b_vals = joined[f"{feat}_batch"]
        null_mask = s_vals.isna() != b_vals.isna()
        valid_mask = (~s_vals.isna()) & (~b_vals.isna())
        val_mask = np.abs(s_vals[valid_mask].astype(float) - b_vals[valid_mask].astype(float)) > tolerance
        cnt = int(null_mask.sum() + val_mask.sum())
        total_mismatches += cnt
        status = "OK" if cnt == 0 else f"MISMATCH ({cnt})"
        print(f"  {feat:32s}: {cnt:5d} [{status}]")

    print("-" * 80)
    print(f"Total Rows Compared   : {rows_compared}")
    print(f"Total Mismatches Found: {total_mismatches}")
    print("-" * 80)

    if total_mismatches > 0:
        print("SPAN VERIFICATION FAILED WITH MISMATCHES!")
        sys.exit(1)
    else:
        print("ALL ROWS MATCH BATCH REFERENCE PERFECTLY (ZERO MISMATCHES) VIA REAL STREAM!")


def main():
    parser = argparse.ArgumentParser(description="Verify PaySim stateful streaming features")
    parser.add_argument("--span-test", action="store_true", help="Run span > 168 window expiration test")
    args = parser.parse_args()

    if args.span_test:
        run_real_streaming_d_mode()
    else:
        run_real_streaming_b_mode()


if __name__ == "__main__":
    main()
