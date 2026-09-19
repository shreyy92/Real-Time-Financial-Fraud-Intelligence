"""
Pipeline Runner CLI for Member 2 — Financial Fraud Intelligence Platform.

Executes the PaySim and IEEE-CIS PySpark feature engineering pipelines
according to feature_config.yaml settings.
"""

import os
import sys
import argparse
import yaml

# Ensure PySpark workers on Windows invoke active Python interpreter
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Add module root to sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from src.spark_session import create_spark_session
from src.paysim.build_features import build_paysim_features
from src.ieee.build_features import build_ieee_features
from src.validation.schema_check import validate_output_schema
from src.validation.feature_quality import validate_feature_quality
from src.validation.leakage_check import audit_feature_leakage, validate_no_target_leakage


def load_config(config_path: str):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def run_paysim(spark, config):
    paysim_cfg = config.get("paysim", {})
    input_path = paysim_cfg.get("input_path", "data/processed/paysim_clean.parquet")
    output_path = paysim_cfg.get("output_path", "data/features/paysim_features.parquet")
    include_tier_b = paysim_cfg.get("include_tier_b", True)
    train_max_step = paysim_cfg.get("split_cuts", {}).get("train_max_step", 500)
    valid_max_step = paysim_cfg.get("split_cuts", {}).get("valid_max_step", 600)
    num_parts = paysim_cfg.get("num_output_partitions", 8)
    feature_version = config.get("feature_version", "v1.0")

    print(f"\n[PaySim] Ingesting cleaned data from: {input_path}")
    if not os.path.exists(input_path) and not input_path.startswith("hdfs://"):
        print(f"[PaySim] WARNING: Input file '{input_path}' not found on local filesystem.")
        print("[PaySim] Place 'paysim_clean.parquet' at the configured input path to execute.")
        return None

    df_out = build_paysim_features(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        include_tier_b=include_tier_b,
        train_max_step=train_max_step,
        valid_max_step=valid_max_step,
        feature_version=feature_version,
        num_output_partitions=num_parts,
    )

    print(f"[PaySim] Successfully generated features at grain: {df_out.count()} rows.")
    print(f"[PaySim] Persisted ML-ready dataset to: {output_path}")

    # Validate output
    val_schema = validate_output_schema(df_out, "paysim")
    val_quality = validate_feature_quality(df_out, "txn_id")
    candidate_features = [c for c in df_out.columns if c not in ["isFraud", "split"]]
    val_leakage = audit_feature_leakage(candidate_features, "paysim")
    print(f"[PaySim] Schema validation: {val_schema['status']}")
    print(f"[PaySim] Quality validation: {val_quality['status']} (Duplicates: {val_quality['duplicate_ids']})")
    print(f"[PaySim] Candidate feature leakage audit: {val_leakage['status']} (Tier B / Risk count: {val_leakage['leakage_risk_count']})")
    return df_out


def run_ieee(spark, config):
    ieee_cfg = config.get("ieee", {})
    input_path = ieee_cfg.get("input_path", "data/processed/ieee_clean.parquet")
    output_path = ieee_cfg.get("output_path", "data/features/ieee_features.parquet")
    train_max_day = ieee_cfg.get("split_cuts", {}).get("train_max_day", 130.0)
    drop_near_constants = ieee_cfg.get("drop_uninformative_near_constants", True)
    num_parts = ieee_cfg.get("num_output_partitions", 4)
    feature_version = config.get("feature_version", "v1.0")

    print(f"\n[IEEE-CIS] Ingesting cleaned data from: {input_path}")
    if not os.path.exists(input_path) and not input_path.startswith("hdfs://"):
        print(f"[IEEE-CIS] WARNING: Input file '{input_path}' not found on local filesystem.")
        print("[IEEE-CIS] Place 'ieee_clean.parquet' at the configured input path to execute.")
        return None

    df_out = build_ieee_features(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        train_max_day=train_max_day,
        drop_uninformative=drop_near_constants,
        feature_version=feature_version,
        num_output_partitions=num_parts,
    )

    print(f"[IEEE-CIS] Successfully generated features at grain: {df_out.count()} rows.")
    print(f"[IEEE-CIS] Persisted ML-ready dataset to: {output_path}")

    # Validate output
    val_schema = validate_output_schema(df_out, "ieee")
    val_quality = validate_feature_quality(df_out, "TransactionID")
    candidate_features = [c for c in df_out.columns if c not in ["isFraud", "split"]]
    val_leakage = audit_feature_leakage(candidate_features, "ieee")
    print(f"[IEEE-CIS] Schema validation: {val_schema['status']}")
    print(f"[IEEE-CIS] Quality validation: {val_quality['status']} (Duplicates: {val_quality['duplicate_ids']})")
    print(f"[IEEE-CIS] Candidate feature leakage audit: {val_leakage['status']} (Risks: {val_leakage['leakage_risk_count']}, Review: {val_leakage['review_count']})")
    return df_out


def main():
    parser = argparse.ArgumentParser(description="Member 2 PySpark Feature Pipeline Runner")
    parser.add_argument(
        "--config",
        default=os.path.join(SCRIPT_DIR, "config", "feature_config.yaml"),
        help="Path to feature_config.yaml",
    )
    parser.add_argument(
        "--pipeline",
        choices=["all", "paysim", "ieee"],
        default="all",
        help="Which pipeline to execute",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    spark_cfg = config.get("spark", {})

    spark = create_spark_session(
        app_name=spark_cfg.get("app_name", "FinancialFraud-Member2"),
        master=spark_cfg.get("master", "local[*]"),
        config_overrides={
            "spark.driver.memory": spark_cfg.get("driver_memory", "4g"),
            "spark.sql.shuffle.partitions": str(spark_cfg.get("shuffle_partitions", 16)),
        },
    )

    print(f"=== Member 2 Feature Engineering Runner ===")
    print(f"Spark version: {spark.version} | App: {spark.sparkContext.appName}")

    try:
        if args.pipeline in ["all", "paysim"]:
            run_paysim(spark, config)
        if args.pipeline in ["all", "ieee"]:
            run_ieee(spark, config)
    finally:
        spark.stop()
        print("\nSpark session stopped. Member 2 execution complete.")


if __name__ == "__main__":
    main()
