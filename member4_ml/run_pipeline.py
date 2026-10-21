"""
CLI Runner for Member 4 Machine Learning Pipeline.

Commands:
1. train: Train a baseline or LightGBM model on precomputed train/valid splits.
2. evaluate: Evaluate a saved model artifact on validation or test data.
3. consume: Run the real-time Kafka scoring consumer daemon.
4. ablate: Run PaySim Tier A vs Tier A+B ablation experiment.

Examples:
  python member4_ml/run_pipeline.py --mode train --dataset paysim --model lightgbm
  python member4_ml/run_pipeline.py --mode evaluate --dataset paysim --model-artifact member4_ml/models/paysim_lightgbm.joblib --split test
  python member4_ml/run_pipeline.py --mode ablate --dataset paysim
  python member4_ml/run_pipeline.py --mode consume --dataset paysim --model-artifact member4_ml/models/paysim_lightgbm.joblib --bootstrap-servers localhost:9092
"""

import argparse
import sys
import os
import json
import logging

# Ensure project root is in sys.path when invoked directly as a script
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from member4_ml.src.data_loader import load_dataset_split
from member4_ml.src.train import ModelTrainer
from member4_ml.src.evaluate import evaluate_predictions
from member4_ml.src.ablation import run_paysim_ablation
from member4_ml.src.scoring_consumer import KafkaScoringConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("Member4CLI")


def handle_train(args):
    """Execute model training on precomputed train and valid splits."""
    logger.info(f"Starting training: Dataset={args.dataset.upper()}, Model={args.model}")

    # Load precomputed train and valid partitions
    logger.info(f"Loading '{args.dataset}' 'train' split from {args.data_path or 'configured path'}...")
    X_train, y_train = load_dataset_split(
        dataset=args.dataset,
        split="train",
        data_path=args.data_path,
        include_tier_b=False,
    )

    logger.info(f"Loading '{args.dataset}' 'valid' split...")
    X_valid, y_valid = load_dataset_split(
        dataset=args.dataset,
        split="valid",
        data_path=args.data_path,
        include_tier_b=False,
    )

    trainer = ModelTrainer(dataset=args.dataset, model_type=args.model)
    res = trainer.fit(
        X_train,
        y_train,
        X_valid,
        y_valid,
        random_state=args.random_state,
        optimize_threshold=True,
    )

    # Save fitted artifact
    saved_path = trainer.save_artifact(model_dir=args.model_dir)
    logger.info(f"Model artifact successfully saved at: {saved_path}")
    logger.info(f"Optimal Threshold (tuned on validation): {res['optimal_threshold']:.4f}")
    logger.info(f"Validation F1: {res['valid_metrics']['f1']:.4f}")
    logger.info(f"Validation PR-AUC (Average Precision): {res['valid_metrics']['average_precision_pr_auc']:.4f}")
    logger.info(f"Validation ROC-AUC: {res['valid_metrics']['roc_auc']:.4f}")


def handle_evaluate(args):
    """Evaluate a saved model artifact on a specified split."""
    if not args.model_artifact or not os.path.exists(args.model_artifact):
        logger.error(f"Please provide an existing model artifact via --model-artifact.")
        sys.exit(1)

    logger.info(f"Loading model artifact from: {args.model_artifact}")
    trainer = ModelTrainer.load_artifact(args.model_artifact)

    logger.info(f"Loading '{args.dataset}' '{args.split}' split...")
    X_eval, y_eval = load_dataset_split(
        dataset=args.dataset,
        split=args.split,
        data_path=args.data_path,
        include_tier_b=trainer.metadata.get("include_tier_b", False),
    )

    probs = trainer.predict_proba(X_eval)
    threshold = args.threshold if args.threshold is not None else trainer.optimal_threshold
    metrics = evaluate_predictions(y_eval, probs, threshold=threshold)

    print("\n" + "=" * 50)
    print(f"EVALUATION RESULTS ({args.dataset.upper()} - {args.split.upper()} SPLIT)")
    print("=" * 50)
    print(f"Model Type:               {trainer.model_type}")
    print(f"Decision Threshold:       {metrics['threshold']:.4f}")
    print(f"Precision:                {metrics['precision']:.4f}")
    print(f"Recall:                   {metrics['recall']:.4f}")
    print(f"F1-Score:                 {metrics['f1']:.4f}")
    print(f"Average Precision (PR):   {metrics['average_precision_pr_auc']:.4f}")
    print(f"ROC-AUC:                  {metrics['roc_auc']:.4f}")
    print(f"Confusion Matrix:         TN={metrics['confusion_matrix']['tn']}, FP={metrics['confusion_matrix']['fp']}, FN={metrics['confusion_matrix']['fn']}, TP={metrics['confusion_matrix']['tp']}")
    print(f"Total Fraud / Legit:      {metrics['total_frauds']} / {metrics['total_legit']} ({metrics['fraud_prevalence']:.4%})")
    print("=" * 50 + "\n")


def handle_ablate(args):
    """Run PaySim Tier A vs Tier A+B simulator artifact ablation experiment."""
    if args.dataset != "paysim":
        logger.error("Ablation study is defined specifically for PaySim simulator balance artifacts.")
        sys.exit(1)

    logger.info("Loading PaySim 'train' and 'valid' splits for ablation...")
    import pandas as pd
    from member4_ml.src.feature_selection import load_config
    cfg = load_config()
    batch_path = args.data_path or cfg["paysim"]["batch_path"]

    if not os.path.exists(batch_path):
        logger.error(f"Parquet dataset not found at '{batch_path}'. Member 2 must provide this file.")
        sys.exit(1)

    df = pd.read_parquet(batch_path, engine="pyarrow")
    df_train = df[df["split"] == "train"]
    df_valid = df[df["split"] == "valid"]

    ablation = run_paysim_ablation(df_train, df_valid, model_type=args.model, random_state=args.random_state)
    print("\n" + "=" * 60)
    print("PAYSIM SIMULATOR ARTIFACT ABLATION REPORT")
    print("=" * 60)
    print(f"Tier A (Authorization-Time: 32 Features):")
    print(f"  Valid F1:      {ablation['tier_a']['valid_metrics']['f1']:.4f}")
    print(f"  Valid PR-AUC:  {ablation['tier_a']['valid_metrics']['average_precision_pr_auc']:.4f}")
    print(f"  Valid ROC-AUC: {ablation['tier_a']['valid_metrics']['roc_auc']:.4f}")
    print("-" * 60)
    print(f"Tier A + B (Simulator Balance Deltas: 37 Features):")
    print(f"  Valid F1:      {ablation['tier_ab']['valid_metrics']['f1']:.4f}")
    print(f"  Valid PR-AUC:  {ablation['tier_ab']['valid_metrics']['average_precision_pr_auc']:.4f}")
    print(f"  Valid ROC-AUC: {ablation['tier_ab']['valid_metrics']['roc_auc']:.4f}")
    print("-" * 60)
    print("EXPLANATION & WARNING:")
    print(ablation["artifact_warning_and_explanation"])
    print("=" * 60 + "\n")


def handle_consume(args):
    """Run real-time Kafka scoring consumer."""
    if not args.model_artifact:
        logger.error("Please specify an existing model artifact via --model-artifact.")
        sys.exit(1)

    consumer = KafkaScoringConsumer(
        dataset=args.dataset,
        model_artifact_path=args.model_artifact,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        cache_capacity=args.cache_capacity,
    )
    logger.info("Starting scoring consumer poll loop. Press Ctrl+C to stop.")
    consumer.start(max_messages=args.max_messages)


def main():
    parser = argparse.ArgumentParser(description="Member 4 ML Pipeline CLI")
    parser.add_argument(
        "--mode",
        choices=["train", "evaluate", "ablate", "consume"],
        required=True,
        help="Action mode to execute.",
    )
    parser.add_argument(
        "--dataset",
        choices=["paysim", "ieee"],
        required=True,
        help="Target dataset ('paysim' or 'ieee').",
    )
    parser.add_argument(
        "--model",
        choices=["baseline", "lightgbm"],
        default="lightgbm",
        help="Model architecture family (default: 'lightgbm').",
    )
    parser.add_argument(
        "--data-path",
        default=None,
        help="Custom path to batch feature Parquet dataset.",
    )
    parser.add_argument(
        "--split",
        default="valid",
        help="Split to evaluate ('valid' or 'test').",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=None,
        help="Custom decision threshold (defaults to optimal threshold from training).",
    )
    parser.add_argument(
        "--model-dir",
        default=None,
        help="Directory to save model artifacts.",
    )
    parser.add_argument(
        "--model-artifact",
        default=None,
        help="Path to fitted .joblib model artifact.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka broker bootstrap servers.",
    )
    parser.add_argument(
        "--input-topic",
        default=None,
        help="Override inbound Kafka topic.",
    )
    parser.add_argument(
        "--output-topic",
        default=None,
        help="Override outbound scoring Kafka topic.",
    )
    parser.add_argument(
        "--group-id",
        default=None,
        help="Kafka consumer group ID.",
    )
    parser.add_argument(
        "--cache-capacity",
        type=int,
        default=100000,
        help="Deduplication cache size in number of keys (default: 100,000).",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Maximum messages to consume before stopping (useful for testing).",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Seed for reproducible training (default: 42).",
    )

    args = parser.parse_args()

    if args.mode == "train":
        handle_train(args)
    elif args.mode == "evaluate":
        handle_evaluate(args)
    elif args.mode == "ablate":
        handle_ablate(args)
    elif args.mode == "consume":
        handle_consume(args)


if __name__ == "__main__":
    main()
