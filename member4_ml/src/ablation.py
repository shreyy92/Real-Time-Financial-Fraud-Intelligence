"""
PaySim Tier A vs Tier A+B Simulator Artifact Ablation Study for Member 4.

Purpose:
Compare model performance strictly between:
1. Tier A (Authorization-Time Features: 32 columns)
2. Tier A + Tier B (Including Post-Transaction Balances & Error Deltas: 37 columns)

Educational & Engineering Note:
Post-transaction balances (newbalanceOrig, newbalanceDest) and error deltas
(err_balance_orig = newbalanceOrig + amount - oldbalanceOrg) are simulator artifacts.
In PaySim, legitimate transactions frequently violate balance accounting (amount > balance),
whereas fraudulent transactions follow precise arithmetic.
Feeding Tier B directly to an ML classifier yields near-perfect metrics (>0.999 ROC-AUC),
which is NOT deployable to real-time pre-authorization payment gateways.
"""

from typing import Dict, Any, Optional
import os
import json
import pandas as pd

from member4_ml.src.feature_selection import load_config, select_features
from member4_ml.src.train import ModelTrainer


def run_paysim_ablation(
    df_train: pd.DataFrame,
    df_valid: pd.DataFrame,
    model_type: str = "lightgbm",
    random_state: int = 42,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Run controlled ablation experiment comparing Tier A vs Tier A+B.

    Both experiments use identical split definitions, hyperparameters,
    random seeds, and threshold-tuning criteria.

    Parameters
    ----------
    df_train : pd.DataFrame
        Training data containing both Tier A and Tier B columns.
    df_valid : pd.DataFrame
        Validation data containing both Tier A and Tier B columns.
    model_type : str
        'baseline' or 'lightgbm'.
    random_state : int
        Seed for reproducibility.
    config : Optional[Dict[str, Any]]
        Optional config dict.

    Returns
    -------
    ablation_results : Dict[str, Any]
        Side-by-side comparison of Tier A vs Tier A+B metrics and artifact explanation.
    """
    config = config or load_config()

    # 1. Experiment 1: Tier A Only (Safe Authorization-Time Features)
    X_train_a, y_train_a = select_features(df_train, dataset="paysim", include_tier_b=False, config=config)
    X_valid_a, y_valid_a = select_features(df_valid, dataset="paysim", include_tier_b=False, config=config)

    trainer_a = ModelTrainer(dataset="paysim", model_type=model_type, config=config)
    res_a = trainer_a.fit(
        X_train_a,
        y_train_a,
        X_valid_a,
        y_valid_a,
        random_state=random_state,
        include_tier_b=False,
    )

    # 2. Experiment 2: Tier A + Tier B (Simulator Balance Artifacts)
    X_train_ab, y_train_ab = select_features(df_train, dataset="paysim", include_tier_b=True, config=config)
    X_valid_ab, y_valid_ab = select_features(df_valid, dataset="paysim", include_tier_b=True, config=config)

    trainer_ab = ModelTrainer(dataset="paysim", model_type=model_type, config=config)
    res_ab = trainer_ab.fit(
        X_train_ab,
        y_train_ab,
        X_valid_ab,
        y_valid_ab,
        random_state=random_state,
        include_tier_b=True,
    )

    explanation = (
        "Tier B includes post-transaction balance features (newbalanceOrig, newbalanceDest) and "
        "accounting error deltas (err_balance_orig, err_balance_dest). In the PaySim simulator, "
        "fraudulent transactions follow exact accounting formulas while 90.1% of legitimate transactions "
        "exhibit accounting violations. The metric inflation observed in Tier A+B represents simulator "
        "arithmetic artifacts rather than deployable authorization-time predictive signal. Real-time "
        "production models must strictly deploy Tier A only."
    )

    return {
        "dataset": "paysim",
        "model_type": model_type,
        "random_state": random_state,
        "tier_a": {
            "num_features": X_train_a.shape[1],
            "optimal_threshold": res_a["optimal_threshold"],
            "train_metrics": res_a["train_metrics"],
            "valid_metrics": res_a["valid_metrics"],
        },
        "tier_ab": {
            "num_features": X_train_ab.shape[1],
            "optimal_threshold": res_ab["optimal_threshold"],
            "train_metrics": res_ab["train_metrics"],
            "valid_metrics": res_ab["valid_metrics"],
        },
        "artifact_warning_and_explanation": explanation,
    }
