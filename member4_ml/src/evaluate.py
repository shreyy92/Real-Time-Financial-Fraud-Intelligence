"""
Authoritative Model Evaluation and Threshold Tuning for Member 4.

Computes class-imbalance metrics:
- Precision, Recall, F1-Score
- Average Precision (PR-AUC / Average Precision via scikit-learn average_precision_score)
- ROC-AUC
- Confusion Matrix (TN, FP, FN, TP)
- Configurable threshold evaluation and threshold selection (maximizing F1 or F_beta on validation)

Handles edge cases:
- Single-class ground truth
- Empty input
- Mismatched lengths
- Non-finite scores
- Invalid thresholds outside [0.0, 1.0]
"""

from typing import Dict, Any, Optional, Tuple, List, Union
import numpy as np
import pandas as pd
from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    average_precision_score,
    roc_auc_score,
    confusion_matrix,
    precision_recall_curve,
)


def validate_evaluation_inputs(
    y_true: Union[pd.Series, np.ndarray, List[int]],
    y_scores: Union[pd.Series, np.ndarray, List[float]],
) -> Tuple[np.ndarray, np.ndarray]:
    """Validate ground truth and predicted probability arrays."""
    y_true_arr = np.asarray(y_true, dtype=np.int32).ravel()
    y_scores_arr = np.asarray(y_scores, dtype=np.float64).ravel()

    if len(y_true_arr) == 0:
        raise ValueError("Input arrays cannot be empty.")

    if len(y_true_arr) != len(y_scores_arr):
        raise ValueError(
            f"Length mismatch: y_true has {len(y_true_arr)} samples, "
            f"y_scores has {len(y_scores_arr)} samples."
        )

    if not np.isfinite(y_scores_arr).all():
        raise ValueError("y_scores contains NaN, Inf, or non-finite values.")

    if np.any((y_scores_arr < 0.0) | (y_scores_arr > 1.0)):
        raise ValueError("y_scores contains probabilities outside [0.0, 1.0].")

    unique_vals = set(np.unique(y_true_arr))
    if not unique_vals.issubset({0, 1}):
        raise ValueError(f"y_true must contain only binary labels {{0, 1}}, got: {unique_vals}")

    return y_true_arr, y_scores_arr


def evaluate_predictions(
    y_true: Union[pd.Series, np.ndarray, List[int]],
    y_scores: Union[pd.Series, np.ndarray, List[float]],
    threshold: float = 0.5,
) -> Dict[str, Any]:
    """
    Compute comprehensive fraud detection evaluation metrics at a specified decision threshold.

    Parameters
    ----------
    y_true : array-like
        Binary ground-truth labels (0 or 1).
    y_scores : array-like
        Predicted probabilities of fraud (in [0.0, 1.0]).
    threshold : float
        Decision threshold for binary classification (default 0.5).

    Returns
    -------
    metrics : Dict[str, Any]
        Dictionary containing precision, recall, f1, average_precision (PR-AUC),
        roc_auc, confusion matrix breakdown, and sample counts.
    """
    if not (0.0 <= threshold <= 1.0):
        raise ValueError(f"Threshold must be in [0.0, 1.0], got {threshold}")

    y_true_arr, y_scores_arr = validate_evaluation_inputs(y_true, y_scores)
    y_pred = (y_scores_arr >= threshold).astype(np.int32)

    n_samples = len(y_true_arr)
    n_pos = int(np.sum(y_true_arr))
    n_neg = n_samples - n_pos
    pos_rate = float(n_pos / n_samples) if n_samples > 0 else 0.0

    # Check for single-class edge case
    is_single_class = (n_pos == 0) or (n_neg == 0)

    if is_single_class:
        # ROC-AUC and PR-AUC are undefined for single-class validation data
        roc_auc = float("nan")
        avg_precision = float("nan")
    else:
        roc_auc = float(roc_auc_score(y_true_arr, y_scores_arr))
        # Note: average_precision_score computes area under precision-recall curve as a weighted mean of precisions
        avg_precision = float(average_precision_score(y_true_arr, y_scores_arr))

    prec = float(precision_score(y_true_arr, y_pred, zero_division=0))
    rec = float(recall_score(y_true_arr, y_pred, zero_division=0))
    f1 = float(f1_score(y_true_arr, y_pred, zero_division=0))

    cm = confusion_matrix(y_true_arr, y_pred, labels=[0, 1])
    tn, fp, fn, tp = [int(v) for v in cm.ravel()]

    return {
        "threshold": float(threshold),
        "precision": prec,
        "recall": rec,
        "f1": f1,
        "average_precision_pr_auc": avg_precision,
        "roc_auc": roc_auc,
        "confusion_matrix": {
            "tn": tn,
            "fp": fp,
            "fn": fn,
            "tp": tp,
        },
        "total_samples": n_samples,
        "total_frauds": n_pos,
        "total_legit": n_neg,
        "fraud_prevalence": pos_rate,
        "single_class_flag": is_single_class,
    }


def find_optimal_threshold(
    y_true: Union[pd.Series, np.ndarray, List[int]],
    y_scores: Union[pd.Series, np.ndarray, List[float]],
    criterion: str = "f1",
    beta: float = 1.0,
    search_points: int = 100,
) -> Tuple[float, Dict[str, Any]]:
    """
    Find optimal decision threshold strictly on validation data.

    Parameters
    ----------
    y_true : array-like
        Validation ground-truth labels.
    y_scores : array-like
        Validation predicted probabilities.
    criterion : str
        'f1' or 'fbeta' (default 'f1').
    beta : float
        Weight of recall vs precision for fbeta (beta > 1 prioritizes recall).
    search_points : int
        Number of threshold candidates to evaluate.

    Returns
    -------
    best_threshold : float
        Optimal threshold in [0.01, 0.99].
    best_metrics : Dict[str, Any]
        Metrics computed at the best threshold.
    """
    y_true_arr, y_scores_arr = validate_evaluation_inputs(y_true, y_scores)

    if np.sum(y_true_arr) == 0:
        # No positive class present; default to 0.5
        best_threshold = 0.5
        return best_threshold, evaluate_predictions(y_true_arr, y_scores_arr, threshold=best_threshold)

    threshold_candidates = np.linspace(0.01, 0.99, search_points)
    best_score = -1.0
    best_threshold = 0.5
    best_metrics = None

    for t in threshold_candidates:
        metrics = evaluate_predictions(y_true_arr, y_scores_arr, threshold=t)
        if criterion == "f1":
            score = metrics["f1"]
        elif criterion == "fbeta":
            p = metrics["precision"]
            r = metrics["recall"]
            b2 = beta ** 2
            score = (1 + b2) * (p * r) / (b2 * p + r) if (p + r) > 0 else 0.0
        else:
            raise ValueError(f"Unknown threshold selection criterion: '{criterion}'")

        if score > best_score:
            best_score = score
            best_threshold = float(t)
            best_metrics = metrics

    return best_threshold, best_metrics
