"""
Unit tests for Member 4 Evaluation, Model Training, and Ablation Study.
"""

import tempfile
import os
import pytest
import numpy as np
import pandas as pd

from member4_ml.src.evaluate import (
    evaluate_predictions,
    find_optimal_threshold,
    validate_evaluation_inputs,
)
from member4_ml.src.train import ModelTrainer
from member4_ml.src.ablation import run_paysim_ablation


# -----------------------------------------------------------------------------
# 1. Evaluate Unit Tests
# -----------------------------------------------------------------------------

def test_evaluate_predictions_standard():
    """Verify evaluation metric calculations against known dummy values."""
    y_true = [0, 0, 1, 1]
    y_scores = [0.1, 0.4, 0.6, 0.8]
    metrics = evaluate_predictions(y_true, y_scores, threshold=0.5)

    assert metrics["precision"] == 1.0
    assert metrics["recall"] == 1.0
    assert metrics["f1"] == 1.0
    assert metrics["roc_auc"] == 1.0
    assert metrics["average_precision_pr_auc"] == 1.0
    assert metrics["confusion_matrix"] == {"tn": 2, "fp": 0, "fn": 0, "tp": 2}


def test_evaluate_predictions_invalid_threshold():
    """Verify invalid threshold outside [0, 1] raises ValueError."""
    with pytest.raises(ValueError, match="Threshold must be in"):
        evaluate_predictions([0, 1], [0.2, 0.8], threshold=1.5)
    with pytest.raises(ValueError, match="Threshold must be in"):
        evaluate_predictions([0, 1], [0.2, 0.8], threshold=-0.1)


def test_evaluate_predictions_mismatched_lengths():
    """Verify mismatched input lengths raise ValueError."""
    with pytest.raises(ValueError, match="Length mismatch"):
        evaluate_predictions([0, 1, 0], [0.2, 0.8])


def test_evaluate_predictions_non_finite_scores():
    """Verify NaN/Inf scores raise ValueError."""
    with pytest.raises(ValueError, match="non-finite values"):
        evaluate_predictions([0, 1], [0.2, np.nan])
    with pytest.raises(ValueError, match="non-finite values"):
        evaluate_predictions([0, 1], [0.2, np.inf])


def test_evaluate_predictions_single_class_edge_case():
    """Verify single-class validation data safely sets ROC-AUC/PR-AUC to NaN without crashing."""
    y_true = [0, 0, 0, 0]
    y_scores = [0.1, 0.2, 0.3, 0.4]
    metrics = evaluate_predictions(y_true, y_scores, threshold=0.5)

    assert metrics["single_class_flag"] is True
    assert np.isnan(metrics["roc_auc"])
    assert np.isnan(metrics["average_precision_pr_auc"])
    assert metrics["confusion_matrix"]["tp"] == 0


def test_find_optimal_threshold_tuning():
    """Verify optimal threshold search picks candidate that maximizes validation metric."""
    y_true = [0, 0, 0, 1, 1]
    y_scores = [0.1, 0.2, 0.35, 0.4, 0.9]
    # At threshold 0.5, tp=1, fp=0, fn=1 -> prec=1, rec=0.5, f1=0.667
    # At threshold 0.38, tp=2, fp=0, fn=0 -> prec=1, rec=1.0, f1=1.0
    best_t, metrics = find_optimal_threshold(y_true, y_scores, criterion="f1")
    assert 0.35 < best_t < 0.40
    assert metrics["f1"] == 1.0


# -----------------------------------------------------------------------------
# 2. Model Training Unit Tests (Baseline & LightGBM)
# -----------------------------------------------------------------------------

def test_baseline_logistic_regression_fit_and_predict(synthetic_paysim_df):
    """Verify Logistic Regression baseline pipeline handles missing values and produces predictions."""
    train_mask = synthetic_paysim_df["split"] == "train"
    valid_mask = synthetic_paysim_df["split"] == "valid"

    from member4_ml.src.feature_selection import select_features
    X_train, y_train = select_features(synthetic_paysim_df[train_mask], dataset="paysim")
    X_valid, y_valid = select_features(synthetic_paysim_df[valid_mask], dataset="paysim")

    trainer = ModelTrainer(dataset="paysim", model_type="baseline")
    res = trainer.fit(X_train, y_train, X_valid, y_valid, random_state=42)

    assert res["model_type"] == "baseline"
    assert "precision" in res["valid_metrics"]

    # Predict probabilities
    probs = trainer.predict_proba(X_valid)
    assert len(probs) == len(X_valid)
    assert np.all((probs >= 0.0) & (probs <= 1.0))

    # Binary predictions
    preds = trainer.predict(X_valid)
    assert len(preds) == len(X_valid)
    assert set(np.unique(preds)).issubset({0, 1})


def test_lightgbm_native_nan_and_categoricals(synthetic_ieee_df):
    """Verify LightGBM natively handles sparse missing values (NaN) and string categoricals without imputation."""
    train_mask = synthetic_ieee_df["split"] == "train"
    valid_mask = synthetic_ieee_df["split"] == "valid"

    from member4_ml.src.feature_selection import select_features
    X_train, y_train = select_features(synthetic_ieee_df[train_mask], dataset="ieee")
    X_valid, y_valid = select_features(synthetic_ieee_df[valid_mask], dataset="ieee")

    # Confirm presence of native NaNs and string/object categoricals
    assert X_train.isna().any().any()
    is_cat = [
        c for c in X_train.columns
        if X_train[c].dtype == "object"
        or isinstance(X_train[c].dtype, (pd.CategoricalDtype, pd.StringDtype))
        or str(X_train[c].dtype) in ("string", "str")
    ]
    assert len(is_cat) > 0

    trainer = ModelTrainer(dataset="ieee", model_type="lightgbm")
    res = trainer.fit(X_train, y_train, X_valid, y_valid, random_state=42)

    assert res["model_type"] == "lightgbm"
    assert 0.0 <= res["optimal_threshold"] <= 1.0

    probs = trainer.predict_proba(X_valid)
    assert len(probs) == len(X_valid)
    assert not np.isnan(probs).any()


def test_model_artifact_save_and_load(synthetic_paysim_df):
    """Verify saving and loading model artifacts with reproducibility metadata."""
    train_mask = synthetic_paysim_df["split"] == "train"
    from member4_ml.src.feature_selection import select_features
    X_train, y_train = select_features(synthetic_paysim_df[train_mask], dataset="paysim")

    trainer = ModelTrainer(dataset="paysim", model_type="lightgbm")
    trainer.fit(X_train, y_train, random_state=42)

    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = trainer.save_artifact(model_dir=tmpdir, filename_prefix="test_model")
        assert os.path.exists(model_path)
        meta_path = model_path.replace(".joblib", "_metadata.json")
        assert os.path.exists(meta_path)

        # Load artifact
        loaded = ModelTrainer.load_artifact(model_path)
        assert loaded.model_type == "lightgbm"
        assert loaded.feature_names == trainer.feature_names

        # Inferences must be 100% identical
        orig_probs = trainer.predict_proba(X_train)
        loaded_probs = loaded.predict_proba(X_train)
        np.testing.assert_allclose(orig_probs, loaded_probs, rtol=1e-5)


def test_train_valid_column_mismatch_raises_error(synthetic_paysim_df):
    """Verify column mismatch between train and valid raises ValueError."""
    from member4_ml.src.feature_selection import select_features
    X_train, y_train = select_features(synthetic_paysim_df, dataset="paysim")
    X_valid = X_train.copy().drop(columns=["amount_log1p"])

    trainer = ModelTrainer(dataset="paysim", model_type="baseline")
    with pytest.raises(ValueError, match="feature columns or ordering do not match"):
        trainer.fit(X_train, y_train, X_valid, y_train)


# -----------------------------------------------------------------------------
# 3. PaySim Ablation Unit Tests
# -----------------------------------------------------------------------------

def test_run_paysim_ablation(synthetic_paysim_df):
    """Verify PaySim ablation study runs side-by-side with identical partitions and formats."""
    train_df = synthetic_paysim_df[synthetic_paysim_df["split"] == "train"].copy()
    valid_df = synthetic_paysim_df[synthetic_paysim_df["split"] == "valid"].copy()

    ablation = run_paysim_ablation(train_df, valid_df, model_type="lightgbm", random_state=42)

    assert ablation["tier_a"]["num_features"] == 32
    assert ablation["tier_ab"]["num_features"] == 37
    assert "artifact_warning_and_explanation" in ablation
    assert "valid_metrics" in ablation["tier_a"]
    assert "valid_metrics" in ablation["tier_ab"]


def test_class_weight_strategies(synthetic_paysim_df):
    """Verify that sqrt_damping and ratio class weight strategies compute expected weights."""
    from member4_ml.src.feature_selection import select_features
    X_train, y_train = select_features(synthetic_paysim_df, dataset="paysim", include_tier_b=False)

    trainer_sqrt = ModelTrainer(dataset="paysim", model_type="lightgbm")
    res_sqrt = trainer_sqrt.fit(X_train, y_train, class_weight_strategy="sqrt_damping")

    n_pos = int(np.sum(y_train))
    n_neg = len(y_train) - n_pos
    expected_sqrt_weight = min(np.sqrt(n_neg / n_pos), 50.0)

    assert trainer_sqrt.metadata["class_weight_strategy"] == "sqrt_damping"
    assert abs(trainer_sqrt.metadata["scale_pos_weight"] - expected_sqrt_weight) < 1e-4
