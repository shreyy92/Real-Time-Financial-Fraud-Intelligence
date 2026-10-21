"""
Training Pipeline for Member 4 Baseline and Gradient Boosted Models.

Features:
- Reproducible model training with fixed random seeds.
- Preprocessing pipeline fitted exclusively on training data.
- Logistic Regression baseline (with median imputation and one-hot encoding).
- LightGBM classifier with native handling of missing values (NaN) and categorical features.
- Imbalance mitigation via class_weight='balanced' or scale_pos_weight.
- Artifact serialization with comprehensive reproducibility metadata.
"""

from typing import Dict, Any, Optional, Tuple, List, Union
import os
import json
import joblib
import numpy as np
import pandas as pd
from datetime import datetime, timezone

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import lightgbm as lgb

from member4_ml.src.feature_selection import load_config, select_features
from member4_ml.src.evaluate import evaluate_predictions, find_optimal_threshold


class ModelTrainer:
    """Trains and serializes fraud detection models adhering to contract safeguards."""

    def __init__(self, dataset: str, model_type: str = "lightgbm", config: Optional[Dict[str, Any]] = None):
        self.dataset = dataset.lower()
        if self.dataset not in ("paysim", "ieee"):
            raise ValueError(f"Unknown dataset '{dataset}'. Must be 'paysim' or 'ieee'.")

        self.model_type = model_type.lower()
        if self.model_type not in ("baseline", "logistic_regression", "lightgbm"):
            raise ValueError(f"Unknown model_type '{model_type}'. Must be 'baseline' or 'lightgbm'.")

        self.config = config or load_config()
        self.fitted_pipeline: Optional[Any] = None
        self.feature_names: List[str] = []
        self.optimal_threshold: float = 0.5
        self.metadata: Dict[str, Any] = {}

    def _validate_train_valid_inputs(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_valid: Optional[pd.DataFrame] = None,
        y_valid: Optional[pd.Series] = None,
    ) -> None:
        """Validate shapes, non-emptiness, feature ordering, and labels."""
        if len(X_train) == 0:
            raise ValueError("X_train cannot be empty.")
        if len(y_train) == 0:
            raise ValueError("y_train cannot be empty.")
        if len(X_train) != len(y_train):
            raise ValueError(f"X_train ({len(X_train)}) and y_train ({len(y_train)}) length mismatch.")

        if X_valid is not None:
            if list(X_train.columns) != list(X_valid.columns):
                raise ValueError("Train and validation feature columns or ordering do not match.")
            if y_valid is not None and len(X_valid) != len(y_valid):
                raise ValueError(f"X_valid ({len(X_valid)}) and y_valid ({len(y_valid)}) length mismatch.")

    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_valid: Optional[pd.DataFrame] = None,
        y_valid: Optional[pd.Series] = None,
        random_state: int = 42,
        optimize_threshold: bool = True,
        threshold_criterion: str = "f1",
        include_tier_b: bool = False,
    ) -> Dict[str, Any]:
        """
        Fit model using precomputed train split, tune threshold on valid split.

        Parameters
        ----------
        X_train : pd.DataFrame
            Training feature matrix.
        y_train : pd.Series
            Training target series (binary).
        X_valid : Optional[pd.DataFrame]
            Validation feature matrix.
        y_valid : Optional[pd.Series]
            Validation target series.
        random_state : int
            Reproducibility seed.
        optimize_threshold : bool
            Whether to optimize decision threshold on validation data.
        threshold_criterion : str
            Optimization objective ('f1' or 'fbeta').
        include_tier_b : bool
            Flag recording whether Tier B was included (for ablation metadata).

        Returns
        -------
        results : Dict[str, Any]
            Training and validation metrics summary.
        """
        self._validate_train_valid_inputs(X_train, y_train, X_valid, y_valid)
        self.feature_names = list(X_train.columns)

        # Determine numerical vs categorical columns
        cat_cols = [
            c for c in X_train.columns
            if X_train[c].dtype == "object"
            or isinstance(X_train[c].dtype, (pd.CategoricalDtype, pd.StringDtype))
            or str(X_train[c].dtype) in ("string", "str")
        ]
        num_cols = [c for c in X_train.columns if c not in cat_cols]

        # Class imbalance calculation
        n_pos = int(np.sum(y_train))
        n_neg = len(y_train) - n_pos
        pos_weight = float(n_neg / n_pos) if n_pos > 0 else 1.0

        if self.model_type in ("baseline", "logistic_regression"):
            # Explicit preprocessing pipeline fitted ONLY on training data
            num_transformer = Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
            ])

            transformers = [("num", num_transformer, num_cols)]
            if cat_cols:
                cat_transformer = Pipeline([
                    ("imputer", SimpleImputer(strategy="constant", fill_value="missing")),
                    ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                ])
                transformers.append(("cat", cat_transformer, cat_cols))

            preprocessor = ColumnTransformer(transformers=transformers)
            clf = LogisticRegression(
                class_weight="balanced",
                max_iter=1000,
                random_state=random_state,
                solver="lbfgs",
            )
            self.fitted_pipeline = Pipeline([
                ("preprocessor", preprocessor),
                ("classifier", clf),
            ])
            self.fitted_pipeline.fit(X_train, y_train)

        elif self.model_type == "lightgbm":
            # For LightGBM, convert object/string categoricals to category dtype
            X_train_lgb = X_train.copy()
            for c in cat_cols:
                X_train_lgb[c] = X_train_lgb[c].astype("category")

            clf = lgb.LGBMClassifier(
                n_estimators=100,
                learning_rate=0.05,
                scale_pos_weight=pos_weight,
                random_state=random_state,
                verbose=-1,
                n_jobs=-1,
            )
            clf.fit(X_train_lgb, y_train)
            self.fitted_pipeline = clf

        # Evaluate on training data
        train_probs = self.predict_proba(X_train)
        train_metrics = evaluate_predictions(y_train, train_probs, threshold=0.5)

        valid_metrics = None
        if X_valid is not None and y_valid is not None:
            valid_probs = self.predict_proba(X_valid)
            if optimize_threshold:
                self.optimal_threshold, valid_metrics = find_optimal_threshold(
                    y_valid, valid_probs, criterion=threshold_criterion
                )
            else:
                self.optimal_threshold = 0.5
                valid_metrics = evaluate_predictions(y_valid, valid_probs, threshold=0.5)

        # Store metadata
        self.metadata = {
            "model_type": self.model_type,
            "dataset": self.dataset,
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "random_state": random_state,
            "num_features": len(self.feature_names),
            "feature_names": self.feature_names,
            "include_tier_b": include_tier_b,
            "train_samples": len(X_train),
            "train_fraud_count": n_pos,
            "train_fraud_rate": float(n_pos / len(X_train)),
            "optimal_threshold": float(self.optimal_threshold),
            "threshold_criterion": threshold_criterion,
            "train_metrics": train_metrics,
            "valid_metrics": valid_metrics,
        }

        return {
            "model_type": self.model_type,
            "optimal_threshold": self.optimal_threshold,
            "train_metrics": train_metrics,
            "valid_metrics": valid_metrics,
        }

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """
        Generate fraud probability predictions for X.

        Ensures column alignment and orders identically to training features.
        """
        if self.fitted_pipeline is None:
            raise RuntimeError("Model has not been fitted yet.")

        missing = [c for c in self.feature_names if c not in X.columns]
        if missing:
            raise ValueError(f"Input X is missing features: {missing[:5]}...")

        X_ordered = X[self.feature_names].copy()

        if self.model_type == "lightgbm":
            # Convert object and string columns to category dtype to match training
            for c in X_ordered.columns:
                if (
                    X_ordered[c].dtype == "object"
                    or isinstance(X_ordered[c].dtype, pd.StringDtype)
                    or str(X_ordered[c].dtype) in ("string", "str")
                ):
                    X_ordered[c] = X_ordered[c].astype("category")
            probs = self.fitted_pipeline.predict_proba(X_ordered)[:, 1]
        else:
            probs = self.fitted_pipeline.predict_proba(X_ordered)[:, 1]

        return probs

    def predict(self, X: pd.DataFrame, threshold: Optional[float] = None) -> np.ndarray:
        """Generate binary fraud predictions using the optimal or specified threshold."""
        thresh = threshold if threshold is not None else self.optimal_threshold
        probs = self.predict_proba(X)
        return (probs >= thresh).astype(np.int32)

    def save_artifact(self, model_dir: Optional[str] = None, filename_prefix: Optional[str] = None) -> str:
        """
        Save fitted model and metadata JSON inside the specified model directory.

        Returns
        -------
        model_path : str
            Path to the saved joblib model artifact.
        """
        if self.fitted_pipeline is None:
            raise RuntimeError("Cannot save an unfitted model.")

        if model_dir is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            model_dir = os.path.join(base_dir, "models")

        os.makedirs(model_dir, exist_ok=True)

        prefix = filename_prefix or f"{self.dataset}_{self.model_type}"
        model_file = os.path.join(model_dir, f"{prefix}.joblib")
        meta_file = os.path.join(model_dir, f"{prefix}_metadata.json")

        # Save model pipeline
        joblib.dump(self.fitted_pipeline, model_file)

        # Save metadata
        with open(meta_file, "w", encoding="utf-8") as f:
            json.dump(self.metadata, f, indent=2)

        return model_file

    @classmethod
    def load_artifact(cls, model_file: str, meta_file: Optional[str] = None) -> "ModelTrainer":
        """Load a saved model pipeline and metadata."""
        if not os.path.exists(model_file):
            raise FileNotFoundError(f"Model artifact not found at: {model_file}")

        if meta_file is None:
            meta_file = model_file.replace(".joblib", "_metadata.json")

        with open(meta_file, "r", encoding="utf-8") as f:
            meta = json.load(f)

        trainer = cls(dataset=meta["dataset"], model_type=meta["model_type"])
        trainer.fitted_pipeline = joblib.load(model_file)
        trainer.feature_names = meta["feature_names"]
        trainer.optimal_threshold = meta.get("optimal_threshold", 0.5)
        trainer.metadata = meta
        return trainer
