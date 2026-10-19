from __future__ import annotations
import matplotlib.pyplot as plt
from sklearn.metrics import precision_recall_curve, average_precision_score

"""
train_fraud_model.py

Purpose:
Train and evaluate baseline machine learning models for synthetic healthcare
claims fraud detection.

Why this file exists:
We already built the data pipeline:
    raw claims -> cleaned claims -> PostgreSQL analytics -> fraud labels

This script takes the labeled dataset and answers:
    "Can we train a model to detect suspicious claims?"

Version 1 goals:
1) Load the labeled dataset.
2) Engineer a few high-signal numeric features.
3) Train two models:
    - Logistic Regression (interpretable baseline)
    - Random Forest (stronger nonlinear baseline)
4) Evaluate bpth models using fraud-relevant metrics.

Important note:
Because fraud is rare (~2.29% in our dataset), accuracy is NOT the main metric.
We care much more about:
- precision
- recall
- F1 score
- ROC AUC

## How to run (from repo root):

 python -u .\\ml\\ train_fraud_model.py
"""


from pathlib import Path
from typing import Any
# -----------------------------------------------------------------------------------
# Third-party imports
# -----------------------------------------------------------------------------------
import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

# ----------------------------------------------------------------------------------- 
# Configuration
# -----------------------------------------------------------------------------------

# Path to the labeled dataset produced by the fraud-label generation step.
DATA_PATH: Path = Path("data") / "clean" / "claims_features.csv"

# Name of the target column.
TARGET_COLUMN: str = "is_fraud"

# Fixed random seed for reproducinility.
RANDOM_STATE: int = 42

# Fraction of the dataset reserved for testing.
TEST_SIZE: float = 0.20

# -----------------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------------

def print_dataset_summary(df: pd.DataFrame, target_col: str) -> None:
    """
    Print basic dataset summary information.
    This acts as a sanity check before modeling.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Input dataset.
    target_col : str
    Name of the fraud label column.
    """
    total_rows = len(df)
    fraud_count = int(df[target_col].sum())
    fraud_rate = (fraud_count / total_rows) * 100 if total_rows > 0 else 0.0

    print("=" * 70)
    print("DATASET SUMMARY")
    print("=" * 70)
    print(f"Rows loaded: {total_rows:,}")
    print(f"Fraud rows: {fraud_count:,}")
    print(f"Fraud rate: {fraud_rate:.2f}%")
    print()
    print("Class distribution:")
    print(df[target_col].value_counts(dropna=False))
    print()

def print_model_results(
    model_name: str,
    y_true: pd.Series,
    y_pred: np.ndarray,
    y_proba: np.ndarray,
) -> None:
    """
    Print evaluation results for a trained model.

    Metrics printed:
    - classification report
    - confusion matrix
    - ROC AUC
    
    Parameters:
    -----------
    model_name : str
        Human-readable model name.
    y_true : pd.Series
        True labels.
    y_pred : np.ndarray
        Predicted class labels.
    y_proba : np.ndarray
        Predicted fraud probabilities for the positive class.
    """
    print("=" * 70)
    print(f"{model_name.upper()} RESULTS")
    print("=" * 70)

    # Classification report includes precision, recall, F1, and support.
    print("Classification Report:")
    print(classification_report(y_true, y_pred, digits=4, zero_division=0))

    # Confusion matrix gives the raw counts of:
    # TN, FP
    # FN, TP
    cm = confusion_matrix(y_true, y_pred)
    print("Confusion Matrix:")
    print(cm)

    # ROC AUC helps summarize randking quality accross thresholds.
    auc = roc_auc_score(y_true, y_proba)
    print(f"ROC AUC: {auc:.4f}")
    print()

def get_feature_names_from_preprocessor(
    preprocessor: ColumnTransformer,
    numeric_features: list[str],
    categorical_features: list[str],
) -> list[str]:
    
    """
    Recover transformed feature names from the fitted preprocessor.

    Why this matters:
    Random Forest can output feature importances, but after one-hot encoding,
    the original columns expand into many transformed columns.
    We need the post-transform names to interpret importances correctly.

    Parameters:
    -----------
    preprocessor : ColumnTransformer
        A fitted ColumnTransformer from the model pipeline.
    numeric_features : list[str]
        Original numeric feature names.
    categorical_features : list[str]
        Original categorical feature names.

    Returns:
    -----------
    list[str]
        Full list of transformed feature names.
    """
    feature_names: list[str] = []

    # Numeric pipeline does not create new column names.
    feature_names.extend(numeric_features)

    # Pull the fitted OneHotEncoder out of the categorical pipeline.
    cat_pipeline = preprocessor.named_transformers_["cat"]
    encoder: OneHotEncoder = cat_pipeline.named_steps["onehot"]

    # These are the expanded categorical feature names after one-hot encoding.
    encoded_names = encoder.get_feature_names_out(categorical_features).tolist()
    feature_names.extend(encoded_names)

    return feature_names

def evaluate_at_threshold(
        y_true,
        y_proba,
        threshold: float,
        model_name: str,
) -> None:
    """
    Evaluate a probabilistic classifier at a custom decision threshold.
    Why this matters:
    -----------------
    By default, many classifiers use a threshold of 0.50:
    predict fraud if probability >= 0.50
    In imbalanced fraud detection, that threshold can often be too strict.
    Lowering the threshold can help catch more fraud cases (high recall),
    though we must consider it may increase false positives.
    
    Parameters:
    -----------------
    y_true : array-like
        True fraud labels.
    y_proba : array-like
        Predicted fraud probabilities.
    threshold : float
        Decision theshold to test.
    model_name : str
        Name of the model being evaluated.
    """

    # Convert predicted probabilities into hard class labels using the threshold.
    y_pred_threshold = (y_proba >= threshold).astype(int)

    print("=" * 70)
    print(f"{model_name.upper()} @ THRESHOLD = {threshold:.2f}")
    print("=" * 70)

    # Print classification metrics at this threshold.
    print("Classification Report:")
    print(classification_report(y_true, y_pred_threshold, digits=4, zero_division=0))

    # Print confusion matrix at this threshold.
    cm = confusion_matrix(y_true, y_pred_threshold)
    print("Confusion Matrix:")
    print(cm)
    print()

def run_threshold_sweep(
        y_true,
        y_proba,
        thresholds: list[float],
        model_name: str,
) -> None:
    """
    Run the same model accross multiple decision thresholds.
    Why of its existence:
    ---------------------
    Fraud detection isn't just wise model choices, but also
    choosing the right operating threshold.
    
    This function helps us compare how precision and recall change
    as we adjust the models sensitivity.
    
    Parameters:
    ---------------------
    y_true : array-like
        True fraud labels.
    y_proba : array-like
        Predicted fraud probabilities for the positive class.
    thresholds : list[float]
        Thresholds to evaluate.
    model_name : str
        Name of the model being tested.
    """
    for threshold in thresholds:
        evaluate_at_threshold(
            y_true=y_true,
            y_proba=y_proba,
            threshold=threshold,
            model_name=model_name,
        )
def train_logistic_with_weight_sweep(
    X_train,
    X_test,
    y_train,
    y_test,
    preprocessor,
    weight_settings: list[dict[int, int]],
) -> tuple[Pipeline, np.ndarray]:
    
    """
    Train multiple Logistic Regression models using different class weights.
    
    Why of its existence:
    Fraud is rare, so a model may learn to ignore it.
    By increasing the fraud-class weight, we tell the model
    that missing fraud should be penalized more heavily.
    
    Parameters
    ----------
    X_train, X_test :
        Training and testing feature matrices.
    y_train, y_test :
        Training and testing target labels.
    preprocessor :
        The fitted or reusable preprocessing pipeline for numeric/categorical data.
    weight_settings :
        A list of dictionaries such as:
           [{0: 1, 1: 5}, {0: 1, 1: 10}, {0: 1, 1: 20}]
           
    Returns
    ----------
    best_model : Pipeline
        The Logistic Regression pipeline with the best ROC AUC on the test set.
    best_proba : np.ndarray
        Predicted fraud probabilities from the best model.
    """
    best_model = None
    best_auc = -1.0
    best_proba = None

    print("=" * 70)
    print("CLASS-WEIGHT SWEEP: LOGISTIC REGRESSION")
    print("=" * 70)

    for weights in weight_settings:
        print(f"[ML] Training logistic Regression with class_weight={weights}")

        model = Pipeline(
            steps=[
                ("preprocessor", preprocessor),
                (
                    "classifier",
                    LogisticRegression(
                        max_iter=1000,
                        class_weight=weights,
                        random_state=RANDOM_STATE,
                    ),
                ),
            ]
        )

        # Fit model on training data
        model.fit(X_train, y_train)

        # Convert predictions to NumPy arrays for cleaner typing and consistency
        y_pred = np.asarray(model.predict(X_test))
        y_proba = np.asarray(model.predict_proba(X_test)[:, 1])

        # Evaluate the current model
        print_model_results(
            model_name=f"Logistic Regression class_weight={weights}",
            y_true=y_test,
            y_pred=y_pred,
            y_proba=y_proba,
        )

        auc = roc_auc_score(y_test, y_proba)
        print(f"[ML] ROC AUC for class_weight={weights}: {auc:.4f}")
        print()

        # Keep the model with the highest ROC AUC
        if auc > best_auc:
            best_auc = auc
            best_model = model
            best_proba = y_proba
    
    print(f"[ML] Best LogisticRegression ROC AUC from weight sweep:{best_auc:.4f}")
    print()

    # These assertions keep the type checker content and protect against impossible states
    assert best_model is not None
    assert best_proba is not None

    return best_model, best_proba

def plot_precision_recall_curve_pretty(
        y_true,
        y_proba,
        model_name: str,
        output_path: Path,
        thresholds_to_mark: list[float] | None = None,
) -> None:
    """
    Create a polished Precision-Recall curve and save it to disk.
    The why of its existence:
    In imbalanced fraud detection, th Precision-Recall curve is one of the most
    useful visual tools given it shows the tradeoff between:
    - how many fraud cases we catch(recall)
    - how many flagged cases are truly fraud (precision)
    """
    # -------------------------------------------------------------------------------
    # Step 1: Compute the full precision-recall curve
    # -------------------------------------------------------------------------------
    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    avg_precision = average_precision_score(y_true, y_proba)

    # Base fraud rate is useful as a visual reference point.
    # If the model performs only at the base rate, it's not doing much better.
    # than bling prioritization.
    base_rate = float(np.mean(y_true))

    # Ensure output directory exists.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # -------------------------------------------------------------------------------
    # Step 2: Create the figure
    # -------------------------------------------------------------------------------
    plt.figure(figsize=(10, 7))

    # Plot the PR curve with a thick line for visual clarity.
    plt.plot(
        recall,
        precision,
        linewidth=3,
        label=f"PR Curve | AP = {avg_precision:.4f}",
        color="#d63384", 
    )

    # Plot a horizontal baseline equal to the fraud rate.
    # This helps viewers understand wether the model is meaningfully
    # better than the raw class prevalence
    plt.axhline(
        y=base_rate,
        linestyle="--",
        linewidth=2,
        color="#9c36b5",
        alpha=0.8,
        label=f"Base fraud rate = {base_rate:.4f}",
    )

    # -------------------------------------------------------------------------------
    # Step 3: Mark selected thresholds on the curve
    # -------------------------------------------------------------------------------
    # precision_recall_curve returns arrays where thresholds has length n-1.
    # The threshold at index i corresponds approximately to precision[i+1], recall[i+1].
    if thresholds_to_mark is not None:
        for target_threshold in thresholds_to_mark:
            # Find the threshold index closest to the requested threshold.
            idx = int(np.argmin(np.abs(thresholds - target_threshold)))

            # Shift by +1 to align with precision/recall arrays.
            point_precision = precision[idx + 1]
            point_recall = recall[idx + 1]

            # Plot the marked point.
            plt.scatter(
                point_recall,
                point_precision,
                s=110,
                color="#ff8fab",
                edgecolor="black",
                linewidth=0.8,
                zorder=5,
            )

            # Annotate the threshold near the point.
            label_text = (
                f"T={target_threshold:.2f}\n"
                f"P={point_precision:.2f}, R={point_recall:.2f}"
            )

            plt.annotate(
                label_text,
                xy=(point_recall, point_precision),
                xytext=(10, 10),
                textcoords="offset points",
                fontsize=9,
                bbox=dict(
                    boxstyle="round,pad=0.3",
                    fc="#fff0f6",
                    ec="#d63384",
                    alpha=0.95,
                ),
            )
    # -------------------------------------------------------------------------------
    # Step 4: Style the chart
    #--------------------------------------------------------------------------------
    plt.title(
        f"Precision-Recall Curve\n{model_name}",
        fontsize=16,
        fontweight="bold",
    )
    plt.xlabel("Recall", fontsize=13)
    plt.ylabel("Precision", fontsize=13)

    # Set plot limits for cleaner framing.
    plt.xlim(0, 1.01)
    plt.ylim(0, 1.05)

    # Add a soft grid for readability without clutter.
    plt.grid(True, linestyle=":", linewidth=0.8, alpha=0.5)

    # Add legend.
    plt.legend(loc="upper right", frameon=True)

    # Tight layout prevents layers from getting cutoff.
    plt.tight_layout()

    # Save a crisp image <3
    plt.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close()

    print(f"[ML] Saved pretty PR curve to: {output_path}")

def plot_threshold_tradeoff_pretty(
    y_true,
    y_proba,
    thresholds_to_plot: list[float],
    model_name: str,
    output_path: Path,
) -> None:
    """
    Plot precision and recall as a function of decision threshold.
    
    Why this chart is important:
    ----------------------------
    This chart makes the precision-recall tradeoff very explicit.
    """
    precisions = []
    recalls = []

    for threshold in thresholds_to_plot:
        # Convert probabilities  to predicted class labels using the threshold.
        y_pred = (y_proba >= threshold).astype(int)

        # Build confusion matrix values manually.
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

        # Precision: of all flagged claims, how many were actually fraud?
        precision_value = tp / (tp + fp) if (tp + fp) > 0 else 0.0

        # Recall: of all actual fraud cases, how many did we catch?
        recall_value = tp / (tp + fn) if (tp + fn) > 0 else 0.0

        precisions.append(precision_value)
        recalls.append(recall_value)
    
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(10, 7))

    # Plot precision line
    plt.plot(
        thresholds_to_plot,
        precisions,
        marker="o",
        linewidth=3,
        markersize=8,
        color="#d63384",
        label="Precision",
    )

    # Plot recall line
    plt.plot(
        thresholds_to_plot,
        recalls,
        marker="o",
        linewidth=3,
        markersize=8,
        color="#9c36b5",
        label="Recall",
    )

    # Annotate each point to make the story easier to follow.
    for x, p, r in zip(thresholds_to_plot, precisions, recalls):
        plt.annotate(
            f"{p:.2f}",
            xy=(x, p),
            xytext=(0, 8),
            textcoords="offset points",
            ha="center",
            fontsize=9,
            color="#d63384",
        )
        plt.annotate(
            f"{r:.2f}",
            xy=(x, r),
            xytext=(0, -16),
            textcoords="offset points",
            ha="center",
            fontsize=9,
            color="#9c36b5",
        )
    plt.title(
        f"Threshold Tradeoff\n{model_name}",
        fontsize=16,
        fontweight="bold",
    )
    plt.xlabel("Decision Threshold", fontsize=13)
    plt.ylabel("Metrix Value", fontsize=13)

    # Invert x-axis so the chart reads intuitively:
    # left = stricter threshold, right = more aggressive threshold
    plt.gca().invert_xaxis()

    plt.ylim(0, 1.05)
    plt.grid(True, linestyle=":", linewidth=0.8, alpha=0.5)
    plt.legend(loc="best")
    plt.tight_layout()

    plt.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close()

    print(f"[ML] Saved threshold tradeoff chart to: {output_path}")
    
# -----------------------------------------------------------------------------------
# Main script logic
# -----------------------------------------------------------------------------------
def main() -> None:
    """
    Execute the full fraud-model training workflow.
    """

    # -------------------------------------------------------------------------------
    # Step 1: Confirm the dataset exists
    # -------------------------------------------------------------------------------
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Labeled dataset not found at: {DATA_PATH}\n"
            f"Run feature_engineering.py first."
        )
    # -------------------------------------------------------------------------------
    # Step 2: Load the dataset
    # -------------------------------------------------------------------------------
    print(f"[ML] Reading labeled claims from: {DATA_PATH}")
    df = pd.read_csv(DATA_PATH)

    # Print a basic dataset summary so we can verify:
    # - total row count
    # - fraud count
    # - class imbalance
    print_dataset_summary(df, TARGET_COLUMN)

 
    # ---------------------------------------------------------------------------------
    # Step 3: Define target and selected feature columns
    # ---------------------------------------------------------------------------------
    # We intentionally exclude raw identifiers like claim_id/member_id/provider_id
    # for version 1 so the model focuses on generalizable behavior instead of
    # memorizing IDs.
    numeric_features = [
        "billed_amount",
        "allowed_amount",
        "paid_amount",
        "billing_ratio",
        "paid_ratio",
        "paid_to_billed",
        "service_year",
        "service_month",
        "service_day",
        "service_dayofweek",
    ]

    categorical_features = [
        "cpt_code",
        "dx_code",
        "status",
        "member_state",
    ]

    feature_columns = numeric_features + categorical_features

    X = df[feature_columns].copy()
    y = df[TARGET_COLUMN].copy()

    # ---------------------------------------------------------------------------------
    # Step 4: Train/test split with stratification
    # ---------------------------------------------------------------------------------
    # Stratify is important here because fraud is rare.
    # It preserves the fraud ratio in both train and test sets.
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
        stratify=y,
    )

    print("=" * 70)
    print("TRAIN / TEST SPLIT")
    print("=" * 70)
    print(f"Training rows: {len(X_train):,}")
    print(f"Testing rows: {len(X_test):,}")
    print()

    # ----------------------------------------------------------------------------------
    # Step 5: Build preprocessing pipelines
    # ----------------------------------------------------------------------------------
    # Numeric pipeline:
    # - impute missing values with median
    # - scale features (important for Logistic Regression)
    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    # Catergorical pipeline:
    # - impute missing values with most frequent category
    # - one-hot encode categories for ML compatibility
    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            (
            "onehot",
            OneHotEncoder(handle_unknown="ignore"),
            ),
        ]
    )
    # ColumnTransformer applies different preprocessing to different columns.
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_features),
            ("cat", categorical_transformer, categorical_features),
        ]
    )

    # -----------------------------------------------------------------------------------
    # Step 6: Model A - Logistic Regression
    # -----------------------------------------------------------------------------------
    # The why of Logistic Regression:
    # - fast baseline
    # - interpretable
    # - good first sanity check
    # class_weight="balanced" tells the model to be more mindful
    # towards the minority class (fraud).
    logistic_model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier",
             LogisticRegression(
                 max_iter=1000,
                 class_weight="balanced",
                 random_state=RANDOM_STATE,
                ),
            ),
        ]
    )

    print("[ML] Training Logistic Regression...")
    logistic_model.fit(X_train, y_train)

    # Predict class labels.
    y_pred_log = np.asarray(logistic_model.predict(X_test))

    # Predict probabilities for the positive class (fraud = 1).
    y_proba_log = np.asarray(logistic_model.predict_proba(X_test)[:, 1])

    print_model_results(
        model_name="Logistic Regression",
        y_true=y_test,
        y_pred=y_pred_log,
        y_proba=y_proba_log,
    )

    # -----------------------------------------------------------------------------------
    # Step 7: Model B - Random Forest
    # -----------------------------------------------------------------------------------
    # The why of Random Forest:
    # - strong baseline for tabular data
    # - can model nonlinear interactions
    # - provides feature importances
    # Note: Random Forest does not need scaling in principle,
    # but reusing the same preprocessor keeps the pipeline cleaner.
    random_forest_model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "classifier",
                RandomForestClassifier(
                    n_estimators=200,
                    random_state=RANDOM_STATE,
                    class_weight="balanced",
                    n_jobs=-1,
                ),
            ),
        ]
    )

    print("[ML] Training Random Forest...")
    random_forest_model.fit(X_train, y_train)

    y_pred_rf = np.asarray(random_forest_model.predict(X_test))
    y_proba_rf = np.asarray(random_forest_model.predict_proba(X_test)[:, 1])

    print_model_results(
        model_name="Random Forest",
        y_true=y_test,
        y_pred=y_pred_rf,
        y_proba=y_proba_rf,
    )

    # -----------------------------------------------------------------------------------
    # Step 8: Feature importance for Random Forest
    # -----------------------------------------------------------------------------------
    # Recover the fitted preprocessor and fitted classifier from the pipeline.
    fitted_preprocessor: ColumnTransformer = random_forest_model.named_steps["preprocessor"]
    fitted_rf: RandomForestClassifier = random_forest_model.named_steps["classifier"]

    # Recover transformed feature names so importance output is interpretable.
    transformed_feature_names = get_feature_names_from_preprocessor(
        preprocessor=fitted_preprocessor,
        numeric_features=numeric_features,
        categorical_features=categorical_features,
    )

    # Pair feature names with importance values.
    importance_df = pd.DataFrame(
        {
            "feature": transformed_feature_names,
            "importance": fitted_rf.feature_importances_,
        }
    ).sort_values("importance", ascending=False)

    print("=" * 70)
    print("TOP RANDOM FOREST FEATURE IMPORTANCES")
    print("=" * 70)
    print(importance_df.head(15).to_string(index=False))
    print()

    # ----------------------------------------------------------------------------------
    # Step 10: Simple model comparison summary
    # ----------------------------------------------------------------------------------
    logistic_auc = roc_auc_score(y_test, y_proba_log)
    random_forest_auc = roc_auc_score(y_test, y_proba_rf)

    print("=" * 70)
    print("MODEL COMPARISON SUMMARY")
    print("=" * 70)
    print(f"Logistic Regression ROC AUC: {logistic_auc:.4f}")
    print(f"Random Forest ROC AUC:       {random_forest_auc:.4f}")
    print()

    if random_forest_auc > logistic_auc:
        print("[ML] Random Forest currently ranks fraud risk better than Logistic Regression.")
    elif logistic_auc > random_forest_auc:
        print("[ML] Logistic Regression currently ranks fraud risk better than Random Forest.")
    else:
        print("[ML] Both models achieved the same ROC AUC on this split.")

    # -----------------------------------------------------------------------------------
    # Step 10b: Class-weight tuning for Logistic Regression
    # -----------------------------------------------------------------------------------
    # We now test stronger fraud weights to see wether the model becomes more
    # sensitive to fraud cases without completely collapsing into false alarms.
    logistic_weight_settings = [
        {0: 1, 1: 5},
        {0: 1, 1: 10},
        {0: 1, 1: 20},
    ]

    best_logistic_model, best_logistic_proba = train_logistic_with_weight_sweep(
        X_train=X_train,
        X_test=X_test,
        y_train=y_train,
        y_test=y_test,
        preprocessor=preprocessor,
        weight_settings=logistic_weight_settings,
    )

    # -----------------------------------------------------------------------------------
    # Step 11: Threshold tuning analysis
    # -----------------------------------------------------------------------------------
    # We test multiple thresholds to see how the fraud-detection tradeoff changes.
    # Lower threshold:
    # - catches more fraud (higher recall)
    # - but increases false positives
    #
    # Higher threshold:
    # - catches fewer fraud cases
    # - but may improve precision
    #
    # This is one of the most essential ideas in imbalanced classification.
    thresholds_to_test = [0.50, 0.30, 0.20, 0.10, 0.05]

    print("=" * 70)
    print("THRESHOLD TUNING: LOGISTIC REGRESSION")
    print("=" * 70)
    run_threshold_sweep(
        y_true=y_test,
        y_proba=best_logistic_proba,
        thresholds=thresholds_to_test,
        model_name="Logistic Regression",
    )

    print("=" * 70)
    print("THRESHOLD TUNING: RANDOM FOREST")
    print("=" * 70)
    run_threshold_sweep(
        y_true=y_test,
        y_proba=y_proba_rf,
        thresholds=thresholds_to_test,
        model_name="Random Forest",
    )

    # -----------------------------------------------------------------------------------
    # Step 12: Precision-Recall curve plots
    # -----------------------------------------------------------------------------------
    thresholds_to_mark = [0.30, 0.20, 0.10, 0.05]

    # Pretty Precision-Recall curves
    plot_precision_recall_curve_pretty(
        y_true=y_test,
        y_proba=y_proba_log,
        model_name="Logistic Regression (baseline)",
        output_path=Path("ml") / "artifacts" / "pr_curve_logistic_baseline_pretty.png",
        thresholds_to_mark=thresholds_to_mark,
    )

    plot_precision_recall_curve_pretty(
        y_true=y_test,
        y_proba=best_logistic_proba,
        model_name="Logistic Regression (best weighted)",
        output_path=Path("ml") / "artifacts" / "pr_curve_logistic_weighted_pretty.png",
        thresholds_to_mark=thresholds_to_mark,
    )

    plot_precision_recall_curve_pretty(
        y_true=y_test,
        y_proba=y_proba_rf,
        model_name="Random Forest",
        output_path=Path("ml") / "artifacts" / "pr_curve_random_forest_pretty.png",
        thresholds_to_mark=thresholds_to_mark,
    )
    # Threshold tradeoff charts
    plot_threshold_tradeoff_pretty(
        y_true=y_test,
        y_proba=best_logistic_proba,
        thresholds_to_plot=[0.50, 0.30, 0.20, 0.10, 0.05],
        model_name="Logistic Regression (best weighted)",
        output_path=Path("ml") / "artifacts" / "threshold_tradeoff_logistic.png",
    )

    plot_threshold_tradeoff_pretty(
        y_true=y_test,
        y_proba=y_proba_rf,
        thresholds_to_plot=[0.50, 0.30, 0.20, 0.10, 0.05],
        model_name="Random Forest",
        output_path=Path("ml") / "artifacts" / "threshold_tradeoff_random_forest.png",
    )

if __name__ == "__main__":
    main()




