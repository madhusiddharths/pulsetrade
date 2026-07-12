"""
Nightly Isolation Forest training pipeline for PulseTrade.

Pulls 30 days of gold features, trains an unsupervised anomaly detector,
evaluates on a held-out final day, and logs run to MLflow.

Designed to be:
  - Standalone runnable: `python -m agent.model_training`
  - Importable from an Airflow DAG (4c)

The actual *triggering* of investigations stays with the z-score detector
in agent/anomaly_detector.py — this model is the slower, multivariate
second-stage detector. See README for the two-tier architecture rationale.
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

# Allow running as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from config import settings
from data.databricks import get_gold_window

logger = logging.getLogger("model_training")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")


# ── config ──────────────────────────────────────────────────────────────────

# Features used for training. Must exist in gold_5min_features.
# Order matters — keep stable across runs for model reuse.
FEATURE_COLS = [
    "mean_change_pct",
    "mean_intraday_range",
    "price_stddev",
    "mean_news_sentiment",
    "news_article_count",
]

# Minimum rows required to train. Below this we log "insufficient data"
# to MLflow and skip the actual training. This is what'll happen in your
# first runs while gold is still backfilling.
MIN_TRAINING_ROWS = 100

# Held-out evaluation window: the last 24h of the 30-day pull.
EVAL_HOURS = 24

# Top-k for precision@k. With 288 5-min windows per day × 5 tickers ≈ 1440 rows,
# k=50 means "flag the top ~3% as anomalous and check how many were *true*
# anomalies." True anomalies for eval are |z-score| >= 2.5 (matches Block 2's
# detector), giving us a no-cost label.
PRECISION_AT_K = 50

# Isolation Forest hyperparameters — tune later, fine for now.
IFOREST_PARAMS = {
    "n_estimators": 100,
    "contamination": 0.03,   # expect ~3% anomalies per the eval definition
    "max_samples": "auto",
    "random_state": 42,
    "n_jobs": -1,
}


# ── types ──────────────────────────────────────────────────────────────────

@dataclass
class TrainingResult:
    """Captured for return value and MLflow logging."""
    n_train_rows: int
    n_eval_rows: int
    n_features: int
    precision_at_k: float | None
    avg_anomaly_score: float | None
    skipped_reason: str | None = None


# ── data prep ──────────────────────────────────────────────────────────────

def _to_feature_matrix(rows: list[dict]) -> pd.DataFrame:
    """Convert raw gold rows to a feature DataFrame; drop rows with any null feature."""
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Coerce to numeric; nulls -> NaN
    for col in FEATURE_COLS:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows missing any feature. We're picky on purpose — training on
    # imputed nulls will tank precision when sentiment data is sparse.
    df = df.dropna(subset=FEATURE_COLS).reset_index(drop=True)
    return df


def _split_train_eval(df: pd.DataFrame, eval_hours: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hold out the last `eval_hours` of data as eval set; everything before is train.
    Splits on window_start so it's strictly chronological.
    """
    if df.empty:
        return df, df

    df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
    cutoff = df["window_start"].max() - pd.Timedelta(hours=eval_hours)
    train = df[df["window_start"] < cutoff].reset_index(drop=True)
    eval_ = df[df["window_start"] >= cutoff].reset_index(drop=True)
    return train, eval_


def _label_anomalies_zscore(df: pd.DataFrame, z_threshold: float = 2.5) -> pd.Series:
    """
    Generate a 'true anomaly' label per row using the same z-score rule as Block 2.
    Per ticker, anything with |z(mean_change_pct)| >= threshold = anomaly.
    Returns boolean Series aligned with df.
    """
    labels = pd.Series(False, index=df.index)
    for ticker, group in df.groupby("ticker"):
        vals = group["mean_change_pct"]
        mean, std = vals.mean(), vals.std()
        if not std or pd.isna(std) or std < 1e-9:
            continue
        z = ((vals - mean) / std).abs()
        labels.loc[group.index] = (z >= z_threshold)
    return labels


# ── training ───────────────────────────────────────────────────────────────

def _train_iforest(X_train: np.ndarray) -> tuple[IsolationForest, StandardScaler]:
    """Fit scaler + Isolation Forest. Returns both for downstream use."""
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_train)

    model = IsolationForest(**IFOREST_PARAMS)
    model.fit(X_scaled)
    return model, scaler


def _evaluate(
    model: IsolationForest,
    scaler: StandardScaler,
    eval_df: pd.DataFrame,
    k: int,
) -> tuple[float, float]:
    """
    Score eval rows; return (precision_at_k, avg_anomaly_score).
    Lower score_samples = more anomalous (sklearn convention).
    """
    X_eval = scaler.transform(eval_df[FEATURE_COLS].values)
    scores = -model.score_samples(X_eval)  # negate so higher = more anomalous
    eval_df = eval_df.copy()
    eval_df["anomaly_score"] = scores

    # Ground truth: z-score-flagged windows (proxy label, see _label_anomalies_zscore)
    true_anomaly = _label_anomalies_zscore(eval_df)
    eval_df["true_anomaly"] = true_anomaly

    # precision@k: of top-k by anomaly_score, how many are true anomalies?
    top_k = eval_df.nlargest(k, "anomaly_score")
    if len(top_k) == 0:
        return 0.0, 0.0
    precision = top_k["true_anomaly"].mean()
    avg_score = scores.mean()
    return float(precision), float(avg_score)


# ── main flow ──────────────────────────────────────────────────────────────

def run_training(
    days_back: int = 30,
    dry_run: bool = False,
) -> TrainingResult:
    """
    Top-level training run.
    - Pulls gold, splits, trains, evaluates
    - Logs everything to MLflow (unless dry_run)
    - Returns TrainingResult
    """
    logger.info("pulling %d days of gold features", days_back)
    rows = get_gold_window(days_back=days_back)
    df = _to_feature_matrix(rows)
    logger.info("after null-drop: %d rows with all features present", len(df))

    if len(df) < MIN_TRAINING_ROWS:
        reason = f"insufficient_data ({len(df)} < {MIN_TRAINING_ROWS})"
        logger.warning("skipping training: %s", reason)
        result = TrainingResult(
            n_train_rows=len(df),
            n_eval_rows=0,
            n_features=len(FEATURE_COLS),
            precision_at_k=None,
            avg_anomaly_score=None,
            skipped_reason=reason,
        )
        if not dry_run:
            _log_to_mlflow(result, model=None, scaler=None)
        return result

    train_df, eval_df = _split_train_eval(df, eval_hours=EVAL_HOURS)
    logger.info("split: train=%d, eval=%d", len(train_df), len(eval_df))

    if len(train_df) < MIN_TRAINING_ROWS or len(eval_df) < PRECISION_AT_K:
        reason = (
            f"insufficient_split (train={len(train_df)}, eval={len(eval_df)}, "
            f"need train>={MIN_TRAINING_ROWS}, eval>={PRECISION_AT_K})"
        )
        logger.warning("skipping training: %s", reason)
        result = TrainingResult(
            n_train_rows=len(train_df),
            n_eval_rows=len(eval_df),
            n_features=len(FEATURE_COLS),
            precision_at_k=None,
            avg_anomaly_score=None,
            skipped_reason=reason,
        )
        if not dry_run:
            _log_to_mlflow(result, model=None, scaler=None)
        return result

    X_train = train_df[FEATURE_COLS].values
    model, scaler = _train_iforest(X_train)
    precision, avg_score = _evaluate(model, scaler, eval_df, PRECISION_AT_K)
    logger.info("eval: precision@%d=%.4f avg_score=%.4f",
                PRECISION_AT_K, precision, avg_score)

    result = TrainingResult(
        n_train_rows=len(train_df),
        n_eval_rows=len(eval_df),
        n_features=len(FEATURE_COLS),
        precision_at_k=precision,
        avg_anomaly_score=avg_score,
    )

    if not dry_run:
        _log_to_mlflow(result, model=model, scaler=scaler)

    return result


# ── MLflow plumbing ────────────────────────────────────────────────────────

def _log_to_mlflow(
    result: TrainingResult,
    model: Optional[IsolationForest],
    scaler: Optional[StandardScaler],
) -> str | None:
    """
    Log run to MLflow. Returns run_id (or None if MLflow init failed).
    All artifacts (model, scaler, features list) go to MLflow's artifact store.
    """
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    mlflow.set_experiment(settings.mlflow_experiment_name)

    with mlflow.start_run() as run:
        # Params
        mlflow.log_params({
            "model_type": "IsolationForest",
            "n_features": result.n_features,
            "feature_cols": ",".join(FEATURE_COLS),
            "eval_hours": EVAL_HOURS,
            "precision_at_k": PRECISION_AT_K,
            **{f"iforest_{k}": v for k, v in IFOREST_PARAMS.items()},
        })

        # Metrics
        mlflow.log_metric("n_train_rows", result.n_train_rows)
        mlflow.log_metric("n_eval_rows", result.n_eval_rows)
        if result.precision_at_k is not None:
            mlflow.log_metric("precision_at_k", result.precision_at_k)
        if result.avg_anomaly_score is not None:
            mlflow.log_metric("avg_anomaly_score", result.avg_anomaly_score)

        # Tags — searchable, easy to filter the registry on
        mlflow.set_tag("dataset_source", "gold_5min_features")
        mlflow.set_tag("stage", "candidate")  # 4c will flip to 'production' on promotion
        if result.skipped_reason:
            mlflow.set_tag("skipped", "true")
            mlflow.set_tag("skip_reason", result.skipped_reason)

        # Model + scaler artifacts (only if we trained)
        if model is not None and scaler is not None:
            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="iforest_model",
                registered_model_name="pulsetrade_iforest",
            )
            # Also log scaler so inference can reproduce preprocessing
            with tempfile.TemporaryDirectory() as tmp:
                import joblib
                p = Path(tmp) / "scaler.pkl"
                joblib.dump(scaler, p)
                mlflow.log_artifact(str(p), artifact_path="iforest_model")

        logger.info("logged run %s to MLflow (experiment: %s)",
                    run.info.run_id, settings.mlflow_experiment_name)
        return run.info.run_id


# ── CLI ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days-back", type=int, default=30,
                        help="how many days of gold history to pull (default 30)")
    parser.add_argument("--dry-run", action="store_true",
                        help="train + eval but don't log to MLflow")
    args = parser.parse_args()

    result = run_training(days_back=args.days_back, dry_run=args.dry_run)
    logger.info("=== result ===")
    for k, v in asdict(result).items():
        logger.info("  %s: %s", k, v)


# ── promotion logic ────────────────────────────────────────────────────────

REGISTERED_MODEL_NAME = "pulsetrade_iforest"
PROMOTION_THRESHOLD = 0.05  # candidate must beat champion by >=5% absolute on precision@k


def get_champion_precision() -> float | None:
    """
    Look up the precision_at_k of the current Production-stage model.
    Returns None if no Production model exists yet (first run).
    """
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    client = mlflow.MlflowClient()

    try:
        versions = client.get_latest_versions(REGISTERED_MODEL_NAME, stages=["Production"])
    except mlflow.exceptions.RestException as e:
        # RESOURCE_DOES_NOT_EXIST -> model registry has no entry yet
        logger.info("registered model not found yet: %s", e)
        return None

    if not versions:
        logger.info("no Production version yet; candidate will become first champion")
        return None

    champ = versions[0]
    run = client.get_run(champ.run_id)
    p = run.data.metrics.get("precision_at_k")
    logger.info("current Production: version=%s run_id=%s precision_at_k=%s",
                champ.version, champ.run_id, p)
    return p


def maybe_promote(candidate_run_id: str, candidate_precision: float) -> dict:
    """
    Compare candidate to current Production. If candidate wins by >= threshold,
    transition candidate to Production and archive prior Production.
    Returns a dict describing the decision (logged + used as XCom return value).
    """
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    client = mlflow.MlflowClient()

    champion_p = get_champion_precision()

    # Find the candidate version that maps to candidate_run_id.
    # log_model with registered_model_name creates a version, but we need the version number.
    candidate_versions = client.search_model_versions(
        f"name='{REGISTERED_MODEL_NAME}' AND run_id='{candidate_run_id}'"
    )
    if not candidate_versions:
        raise RuntimeError(f"no model version found for run_id {candidate_run_id}")
    candidate_version = candidate_versions[0].version

    decision = {
        "candidate_run_id": candidate_run_id,
        "candidate_version": candidate_version,
        "candidate_precision": candidate_precision,
        "champion_precision": champion_p,
        "threshold": PROMOTION_THRESHOLD,
    }

    # First-run case: no champion exists yet, promote unconditionally.
    if champion_p is None:
        client.transition_model_version_stage(
            name=REGISTERED_MODEL_NAME,
            version=candidate_version,
            stage="Production",
            archive_existing_versions=False,
        )
        decision["action"] = "promoted_first_champion"
        logger.info("PROMOTED v%s (first champion); precision_at_k=%.4f",
                    candidate_version, candidate_precision)
        return decision

    # Compare
    delta = candidate_precision - champion_p
    if delta >= PROMOTION_THRESHOLD:
        client.transition_model_version_stage(
            name=REGISTERED_MODEL_NAME,
            version=candidate_version,
            stage="Production",
            archive_existing_versions=True,  # auto-archive prior Production
        )
        decision["action"] = "promoted"
        decision["delta"] = delta
        logger.info("PROMOTED v%s; precision_at_k %.4f -> %.4f (delta=%.4f)",
                    candidate_version, champion_p, candidate_precision, delta)
    else:
        decision["action"] = "kept_champion"
        decision["delta"] = delta
        logger.info("KEPT champion; candidate v%s precision_at_k %.4f vs champion %.4f (delta=%.4f, threshold=%.4f)",
                    candidate_version, candidate_precision, champion_p, delta, PROMOTION_THRESHOLD)

    return decision
    
if __name__ == "__main__":
    main()