"""
Day 6 Block 4c — Nightly model retrain with conditional MLflow promotion.

Schedule: 0 2 * * *  (2 AM UTC daily)

Flow:
  1. train_model    — retrain Isolation Forest on rolling 30-day gold window
  2. decide_promote — compare new model's precision@k to current Production
                       champion. Promote if candidate beats by >=5% (or if
                       no champion exists yet — first run case).

Failure modes:
  - Insufficient gold data -> train_model returns a "skipped" result and
    decide_promote becomes a no-op. DAG run is green either way; the
    skipped_reason is in the MLflow run tags.
  - MLflow auth fails -> hard fail in train_model. Visible immediately.
  - Promotion conflict (someone manually moved a version) -> hard fail in
    decide_promote so we don't accidentally undo a manual override.
"""

from datetime import datetime, timedelta
import sys

from airflow.decorators import dag, task

# Mount api/ into Python path so we can import the training module
_API_PATH = "/opt/pulsetrade-api"
if _API_PATH not in sys.path:
    sys.path.insert(0, _API_PATH)


@dag(
    dag_id="pulsetrade_nightly_retrain",
    description="Retrain Isolation Forest nightly; conditionally promote via MLflow",
    schedule="0 2 * * *",
    start_date=datetime(2026, 5, 7),
    catchup=False,
    max_active_runs=1,
    tags=["pulsetrade", "day6", "ml"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
)
def nightly_retrain():

    @task
    def train_model() -> dict:
        """
        Train Isolation Forest, eval, log to MLflow. Returns a dict with
        run_id + metrics for the downstream promotion task.
        """
        from agent.model_training import run_training
        from dataclasses import asdict

        result = run_training(days_back=30, dry_run=False)
        out = asdict(result)
        out["run_id"] = _last_run_id()  # populated by run_training's mlflow.start_run
        return out

    @task
    def decide_promote(training_result: dict) -> dict:
        """
        If we have a real precision_at_k (training wasn't skipped),
        decide whether to promote.
        """
        if training_result.get("skipped_reason"):
            return {
                "action": "skipped",
                "reason": training_result["skipped_reason"],
            }

        from agent.model_training import maybe_promote
        return maybe_promote(
            candidate_run_id=training_result["run_id"],
            candidate_precision=training_result["precision_at_k"],
        )

    decide_promote(train_model())


def _last_run_id() -> str:
    """
    Helper to grab the run_id of the most-recently completed MLflow run
    in the experiment. We use this because run_training() doesn't currently
    return the run_id directly — keeps the training module decoupled from
    DAG plumbing.
    """
    import mlflow
    from config import settings

    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    client = mlflow.MlflowClient()
    exp = client.get_experiment_by_name(settings.mlflow_experiment_name)
    if exp is None:
        raise RuntimeError(f"experiment not found: {settings.mlflow_experiment_name}")
    runs = client.search_runs(
        experiment_ids=[exp.experiment_id],
        order_by=["start_time DESC"],
        max_results=1,
    )
    if not runs:
        raise RuntimeError("no runs found in experiment; training may not have logged")
    return runs[0].info.run_id


nightly_retrain()