"""
Day 6 Block 3 — Anomaly sweep DAG.

Every 5 minutes:
  1. run_detector       — z-score detector inserts new flags into anomaly_queue
  2. claim_batch        — atomically claim up to N pending rows -> 'processing'
  3. investigate_one*   — dynamic task mapping; one task per claimed anomaly
                          (calls FastAPI /investigate, updates queue row)

Schedule: */5 * * * *  (matches gold table cadence)

Failure modes:
  - Detector errors -> DAG fails fast, no anomalies claimed; safe to retry.
  - claim_batch returns [] -> downstream skips automatically.
  - investigate_one fails -> that anomaly stays in 'processing' forever
    until you reset it. Manual reset query at the bottom of this file.
"""

from datetime import datetime, timedelta
import sys
from pathlib import Path

from airflow.decorators import dag, task

# Import the detector. The api/ folder is bind-mounted in docker-compose
# at /opt/airflow/dags/lib/... wait, it's not. We import from a local helper.
from lib.pulsetrade import get_engine, post_investigate


# Path setup: mount api/agent and api/data so we can import the detector.
# Done at DAG-parse time so all tasks inherit it.
_API_PATH = "/opt/pulsetrade-api"
if _API_PATH not in sys.path:
    sys.path.insert(0, _API_PATH)


@dag(
    dag_id="pulsetrade_anomaly_sweep",
    description="Detect price anomalies in gold and trigger investigations",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 5, 7),
    catchup=False,
    max_active_runs=1,
    tags=["pulsetrade", "day6"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(minutes=10),
    },
)
def anomaly_sweep():

    @task
    def run_detector() -> int:
        """Run z-score detector. Returns count of new rows inserted to queue."""
        # Lazy import: keeps module-level parsing fast in scheduler
        from agent.anomaly_detector import detect_anomalies, insert_anomalies
        anomalies = detect_anomalies(lookback_minutes=24 * 60)
        inserted = insert_anomalies(anomalies)
        return inserted

    @task
    def claim_batch(_inserted: int) -> list[dict]:
        """
        Claim up to 20 pending anomalies; flip them to 'processing'.
        The _inserted parameter is just a dependency signal — we don't use the value.
        """
        from data.anomaly_queue import claim_pending
        engine = get_engine()
        claimed = claim_pending(engine, limit=20)
        # Convert datetime fields to ISO strings for XCom-safe serialization
        return [
            {
                "id": r["id"],
                "ticker": r["ticker"],
                "anomaly_type": r["anomaly_type"],
                "window_start": r["window_start"].isoformat(),
                "z_score": r["z_score"],
            }
            for r in claimed
        ]

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def investigate_one(claimed: dict) -> dict:
        """
        Run one investigation. Calls FastAPI /investigate, updates queue row.
        Each mapped instance gets its own claimed anomaly dict.
        """
        from data.anomaly_queue import mark_done, mark_failed
        engine = get_engine()
        try:
            resp = post_investigate(
                ticker=claimed["ticker"],
                anomaly_type=claimed["anomaly_type"],
                lookback_minutes=360,
                timeout_seconds=120,
            )
            mark_done(
                engine=engine,
                queue_id=claimed["id"],
                investigation_id=resp["investigation_id"],
            )
            return {
                "queue_id": claimed["id"],
                "investigation_id": resp["investigation_id"],
                "ticker": claimed["ticker"],
                "iterations": resp.get("iterations"),
                "tool_calls": resp.get("tool_calls"),
            }
        except Exception as e:
            mark_failed(engine=engine, queue_id=claimed["id"])
            raise

    inserted = run_detector()
    claimed = claim_batch(inserted)
    investigate_one.expand(claimed=claimed)


anomaly_sweep()


# ─────────────────────────────────────────────────────────────────────────────
# Manual cleanup queries (paste into psql when needed)
#
#  -- Reset stuck 'processing' rows older than 30 min back to 'pending':
#  UPDATE anomaly_queue
#  SET status = 'pending'
#  WHERE status = 'processing' AND detected_at < NOW() - INTERVAL '30 minutes';
#
#  -- Wipe a flood of bad anomalies (during testing):
#  DELETE FROM anomaly_queue WHERE detected_at < NOW() - INTERVAL '1 hour';
#
#  -- See queue state:
#  SELECT status, COUNT(*) FROM anomaly_queue GROUP BY status;
# ─────────────────────────────────────────────────────────────────────────────