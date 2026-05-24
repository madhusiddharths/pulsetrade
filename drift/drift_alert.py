# drift/drift_alert.py
"""
Drift alert persistence — writes drift detection results to the application
Postgres (the same DB holding `investigations` and `anomaly_queue`).

Connection pattern MATCHES airflow/dags/lib/pulsetrade.py:
  - SQLAlchemy engine (not raw psycopg2)
  - PULSETRADE_PG_* env vars (the DAG-side naming convention)

These env vars are injected into the drift container by the DockerOperator
in the DAG. When run standalone (docker run / local), they come from .env.
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_engine: Optional[Engine] = None

# ── Alert thresholds (both conditions trigger an alert) ──────────────────────
# Share-based: alert if this fraction (or more) of columns drifted.
SHARE_THRESHOLD = 0.30
# Key-feature-based: alert if ANY of these specific columns drifted, even if
# overall share is below the threshold. These are the features whose drift
# most directly degrades the agent / detector.
KEY_FEATURES = ["mean_news_sentiment", "mean_change_pct"]


def get_engine() -> Engine:
    """SQLAlchemy engine for the application Postgres (same as pulsetrade.py)."""
    global _engine
    if _engine is None:
        url = (
            f"postgresql+psycopg2://{os.environ['PULSETRADE_PG_USER']}:"
            f"{os.environ['PULSETRADE_PG_PASSWORD']}@"
            f"{os.environ['PULSETRADE_PG_HOST']}:{os.environ['PULSETRADE_PG_PORT']}/"
            f"{os.environ['PULSETRADE_PG_DB']}"
        )
        _engine = create_engine(url, pool_pre_ping=True, pool_size=2, max_overflow=2)
    return _engine


def init_drift_table() -> None:
    """
    Create the drift_alerts table if it doesn't exist.

    Note p_values are stored in a JSONB blob; double precision is fine for the
    tiny scientific-notation values (e.g. 3.59e-10) the K-S test produces.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS drift_alerts (
        id              SERIAL PRIMARY KEY,
        run_at          TIMESTAMPTZ NOT NULL,
        n_columns       INTEGER NOT NULL,
        n_drifted       INTEGER NOT NULL,
        share_drifted   DOUBLE PRECISION NOT NULL,
        drifted_columns JSONB NOT NULL,
        per_column      JSONB NOT NULL,
        alerted         BOOLEAN NOT NULL,
        alert_reason    TEXT,
        report_path     TEXT,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    with get_engine().begin() as conn:
        conn.execute(text(ddl))
    logger.info("drift_alerts table ready")


def evaluate_alert(summary: dict) -> tuple[bool, str]:
    """
    Decide whether this drift result warrants an alert.

    Returns (should_alert, reason). Both conditions are checked:
      - share of drifted columns >= SHARE_THRESHOLD
      - any KEY_FEATURE drifted
    """
    reasons = []

    if summary["share_drifted"] >= SHARE_THRESHOLD:
        reasons.append(
            f"share {summary['share_drifted']:.2f} >= {SHARE_THRESHOLD}"
        )

    drifted_keys = [c for c in KEY_FEATURES if c in summary["drifted_columns"]]
    if drifted_keys:
        reasons.append(f"key feature(s) drifted: {', '.join(drifted_keys)}")

    should_alert = len(reasons) > 0
    return should_alert, "; ".join(reasons) if reasons else "no drift above thresholds"


def write_drift_alert(summary: dict, report_path: str = "") -> int:
    """
    Persist a drift run to drift_alerts. Always writes a row (so we have a
    full history of every run), with `alerted` flagging whether it crossed
    a threshold. Returns the new row id.
    """
    should_alert, reason = evaluate_alert(summary)

    row = {
        "run_at": datetime.now(timezone.utc),
        "n_columns": summary["n_columns"],
        "n_drifted": summary["n_drifted"],
        "share_drifted": summary["share_drifted"],
        "drifted_columns": json.dumps(summary["drifted_columns"]),
        "per_column": json.dumps(summary["columns"], default=str),
        "alerted": should_alert,
        "alert_reason": reason,
        "report_path": report_path,
    }

    insert = text("""
        INSERT INTO drift_alerts
            (run_at, n_columns, n_drifted, share_drifted, drifted_columns,
             per_column, alerted, alert_reason, report_path)
        VALUES
            (:run_at, :n_columns, :n_drifted, :share_drifted, :drifted_columns,
             :per_column, :alerted, :alert_reason, :report_path)
        RETURNING id
    """)
    with get_engine().begin() as conn:
        result = conn.execute(insert, row)
        new_id = result.scalar_one()

    level = logging.WARNING if should_alert else logging.INFO
    logger.log(level, "drift_alert #%s written — alerted=%s (%s)",
               new_id, should_alert, reason)
    return new_id