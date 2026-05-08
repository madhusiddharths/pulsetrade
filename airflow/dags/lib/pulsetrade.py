"""
Helpers for PulseTrade DAGs.

Why this lives in airflow/dags/lib/ and not api/:
  - DAGs run in the Airflow container; they don't have access to api/agent/
  - We can't easily install api/ as a package inside Airflow without a custom image
  - Easier to duplicate a few lines of glue code than wire up cross-container imports

The detector itself IS still imported via Python path manipulation (see run_detector
in the DAG). That works because anomaly_detector.py only depends on data/databricks.py
and data/postgres.py — both lightweight imports. If the import chain grows, switch
to a custom Airflow image with api/ pip-installed.
"""

import logging
import os
from typing import Optional

import requests
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_engine: Optional[Engine] = None


def get_engine() -> Engine:
    """
    SQLAlchemy engine for the investigations Postgres.
    Built from the env vars set in docker-compose.yml.
    """
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


def post_investigate(
    ticker: str,
    anomaly_type: str,
    lookback_minutes: int = 360,
    timeout_seconds: int = 60,
) -> dict:
    """
    Hit the FastAPI /investigate endpoint synchronously.
    Returns the parsed JSON response. Raises on HTTP error.
    """
    url = os.environ["PULSETRADE_API_URL"] + "/investigate"
    body = {
        "ticker": ticker,
        "anomaly_type": anomaly_type,
        "lookback_minutes": lookback_minutes,
    }
    logger.info("POST %s body=%s", url, body)
    resp = requests.post(url, json=body, timeout=timeout_seconds)
    resp.raise_for_status()
    return resp.json()