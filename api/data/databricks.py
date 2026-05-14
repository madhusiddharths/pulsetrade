# api/data/databricks.py
"""
Databricks SQL client — reads gold/silver Delta tables.

Why SQL Connector and not PySpark?
    - The agent runs in a small FastAPI process, not a Spark cluster
    - We just need to query existing tables, not transform data
    - SQL Connector is ~10MB, PySpark would be ~300MB
    - Connections are managed via context managers; cursor is per-call
"""

from contextlib import contextmanager
from datetime import datetime
from typing import Iterator

from databricks import sql
from databricks.sql.client import Connection

from config import settings
from typing import Iterator, Optional


def _hostname() -> str:
    """Strip https:// from DATABRICKS_HOST — connector wants bare hostname."""
    h = settings.databricks_host
    return h.removeprefix("https://").removeprefix("http://").rstrip("/")


@contextmanager
def get_connection() -> Iterator[Connection]:
    """
    Context manager for a Databricks SQL connection.

    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                print(cur.fetchall())
    """
    conn = sql.connect(
        server_hostname=_hostname(),
        http_path=settings.databricks_http_path,
        access_token=settings.databricks_token,
    )
    try:
        yield conn
    finally:
        conn.close()


def _rows_to_dicts(cursor) -> list[dict]:
    """Convert cursor results into list[dict] keyed by column name."""
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ─────────────────────────────────────────────────────────────────────────────
# Public query functions — these are what the agent's nodes/tools will call
# ─────────────────────────────────────────────────────────────────────────────
def get_recent_gold(ticker: str, lookback_minutes: int = 30) -> list[dict]:
    """
    Pull gold_5min_features rows for a ticker over the last N minutes.

    Returns most-recent-first list of dicts with all gold columns.
    """
    cat = settings.databricks_catalog
    sch = settings.databricks_schema

    query = f"""
    SELECT
        ticker, window_start, window_end, n_observations,
        open_5min, close_5min, high_5min, low_5min,
        mean_price, price_stddev, mean_change_pct, mean_intraday_range,
        mean_news_sentiment, news_article_count, gold_processed_at
    FROM {cat}.{sch}.gold_5min_features
    WHERE ticker = ?
      AND window_start >= current_timestamp() - INTERVAL {int(lookback_minutes)} MINUTES
    ORDER BY window_start DESC
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, (ticker,))
        return _rows_to_dicts(cur)


def get_news_for_window(
    ticker: str,
    start: datetime,
    end: datetime,
    limit: int = 20,
) -> list[dict]:
    """
    Pull silver_market_news articles for a ticker between [start, end].
    Returns most-recent-first; capped at `limit`.
    """
    cat = settings.databricks_catalog
    sch = settings.databricks_schema

    query = f"""
    SELECT
        article_id, ticker, is_primary_ticker,
        title, description, source_name, url, category,
        published_at, ingested_at,
        sentiment_label, sentiment_score
    FROM {cat}.{sch}.silver_market_news
    WHERE ticker = ?
      AND ingested_at BETWEEN ? AND ?
    ORDER BY ingested_at DESC
    LIMIT {int(limit)}
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, (ticker, start, end))
        return _rows_to_dicts(cur)

def get_all_gold_for_detection(lookback_minutes: int = 24 * 60) -> list[dict]:
    """
    Pull gold_5min_features for ALL tickers over the last N minutes.

    Used by the anomaly detector (and future batch jobs) which need a baseline
    per ticker and don't know the ticker list ahead of time. Pulls only the
    columns needed for z-score detection — keeps result size manageable.

    Returns ordered by (ticker, window_start) for easy per-ticker grouping.
    """
    cat = settings.databricks_catalog
    sch = settings.databricks_schema

    query = f"""
    SELECT
        ticker,
        window_start,
        window_end,
        mean_price,
        mean_change_pct,
        mean_intraday_range,
        n_observations
    FROM {cat}.{sch}.gold_5min_features
    WHERE window_end >= current_timestamp() - INTERVAL {int(lookback_minutes)} MINUTES
    ORDER BY ticker, window_start
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        return _rows_to_dicts(cur)

def get_gold_window(
    days_back: int = 30,
    end_time: Optional[datetime] = None,
) -> list[dict]:
    """
    Pull all gold_5min_features rows over a [days_back] window ending at end_time.

    Used by the nightly Isolation Forest training job — needs a longer history
    than the realtime detector. If end_time is None, defaults to NOW.

    Returns ordered by (ticker, window_start) for predictable feature engineering.
    """
    cat = settings.databricks_catalog
    sch = settings.databricks_schema

    end_clause = (
        f"AND window_start <= TIMESTAMP'{end_time.isoformat()}'"
        if end_time is not None
        else ""
    )

    query = f"""
    SELECT
        ticker,
        window_start,
        window_end,
        mean_price,
        price_stddev,
        mean_change_pct,
        mean_intraday_range,
        mean_news_sentiment,
        news_article_count,
        n_observations
    FROM {cat}.{sch}.gold_5min_features
    WHERE window_start >= current_timestamp() - INTERVAL {int(days_back)} DAYS
      {end_clause}
    ORDER BY ticker, window_start
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        return _rows_to_dicts(cur)

def healthcheck() -> dict:
    """
    Quick reachability check used by /ready endpoint.
    Returns {"ok": True} or {"ok": False, "error": "..."}.
    """
    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            row = cur.fetchone()
        return {"ok": row[0] == 1}
    except Exception as e:
        return {"ok": False, "error": str(e)}