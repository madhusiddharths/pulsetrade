"""
Data access layer for the Streamlit dashboard.

Two key principles:
  1. EVERY query has a try/except wrapper that returns synthetic fallback
     data on failure. The dashboard must NEVER crash because Databricks
     is slow or Postgres is down — a dashboard that crashes when its source
     is sick is worse than one that shows obviously-stale-or-fake data.
  2. Streamlit's @st.cache_data is used aggressively (TTL 30s) so we don't
     hit Databricks on every UI interaction.

When a fallback kicks in, the caller sees `is_synthetic=True` in the returned
dict so it can display a banner like "showing synthetic data — Databricks
unavailable". That visibility is the whole point.
"""

from __future__ import annotations

import os
import random
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

import pandas as pd
import streamlit as st
from databricks import sql as dbsql
from databricks.sql.client import Connection
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# ── Config (env-driven) ─────────────────────────────────────────────────────

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "").removeprefix("https://").rstrip("/")
DATABRICKS_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
DATABRICKS_CATALOG = os.environ.get("DATABRICKS_CATALOG", "workspace")
DATABRICKS_SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "pulsetrade")

POSTGRES_URL = os.environ.get(
    "POSTGRES_URL",
    "postgresql+psycopg2://pulsetrade:pulsetrade_dev@localhost:5433/pulsetrade",
)

# Shorter timeout than the API uses — dashboard shouldn't hang on slow queries
DATABRICKS_TIMEOUT_SECONDS = 15

TICKERS = ["AAPL", "GOOGL", "MSFT", "NVDA", "TSLA"]


# ── Result wrapper ──────────────────────────────────────────────────────────

@dataclass
class QueryResult:
    """Wraps a DataFrame plus metadata about whether it came from a real query
    or a synthetic fallback. Caller checks .is_synthetic to render a banner."""
    df: pd.DataFrame
    is_synthetic: bool = False
    error: Optional[str] = None


# ── Databricks connection ───────────────────────────────────────────────────

@contextmanager
def _databricks_conn() -> Iterator[Connection]:
    if not DATABRICKS_HOST or not DATABRICKS_HTTP_PATH or not DATABRICKS_TOKEN:
        raise RuntimeError("Databricks env vars not set")
    conn = dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
        _socket_timeout=DATABRICKS_TIMEOUT_SECONDS,
    )
    try:
        yield conn
    finally:
        conn.close()


def _rows_to_df(cursor) -> pd.DataFrame:
    cols = [d[0] for d in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=cols)


# ── Postgres engine ─────────────────────────────────────────────────────────

_pg_engine: Optional[Engine] = None


def _pg() -> Engine:
    global _pg_engine
    if _pg_engine is None:
        _pg_engine = create_engine(POSTGRES_URL, pool_pre_ping=True, pool_size=2)
    return _pg_engine


# ── Synthetic data generators (fallbacks) ───────────────────────────────────

def _synthetic_gold(hours: int = 6) -> pd.DataFrame:
    """Plausible-looking OHLC data for when Databricks is unavailable.
    Generates a random-walk price for each ticker over `hours` of 5-min windows."""
    rows = []
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    now = now - timedelta(minutes=now.minute % 5)  # round down to 5-min boundary

    base_prices = {"AAPL": 220, "GOOGL": 175, "MSFT": 425, "NVDA": 138, "TSLA": 245}

    for ticker in TICKERS:
        price = base_prices[ticker]
        for i in range(hours * 12):  # 12 windows per hour
            window_start = now - timedelta(minutes=5 * (hours * 12 - i))
            # Small random walk
            change_pct = random.gauss(0, 0.002)
            new_price = price * (1 + change_pct)
            high = max(price, new_price) * (1 + abs(random.gauss(0, 0.001)))
            low = min(price, new_price) * (1 - abs(random.gauss(0, 0.001)))
            rows.append({
                "ticker": ticker,
                "window_start": window_start,
                "window_end": window_start + timedelta(minutes=5),
                "open_5min": price,
                "close_5min": new_price,
                "high_5min": high,
                "low_5min": low,
                "mean_price": (price + new_price) / 2,
                "mean_change_pct": change_pct,
                "mean_news_sentiment": random.gauss(0, 0.3),
                "news_article_count": random.randint(0, 5),
                "n_observations": random.randint(5, 12),
            })
            price = new_price
    return pd.DataFrame(rows)


def _synthetic_investigations() -> pd.DataFrame:
    """Fake investigation rows for when Postgres is unavailable."""
    now = datetime.now(timezone.utc)
    return pd.DataFrame([
        {
            "id": i,
            "ticker": random.choice(TICKERS),
            "anomaly_type": "price_spike",
            "window_start": now - timedelta(hours=random.randint(1, 24)),
            "report_markdown": (
                "## Summary\n_(Synthetic example — Postgres unavailable.)_\n\n"
                "## Evidence\nNo real data to display."
            ),
            "created_at": now - timedelta(hours=random.randint(1, 24)),
        }
        for i in range(1, 6)
    ])


# ── Public query API ────────────────────────────────────────────────────────

@st.cache_data(ttl=30, show_spinner=False)
def get_recent_gold(hours: int = 6) -> QueryResult:
    """Pull last `hours` of gold for all tickers. Falls back to synthetic on error."""
    query = f"""
        SELECT
            ticker, window_start, window_end,
            open_5min, close_5min, high_5min, low_5min,
            mean_price, mean_change_pct,
            mean_news_sentiment, news_article_count,
            n_observations
        FROM {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.gold_5min_features
        WHERE window_start >= current_timestamp() - INTERVAL {int(hours)} HOURS
        ORDER BY ticker, window_start
    """
    try:
        with _databricks_conn() as conn, conn.cursor() as cur:
            cur.execute(query)
            df = _rows_to_df(cur)
        if df.empty:
            # Not an error — Databricks responded, just no rows yet.
            return QueryResult(df=df, is_synthetic=False)
        return QueryResult(df=df, is_synthetic=False)
    except Exception as e:
        return QueryResult(
            df=_synthetic_gold(hours=hours),
            is_synthetic=True,
            error=str(e)[:200],
        )

@st.cache_data(ttl=30, show_spinner=False)
def get_ticker_gold(ticker: str, hours: int = 6) -> QueryResult:
    """
    Pull last `hours` of gold for a single ticker. Same fallback pattern
    as get_recent_gold — synthetic data on failure.
    """
    result = get_recent_gold(hours=hours)
    if result.df.empty:
        return result
    filtered = result.df[result.df["ticker"] == ticker].copy()
    return QueryResult(
        df=filtered.reset_index(drop=True),
        is_synthetic=result.is_synthetic,
        error=result.error,
    )


@st.cache_data(ttl=30, show_spinner=False)
def get_latest_per_ticker() -> QueryResult:
    """
    Latest row per ticker for the summary cards at top of Live Prices page.
    Reads the last hour's worth of gold and keeps only the most recent
    window per ticker.
    """
    result = get_recent_gold(hours=1)
    if result.df.empty:
        return result
    latest = (
        result.df
        .sort_values("window_start")
        .groupby("ticker")
        .tail(1)
        .reset_index(drop=True)
    )
    return QueryResult(
        df=latest,
        is_synthetic=result.is_synthetic,
        error=result.error,
    )

@st.cache_data(ttl=60, show_spinner=False)
def get_investigations(limit: int = 50) -> QueryResult:
    """Recent investigations from Postgres. Falls back to synthetic on error."""
    sql = text("""
        SELECT id, ticker, anomaly_type, window_start,
               report_markdown, agent_thoughts, created_at
        FROM investigations
        ORDER BY created_at DESC
        LIMIT :limit
    """)
    try:
        with _pg().connect() as conn:
            df = pd.read_sql(sql, conn, params={"limit": limit})
        return QueryResult(df=df, is_synthetic=False)
    except Exception as e:
        return QueryResult(
            df=_synthetic_investigations(),
            is_synthetic=True,
            error=str(e)[:200],
        )


@st.cache_data(ttl=30, show_spinner=False)
def get_news_sentiment(hours: int = 24) -> QueryResult:
    """
    Per-ticker sentiment time series from gold. Mean sentiment + article count.
    Reuses the gold table since we already aggregated sentiment per window.
    """
    result = get_recent_gold(hours=hours)
    if result.df.empty:
        return result
    # Filter to just the columns this page needs
    cols = ["ticker", "window_start", "mean_news_sentiment", "news_article_count"]
    available = [c for c in cols if c in result.df.columns]
    return QueryResult(
        df=result.df[available].copy(),
        is_synthetic=result.is_synthetic,
        error=result.error,
    )