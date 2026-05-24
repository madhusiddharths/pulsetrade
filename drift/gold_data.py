# drift/gold_data.py
"""
Data layer for drift detection — SELF-CONTAINED (does not import from api/).

Why self-contained: this package will run inside an Airflow worker in Block 3,
a separate deployable unit from the FastAPI service. Duplicating ~15 lines of
connection setup keeps the two independent. The connection logic below is
copied from api/data/databricks.py intentionally.

THE KEY DESIGN PRINCIPLE — SWAPPABLE DATA SOURCE:
    get_reference_window()  → the "baseline" distribution
    get_current_window()    → the "current" distribution to check for drift

Today, with only ~10 days of intermittently-collected data, we can't do real
week-over-week comparison (the windows wouldn't be comparable — see the
"comparability" lesson). So get_current_window(inject_drift=True) takes real
data and artificially shifts the sentiment feature to DEMONSTRATE detection.

When enough clean production history accumulates, flip inject_drift=False and
change the two window functions to pull real time-separated windows. Nothing
else in the pipeline changes.
"""
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd
from dotenv import load_dotenv
from databricks import sql
from databricks.sql.client import Connection

# Load project .env so DATABRICKS_* vars are available when run standalone.
load_dotenv(Path(__file__).resolve().parent.parent / ".env")
# ── Feature columns we drift-check (confirmed against DESCRIBE output) ────────
# Excludes: ticker (id), window_start/end & gold_processed_at (timestamps),
# n_observations (operational counter, not a feature).
FEATURE_COLUMNS = [
    "open_5min",
    "close_5min",
    "high_5min",
    "low_5min",
    "mean_price",
    "price_stddev",
    "mean_change_pct",
    "mean_intraday_range",
    "mean_news_sentiment",
    "news_article_count",
]

# The feature we inject drift into for the demo. Sentiment drift is a REAL
# failure mode for PulseTrade: if FinBERT's behavior shifts or news tone
# changes systematically (e.g. market crash → everything negative), the
# anomaly detector's sentiment rules silently break.
DRIFT_TARGET_COLUMN = "mean_news_sentiment"


# ── Connection (env var names matched to the real .env) ──────────────────────
def _hostname() -> str:
    """Strip scheme from the host — the connector wants a bare hostname."""
    h = os.environ["DATABRICKS_HOST"]
    return h.removeprefix("https://").removeprefix("http://").rstrip("/")


@contextmanager
def get_connection() -> Iterator[Connection]:
    """Context manager for a Databricks SQL connection."""
    conn = sql.connect(
        server_hostname=_hostname(),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],      # ← was DATABRICKS_HTTP_PATH
        access_token=os.environ["DATABRICKS_TOKEN"],  # ← confirm spelling (see note)
    )
    try:
        yield conn
    finally:
        conn.close()


# Catalog/schema are not in .env — they're defaulted in api/config.py.
# Hardcoded here since the drift package is self-contained and these values
# are stable. Confirm against `grep catalog api/config.py`.
DATABRICKS_CATALOG = "workspace"
DATABRICKS_SCHEMA = "pulsetrade"


def _table() -> str:
    """Fully-qualified table name (catalog.schema.table)."""
    return f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.gold_5min_features"


# ── Core data access ─────────────────────────────────────────────────────────
def fetch_gold_dataframe(limit: Optional[int] = None) -> pd.DataFrame:
    """
    Pull gold feature rows into a pandas DataFrame.

    Pools ALL tickers — with only 88 rows/ticker, per-ticker drift tests would
    be statistically weak. We're checking whether the FEATURE distribution
    drifted, not whether one ticker drifted, so pooling is correct.

    Returns a DataFrame with exactly FEATURE_COLUMNS (plus ticker for context,
    dropped before drift analysis).
    """
    cols = ", ".join(FEATURE_COLUMNS)
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
    SELECT ticker, {cols}
    FROM {_table()}
    ORDER BY window_start
    {limit_clause}
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        col_names = [d[0] for d in cur.description]
        rows = [dict(zip(col_names, row)) for row in cur.fetchall()]

    df = pd.DataFrame(rows)
    # Ensure numeric dtypes — the connector sometimes returns Decimal for
    # doubles, which Evidently's stat tests don't like.
    for c in FEATURE_COLUMNS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# ── Swappable window functions ───────────────────────────────────────────────
def _split_shuffled(df: pd.DataFrame, seed: int = 42) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Shuffle then split 50/50.

    Shuffle-before-split is CRITICAL (the iris lesson): it guarantees both
    halves are the same population, so any drift we then detect is the drift
    we INJECTED, not an artifact of how the rows were ordered in the table.
    """
    shuffled = df.sample(frac=1.0, random_state=seed).reset_index(drop=True)
    mid = len(shuffled) // 2
    return shuffled.iloc[:mid].copy(), shuffled.iloc[mid:].copy()


def get_reference_window() -> pd.DataFrame:
    """The baseline half — untouched real data, features only."""
    full = fetch_gold_dataframe()
    reference, _ = _split_shuffled(full)
    return reference[FEATURE_COLUMNS].copy()


def get_current_window(inject_drift: bool = True) -> pd.DataFrame:
    """
    The 'current' half to check for drift.

    inject_drift=True  (demo mode): shift the sentiment feature so Evidently
                       has real drift to catch. Honest demonstration of the
                       detection capability on the real schema.
    inject_drift=False (future production mode): return clean real data. You'd
                       pair this with window functions that pull genuinely
                       time-separated windows (this week vs last week).
    """
    full = fetch_gold_dataframe()
    _, current = _split_shuffled(full)
    current = current[FEATURE_COLUMNS].copy()

    if inject_drift:
        # Shift sentiment strongly negative + compress its spread, simulating
        # a regime where news turned systematically bearish. Large enough that
        # the K-S test will clearly flag it.
        current[DRIFT_TARGET_COLUMN] = current[DRIFT_TARGET_COLUMN] * 0.4 - 0.5

    return current


if __name__ == "__main__":
    # Quick connectivity + shape check, no Evidently yet.
    ref = get_reference_window()
    cur = get_current_window(inject_drift=True)
    print(f"[gold_data] reference: {ref.shape}, current: {cur.shape}")
    print(f"[gold_data] feature columns: {list(ref.columns)}")
    print(f"[gold_data] reference sentiment mean: {ref[DRIFT_TARGET_COLUMN].mean():.4f}")
    print(f"[gold_data] current  sentiment mean: {cur[DRIFT_TARGET_COLUMN].mean():.4f} (drift injected)")