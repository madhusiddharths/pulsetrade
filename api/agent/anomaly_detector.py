"""
Z-score based anomaly detector for PulseTrade gold features.

Reads recent windows from gold_5min_features in Databricks, computes per-ticker
z-scores against a rolling baseline, and inserts flagged windows into the
anomaly_queue table in Postgres.

Designed to be:
  - Standalone runnable: `python -m agent.anomaly_detector`
  - Importable from an Airflow DAG (see Block 3)

The detection rule is intentionally simple. It is the *trigger*, not the model.
The Isolation Forest / MLflow pipeline in Block 4 is a separate, parallel detector
used for nightly model evaluation and as a stronger second-stage filter.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Allow running as a script from anywhere
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text
from data.databricks import get_all_gold_for_detection
from data.postgres import get_engine

logger = logging.getLogger("anomaly_detector")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")


# --- config -----------------------------------------------------------------

# What counts as anomalous. Tune later. 2.5 is roughly "1-in-100 windows."
DEFAULT_Z_THRESHOLD = 2.5

# How many recent windows to consider as candidates for flagging.
# 5-min windows × 12 = last hour.
DEFAULT_RECENT_WINDOWS = 12

# How many windows to use as the baseline for mean/stddev.
# 5-min windows × 288 = last 24 hours.
DEFAULT_BASELINE_WINDOWS = 288

# Minimum number of baseline windows required before we trust the stats.
# Prevents flagging anomalies during cold-start when stddev is unstable.
MIN_BASELINE_N = 30


# --- types ------------------------------------------------------------------

@dataclass
class Anomaly:
    ticker: str
    anomaly_type: str
    window_start: datetime
    window_end: datetime
    z_score: float
    trigger_value: float
    baseline_mean: float
    baseline_stddev: float


# --- detection --------------------------------------------------------------

def _fetch_gold_for_detection(
    lookback_minutes: int,
) -> list[dict]:
    """
    Pull recent gold windows for all tickers across the lookback window.
    Delegates to data.databricks for the actual query — keeps SQL in one place.
    """
    rows = get_all_gold_for_detection(lookback_minutes)
    logger.info("fetched %d gold rows (lookback=%d min)", len(rows), lookback_minutes)
    return rows


def _detect_for_ticker(
    ticker: str,
    rows: list[dict],
    z_threshold: float,
    recent_n: int,
    baseline_n: int,
) -> list[Anomaly]:
    """
    Run z-score detection on a single ticker's rows (already sorted by window_start).
    """
    if len(rows) < MIN_BASELINE_N:
        logger.info("%s: only %d windows, skipping (need %d)", ticker, len(rows), MIN_BASELINE_N)
        return []

    # Baseline = all but the most recent N windows
    baseline = rows[:-recent_n] if len(rows) > recent_n else []
    recent = rows[-recent_n:]

    if len(baseline) < MIN_BASELINE_N:
        logger.info(
            "%s: baseline too small (%d < %d), skipping",
            ticker, len(baseline), MIN_BASELINE_N,
        )
        return []

    # Detect on mean_change_pct (price spikes) — the most useful signal we have.
    # You could fan out to mean_intraday_range, mean_news_sentiment, etc.
    baseline_vals = [r["mean_change_pct"] for r in baseline if r["mean_change_pct"] is not None]
    if len(baseline_vals) < MIN_BASELINE_N:
        return []

    baseline_mean = sum(baseline_vals) / len(baseline_vals)
    var = sum((v - baseline_mean) ** 2 for v in baseline_vals) / len(baseline_vals)
    baseline_stddev = var ** 0.5

    if baseline_stddev < 1e-9:
        # All baseline values identical — any non-zero recent change is "anomalous"
        # but stats are degenerate. Skip rather than spam the queue.
        logger.info("%s: baseline stddev ~ 0, skipping", ticker)
        return []

    anomalies = []
    for r in recent:
        val = r.get("mean_change_pct")
        if val is None:
            continue
        z = (val - baseline_mean) / baseline_stddev
        if abs(z) >= z_threshold:
            anomalies.append(Anomaly(
                ticker=ticker,
                anomaly_type="price_spike",
                window_start=r["window_start"],
                window_end=r["window_end"],
                z_score=round(z, 3),
                trigger_value=round(val, 4),
                baseline_mean=round(baseline_mean, 4),
                baseline_stddev=round(baseline_stddev, 4),
            ))
            logger.info(
                "%s flagged: window=%s z=%.2f val=%.4f baseline_mean=%.4f stddev=%.4f",
                ticker, r["window_start"].isoformat(), z, val, baseline_mean, baseline_stddev,
            )

    return anomalies


def detect_anomalies(
    lookback_minutes: int = 24 * 60,
    z_threshold: float = DEFAULT_Z_THRESHOLD,
    recent_n: int = DEFAULT_RECENT_WINDOWS,
    baseline_n: int = DEFAULT_BASELINE_WINDOWS,
) -> list[Anomaly]:
    """
    Top-level entry point. Pulls gold, runs per-ticker detection, returns flagged.
    """
    rows = _fetch_gold_for_detection(lookback_minutes)
    if not rows:
        logger.warning("no gold rows returned; nothing to detect")
        return []

    by_ticker: dict[str, list[dict]] = {}
    for r in rows:
        by_ticker.setdefault(r["ticker"], []).append(r)

    all_anomalies: list[Anomaly] = []
    for ticker, ticker_rows in by_ticker.items():
        flagged = _detect_for_ticker(
            ticker=ticker,
            rows=ticker_rows,
            z_threshold=z_threshold,
            recent_n=recent_n,
            baseline_n=baseline_n,
        )
        all_anomalies.extend(flagged)

    logger.info("detection complete: %d anomalies flagged across %d tickers",
                len(all_anomalies), len(by_ticker))
    return all_anomalies


# --- queue insertion --------------------------------------------------------

def insert_anomalies(anomalies: list[Anomaly]) -> int:
    """
    Bulk insert into anomaly_queue. Idempotent on (ticker, window_start, anomaly_type)
    via the unique index — re-runs on overlapping data are no-ops.
    Returns number of NEW rows inserted (not re-inserted duplicates).
    """
    if not anomalies:
        return 0

    sql = text("""
        INSERT INTO anomaly_queue
          (ticker, anomaly_type, window_start, window_end,
           z_score, trigger_value, baseline_mean, baseline_stddev, status)
        VALUES
          (:ticker, :anomaly_type, :window_start, :window_end,
           :z_score, :trigger_value, :baseline_mean, :baseline_stddev, 'pending')
        ON CONFLICT (ticker, window_start, anomaly_type) DO NOTHING
    """)

    params = [
        {
            "ticker": a.ticker,
            "anomaly_type": a.anomaly_type,
            "window_start": a.window_start,
            "window_end": a.window_end,
            "z_score": a.z_score,
            "trigger_value": a.trigger_value,
            "baseline_mean": a.baseline_mean,
            "baseline_stddev": a.baseline_stddev,
        }
        for a in anomalies
    ]

    eng = get_engine()
    with eng.begin() as conn:
        result = conn.execute(sql, params)
        inserted = result.rowcount

    logger.info(
        "inserted %d new anomalies into anomaly_queue (deduped %d)",
        inserted, len(anomalies) - inserted,
    )
    return inserted


# --- CLI --------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback-minutes", type=int, default=24 * 60,
                        help="how far back to fetch gold (default 1440 = 24h)")
    parser.add_argument("--z-threshold", type=float, default=DEFAULT_Z_THRESHOLD)
    parser.add_argument("--recent-windows", type=int, default=DEFAULT_RECENT_WINDOWS,
                        help="how many recent windows to check for anomalies (default 12 = 1h)")
    parser.add_argument("--dry-run", action="store_true",
                        help="detect but don't insert into queue")
    args = parser.parse_args()

    anomalies = detect_anomalies(
        lookback_minutes=args.lookback_minutes,
        z_threshold=args.z_threshold,
        recent_n=args.recent_windows,
    )

    if args.dry_run:
        logger.info("--dry-run: would insert %d anomalies", len(anomalies))
        for a in anomalies:
            logger.info("  %s @ %s: z=%.2f val=%.4f",
                        a.ticker, a.window_start.isoformat(), a.z_score, a.trigger_value)
        return

    inserted = insert_anomalies(anomalies)
    logger.info("done: %d new rows in anomaly_queue", inserted)


if __name__ == "__main__":
    main()