"""
Client for the PulseTrade FastAPI service (/investigate, /health).
Used only by the trigger panel (Block 4). Synchronous — the dashboard
will show a spinner while the ReAct agent runs (~15-30s).
"""

import os
from typing import Optional

import requests


API_URL = os.environ.get("PULSETRADE_API_URL", "http://localhost:8000")


def health() -> tuple[bool, Optional[str]]:
    """Returns (is_healthy, error_message_or_none)."""
    try:
        resp = requests.get(f"{API_URL}/health", timeout=3)
        return resp.status_code == 200, None
    except Exception as e:
        return False, str(e)[:120]


def trigger_investigation(
    ticker: str,
    anomaly_type: str = "price_spike",
    lookback_minutes: int = 360,
) -> dict:
    """
    POST /investigate. Blocks until the agent finishes (~15-30s).
    Returns the response JSON. Raises on HTTP error.
    """
    resp = requests.post(
        f"{API_URL}/investigate",
        json={
            "ticker": ticker,
            "anomaly_type": anomaly_type,
            "lookback_minutes": lookback_minutes,
        },
        timeout=90,  # ReAct agent can take 30s + buffer
    )
    resp.raise_for_status()
    return resp.json()

def format_investigation_summary(resp: dict) -> str:
    """
    Format the API's /investigate response into a one-liner for
    the success banner. Defensive — falls back gracefully if any
    field is missing.
    """
    parts = []
    if "investigation_id" in resp:
        parts.append(f"ID #{resp['investigation_id']}")
    if "ticker" in resp:
        parts.append(resp["ticker"])
    if "iterations" in resp and resp["iterations"] is not None:
        parts.append(f"{resp['iterations']} ReAct iter")
    if "tool_calls" in resp and resp["tool_calls"] is not None:
        parts.append(f"{resp['tool_calls']} tool call(s)")
    if "gold_rows" in resp and resp["gold_rows"] is not None:
        parts.append(f"{resp['gold_rows']} gold rows")
    return "  ·  ".join(parts) if parts else "completed"