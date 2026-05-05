# api/agent/state.py
"""
LangGraph state for the investigation agent.

State flows through nodes accumulating data:
    fetch_context fills `gold_context`
    fetch_news    fills `news_context`
    reason        fills `reasoning` and `report_markdown`
    write_report  fills `report_id`
"""

from datetime import datetime
from typing import TypedDict


class AgentState(TypedDict, total=False):
    # ── Inputs (set at graph entry) ──────────────────────────────────────────
    ticker: str
    anomaly_type: str
    window_start: datetime
    lookback_minutes: int        # how far back fetch_context queries gold

    # ── Filled by fetch_context ──────────────────────────────────────────────
    gold_context: list[dict]

    # ── Filled by fetch_news ─────────────────────────────────────────────────
    news_context: list[dict]

    # ── Filled by reason ─────────────────────────────────────────────────────
    reasoning: str               # raw LLM output
    report_markdown: str         # cleaned/formatted brief saved to Postgres

    # ── Filled by write_report ───────────────────────────────────────────────
    report_id: int

    # ── Diagnostics (any node can append) ────────────────────────────────────
    errors: list[str]


def make_initial_state(
    ticker: str,
    anomaly_type: str,
    window_start: datetime,
    lookback_minutes: int = 30,
) -> AgentState:
    """Build the entry-point state from request inputs."""
    return AgentState(
        ticker=ticker,
        anomaly_type=anomaly_type,
        window_start=window_start,
        lookback_minutes=lookback_minutes,
        gold_context=[],
        news_context=[],
        reasoning="",
        report_markdown="",
        report_id=0,
        errors=[],
    )