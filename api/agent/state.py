# api/agent/state.py
"""
LangGraph state for the investigation agent.

State flows through nodes accumulating data:
    fetch_context fills `gold_context` (still hardcoded — free baseline)
    investigate   runs a ReAct loop, accumulates `messages`, fills `reasoning`
    write_report  fills `report_id`
"""

from datetime import datetime
from typing import Any, TypedDict


class AgentState(TypedDict, total=False):
    # ── Inputs (set at graph entry) ──────────────────────────────────────────
    ticker: str
    anomaly_type: str
    window_start: datetime
    lookback_minutes: int

    # ── Filled by fetch_context (kept from Day 4 — free baseline) ────────────
    gold_context: list[dict]

    # ── Filled by investigate (ReAct loop) ───────────────────────────────────
    messages: list[Any]          # LangChain message objects
    iterations: int              # how many ReAct turns ran
    reasoning: str               # final answer extracted from last AIMessage
    report_markdown: str         # cleaned/formatted brief saved to Postgres

    # ── Filled by write_report ───────────────────────────────────────────────
    report_id: int

    # ── Diagnostics ──────────────────────────────────────────────────────────
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
        messages=[],
        iterations=0,
        reasoning="",
        report_markdown="",
        report_id=0,
        errors=[],
    )