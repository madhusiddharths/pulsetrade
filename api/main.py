# api/main.py
"""
PulseTrade FastAPI service — agent-orchestrated investigation API.

Endpoints:
  GET  /health                       — liveness
  GET  /ready                        — readiness (Databricks + Postgres reachable)
  POST /investigate                  — trigger a new agent investigation
  GET  /investigations/{id}          — fetch a previously-saved report
"""

# Load .env into os.environ BEFORE importing anything that might read env vars
# (LangChain reads LANGSMITH_TRACING via os.getenv at import time)
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import asyncio
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from config import settings
from data import databricks as dbx
from data import postgres as pg
from agent.graph import agent
from agent.state import make_initial_state


print(f"[debug] running with: {sys.executable}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# Lifespan
# ─────────────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[startup] PulseTrade API starting...", flush=True)
    print(f"[startup] databricks host: {settings.databricks_host}", flush=True)
    print(
        f"[startup] postgres: {settings.postgres_host}:{settings.postgres_port}/"
        f"{settings.postgres_db}",
        flush=True,
    )

    try:
        pg.init_schema()
        print("[startup] postgres schema ok", flush=True)
    except Exception as e:
        print(f"[startup][WARN] postgres init failed: {e}", flush=True)

    yield
    print("[shutdown] PulseTrade API shutting down...", flush=True)


app = FastAPI(
    title="PulseTrade Investigation API",
    description="Agentic AI investigations for financial market anomalies",
    version="0.1.0",
    lifespan=lifespan,
)


# ─────────────────────────────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────────────────────────────
class HealthResponse(BaseModel):
    status: str
    timestamp: str
    version: str


class ReadinessResponse(BaseModel):
    status: str
    timestamp: str
    version: str
    checks: dict


class InvestigationRequest(BaseModel):
    """
    Inbound trigger from the anomaly detector (or manual curl).

    `window_start` should be the ISO timestamp of the 5-min window where the
    anomaly was detected. Defaults to current UTC time if not provided —
    convenient for manual curls and "investigate now" UI buttons. The Day 6
    anomaly detector always sets this explicitly.
    """
    ticker: str = Field(..., min_length=1, max_length=10, examples=["AAPL"])
    anomaly_type: str = Field(..., min_length=1, examples=["price_spike"])
    window_start: Optional[datetime] = Field(
        default=None,
        examples=["2026-05-04T21:15:00Z"],
        description="ISO timestamp of the anomaly window. Defaults to now (UTC).",
    )
    lookback_minutes: int = Field(
        default=30,
        ge=5,
        le=24 * 60 * 7,
        description="how far back fetch_context queries gold",
    )


class InvestigationResponse(BaseModel):
    investigation_id: int
    ticker: str
    anomaly_type: str
    window_start: datetime
    gold_rows: int
    iterations: int                 # NEW — replaces news_rows
    tool_calls: int                 # NEW — how many tool calls happened
    report_markdown: str
    errors: list[str] = []


class InvestigationDetail(BaseModel):
    id: int
    ticker: str
    anomaly_type: Optional[str]
    window_start: Optional[datetime]
    report_markdown: Optional[str]
    agent_thoughts: Optional[Any]
    created_at: datetime


# ─────────────────────────────────────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="ok",
        timestamp=datetime.now(timezone.utc).isoformat(),
        version=app.version,
    )


@app.get("/ready", response_model=ReadinessResponse)
async def ready():
    dbx_check = dbx.healthcheck()
    pg_check = pg.healthcheck()
    all_ok = dbx_check.get("ok") and pg_check.get("ok")
    return ReadinessResponse(
        status="ready" if all_ok else "degraded",
        timestamp=datetime.now(timezone.utc).isoformat(),
        version=app.version,
        checks={"databricks": dbx_check, "postgres": pg_check},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Investigation endpoints
# ─────────────────────────────────────────────────────────────────────────────
@app.post(
    "/investigate",
    response_model=InvestigationResponse,
    status_code=201,
    summary="Trigger an agent investigation",
    description=(
        "Runs the 4-node LangGraph agent: fetch_context → fetch_news → "
        "reason → write_report. Returns the investigation id plus the "
        "markdown brief. Synchronous: waits for the agent to complete "
        "(typically 3-15 seconds depending on Gemini latency)."
    ),
)
async def investigate(req: InvestigationRequest):
    # Default to now if caller didn't specify; normalize naive timestamps to UTC
    ws = req.window_start or datetime.now(timezone.utc)
    if ws.tzinfo is None:
        ws = ws.replace(tzinfo=timezone.utc)

    initial = make_initial_state(
        ticker=req.ticker,
        anomaly_type=req.anomaly_type,
        window_start=ws,
        lookback_minutes=req.lookback_minutes,
    )

    # The agent does blocking I/O (Databricks SQL, Gemini HTTP, Postgres).
    # asyncio.to_thread runs it on a worker thread so the FastAPI event
    # loop stays free to serve other requests during the 3-15s investigation.
    try:
        final = await asyncio.to_thread(agent.invoke, initial)
    except Exception as e:
        # The agent is supposed to capture errors into state, not raise.
        # If we got here, something more fundamental broke (graph compile
        # error, OOM, etc.) — return 500.
        raise HTTPException(
            status_code=500,
            detail=f"agent invocation crashed: {e}",
        )

    rid = final.get("report_id", 0)
    if not rid:
        # Agent ran but didn't persist. Postgres write probably failed.
        raise HTTPException(
            status_code=500,
            detail={
                "message": "agent completed without persisting a report",
                "errors": final.get("errors", []),
            },
        )

    # Count tool calls from the LangChain message log
    tool_calls = sum(
        len(getattr(m, "tool_calls", None) or [])
        for m in final.get("messages", [])
        if type(m).__name__ == "AIMessage"
    )

    return InvestigationResponse(
        investigation_id=rid,
        ticker=req.ticker,
        anomaly_type=req.anomaly_type,
        window_start=ws,
        gold_rows=len(final.get("gold_context", [])),
        iterations=final.get("iterations", 0),
        tool_calls=tool_calls,
        report_markdown=final.get("report_markdown", ""),
        errors=final.get("errors", []),
    )


@app.get(
    "/investigations/{investigation_id}",
    response_model=InvestigationDetail,
    summary="Fetch a saved investigation by id",
)
async def get_investigation_endpoint(investigation_id: int):
    row = await asyncio.to_thread(pg.get_investigation, investigation_id)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"investigation {investigation_id} not found",
        )
    return InvestigationDetail(**row)