# api/main.py
"""
PulseTrade FastAPI service — agent-orchestrated investigation API.
"""

import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic import BaseModel

from config import settings
from data import databricks as dbx
from data import postgres as pg


print(f"[debug] running with: {sys.executable}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[startup] PulseTrade API starting...", flush=True)
    print(f"[startup] databricks host: {settings.databricks_host}", flush=True)
    print(f"[startup] postgres: {settings.postgres_host}:{settings.postgres_port}/"
          f"{settings.postgres_db}", flush=True)

    # Ensure investigations table exists before serving traffic
    try:
        pg.init_schema()
        print("[startup] postgres schema ok", flush=True)
    except Exception as e:
        print(f"[startup][WARN] postgres init failed: {e}", flush=True)
        # Don't crash — /ready will report unready

    yield
    print("[shutdown] PulseTrade API shutting down...", flush=True)


app = FastAPI(
    title="PulseTrade Investigation API",
    description="Agentic AI investigations for financial market anomalies",
    version="0.1.0",
    lifespan=lifespan,
)


# ── Models ──────────────────────────────────────────────────────────────────
class HealthResponse(BaseModel):
    status: str
    timestamp: str
    version: str


class ReadinessResponse(BaseModel):
    status: str
    timestamp: str
    version: str
    checks: dict


# ── Endpoints ───────────────────────────────────────────────────────────────
@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="ok",
        timestamp=datetime.now(timezone.utc).isoformat(),
        version=app.version,
    )


@app.get("/ready", response_model=ReadinessResponse)
async def ready():
    """
    Real readiness check: hit Databricks + Postgres.
    Returns 200 with status='ready' or status='degraded' depending on results.
    """
    dbx_check = dbx.healthcheck()
    pg_check = pg.healthcheck()

    all_ok = dbx_check.get("ok") and pg_check.get("ok")
    return ReadinessResponse(
        status="ready" if all_ok else "degraded",
        timestamp=datetime.now(timezone.utc).isoformat(),
        version=app.version,
        checks={"databricks": dbx_check, "postgres": pg_check},
    )