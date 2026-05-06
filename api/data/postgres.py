# api/data/postgres.py
"""
Postgres client — stores agent investigation reports.

Schema: investigations table holds one row per agent run, with the final
markdown brief plus the LangGraph state at end-of-run (for replay/debug).
"""

import json
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, MetaData, Table,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine

from config import settings


# ── Engine: single instance, lazily created ─────────────────────────────────
_engine: Optional[Engine] = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings.postgres_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,  # validate connections on checkout
        )
    return _engine


# ── Schema (raw SQL — simpler than ORM for one table) ───────────────────────
CREATE_INVESTIGATIONS_SQL = """
CREATE TABLE IF NOT EXISTS investigations (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10)  NOT NULL,
    anomaly_type    VARCHAR(50),
    window_start    TIMESTAMPTZ,
    report_markdown TEXT,
    agent_thoughts  JSONB,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_investigations_ticker
    ON investigations (ticker);

CREATE INDEX IF NOT EXISTS idx_investigations_created_at
    ON investigations (created_at DESC);
"""


def init_schema() -> None:
    """Create the investigations table + indexes if they don't exist."""
    eng = get_engine()
    with eng.begin() as conn:
        for stmt in CREATE_INVESTIGATIONS_SQL.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))


# ── Public API ──────────────────────────────────────────────────────────────
def save_investigation(
    ticker: str,
    anomaly_type: str,
    window_start: datetime,
    report_markdown: str,
    agent_thoughts: dict,
) -> int:
    """
    Insert a completed investigation. Returns the new row's id.
    `agent_thoughts` is the LangGraph state at end-of-run, serialized to JSONB.
    """
    eng = get_engine()
    insert_sql = text("""
        INSERT INTO investigations
            (ticker, anomaly_type, window_start, report_markdown, agent_thoughts)
        VALUES (:ticker, :anomaly_type, :window_start, :report, CAST(:thoughts AS JSONB))
        RETURNING id
    """)
    with eng.begin() as conn:
        row = conn.execute(
            insert_sql,
            {
                "ticker": ticker,
                "anomaly_type": anomaly_type,
                "window_start": window_start,
                "report": report_markdown,
                "thoughts": json.dumps(agent_thoughts, default=str),
            },
        ).fetchone()
    return int(row[0])


def get_investigation(investigation_id: int) -> Optional[dict]:
    """Fetch one investigation by id, or None."""
    eng = get_engine()
    sel = text("""
        SELECT id, ticker, anomaly_type, window_start,
               report_markdown, agent_thoughts, created_at
        FROM investigations WHERE id = :id
    """)
    with eng.connect() as conn:
        row = conn.execute(sel, {"id": investigation_id}).mappings().fetchone()
    return dict(row) if row else None


def healthcheck() -> dict:
    """Quick reachability + schema-exists check used by /ready."""
    try:
        eng = get_engine()
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
            # confirm table exists (init_schema may not have run yet)
            res = conn.execute(text(
                "SELECT to_regclass('public.investigations') AS t"
            )).fetchone()
            table_present = res[0] is not None
        return {"ok": True, "table_present": table_present}
    except Exception as e:
        return {"ok": False, "error": str(e)}