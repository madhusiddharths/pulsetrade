"""
anomaly_queue table operations: claim pending rows, mark complete/failed.

Importable from FastAPI (already has SQLAlchemy engine pool) AND from
Airflow DAGs (which set up their own connection — see airflow/dags/lib/).
The shared piece is the SQL; the connection differs by context.
"""

from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine


def claim_pending(engine: Engine, limit: int = 50) -> list[dict]:
    """
    Atomically claim up to `limit` pending anomalies.

    Flips them from 'pending' to 'processing' in a single UPDATE...RETURNING
    so two concurrent runs (e.g., overlapping DAG retries) can't both grab
    the same rows. Uses SKIP LOCKED to skip rows another transaction is
    already updating, avoiding contention.
    """
    sql = text("""
        WITH next_batch AS (
            SELECT id
            FROM anomaly_queue
            WHERE status = 'pending'
            ORDER BY detected_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
        )
        UPDATE anomaly_queue aq
        SET status = 'processing'
        FROM next_batch
        WHERE aq.id = next_batch.id
        RETURNING aq.id, aq.ticker, aq.anomaly_type,
                  aq.window_start, aq.window_end,
                  aq.z_score, aq.trigger_value
    """)

    with engine.begin() as conn:
        rows = conn.execute(sql, {"limit": limit}).mappings().all()
    return [dict(r) for r in rows]


def mark_done(
    engine: Engine,
    queue_id: int,
    investigation_id: int,
) -> None:
    """Mark a queue row done and link it to the investigation that handled it."""
    sql = text("""
        UPDATE anomaly_queue
        SET status = 'done',
            processed_at = NOW(),
            investigation_id = :inv_id
        WHERE id = :id
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"id": queue_id, "inv_id": investigation_id})


def mark_failed(engine: Engine, queue_id: int, error: Optional[str] = None) -> None:
    """
    Mark a queue row failed. We don't have an `error` column intentionally —
    the failure mode is captured in Airflow logs + LangSmith. If you want
    to persist errors here later, add a column.
    """
    sql = text("""
        UPDATE anomaly_queue
        SET status = 'failed',
            processed_at = NOW()
        WHERE id = :id
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"id": queue_id})