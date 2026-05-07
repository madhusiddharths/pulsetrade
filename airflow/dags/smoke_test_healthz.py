"""
Day 6 Block 1 smoke test.

Hits the FastAPI healthz endpoint from inside the Airflow container.
If this passes, the Airflow → FastAPI bridge is working and we can build
real DAGs on top.
"""

from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task


@dag(
    dag_id="smoke_test_healthz",
    description="Block 1 smoke test - confirm Airflow can reach FastAPI",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 5, 7),
    catchup=False,
    tags=["smoke", "day6"],
    default_args={
        "retries": 0,
        "execution_timeout": timedelta(seconds=10),
    },
)
def smoke_test_healthz():
    @task
    def hit_healthz():
        import os
        url = os.environ["PULSETRADE_API_URL"] + "/health"   # was /healthz
        print(f"[smoke] GET {url}")
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        body = resp.json()
        print(f"[smoke] status={resp.status_code} body={body}")
        assert body.get("status") == "ok", f"unexpected body: {body}"
        return body

    hit_healthz()


smoke_test_healthz()