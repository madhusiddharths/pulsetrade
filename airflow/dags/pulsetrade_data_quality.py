"""
pulsetrade_data_quality — daily data drift detection.

Spawns the `pulsetrade-drift:latest` container (Docker-out-of-Docker) which:
  - pulls reference + current windows from gold_5min_features
  - runs Evidently DataDriftPreset
  - writes a drift_alerts row to the application Postgres
  - emits a timestamped HTML report to the mounted reports volume

Why DockerOperator (not PythonOperator): the drift job is a separate
deployable unit with a heavy, isolated dependency tree (Evidently + sklearn +
scipy). Running it as its own container keeps those deps out of the Airflow
image and mirrors how it'll run in K8s later (KubernetesPodOperator / CronJob)
— same image, same entrypoint, different orchestrator.

Schedule: daily. Drift is a slow phenomenon; daily checking is realistic and
conserves Databricks Free Edition compute (vs hourly).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# Env vars to forward into the drift container. They already exist in the
# Airflow worker's environment (x-airflow-common-env in docker-compose.yaml),
# so we read them here rather than hardcoding — no secrets live in this file.
_FORWARD_ENV_KEYS = [
    "DATABRICKS_HOST",
    "DATABRICKS_HTTP_PATH",
    "DATABRICKS_TOKEN",
    "DATABRICKS_CATALOG",
    "DATABRICKS_SCHEMA",
    "PULSETRADE_PG_HOST",
    "PULSETRADE_PG_PORT",
    "PULSETRADE_PG_USER",
    "PULSETRADE_PG_PASSWORD",
    "PULSETRADE_PG_DB",
]

drift_environment = {k: os.environ.get(k, "") for k in _FORWARD_ENV_KEYS}
# Tell the drift job where to write reports inside its own container.
drift_environment["DRIFT_OUTPUT_DIR"] = "/reports"
drift_environment["DRIFT_INJECT"] = "true"  # demo mode; flip to "false" for real data

# Host path where drift HTML reports should land. This is a HOST path because
# the DockerOperator spawns a SIBLING container (not a child) — the volume
# source is resolved by the host Docker daemon, not relative to the worker.
# Adjust if your project lives elsewhere on the host.
HOST_REPORTS_DIR = os.environ.get(
    "DRIFT_HOST_REPORTS_DIR",
    "/Users/madhusiddharthsuthagar/Documents/python/pulsetrade/drift/reports",
)

default_args = {
    "owner": "pulsetrade",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pulsetrade_data_quality",
    description="Daily Evidently data drift check on gold_5min_features",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["pulsetrade", "data-quality", "drift", "evidently"],
) as dag:

    run_drift = DockerOperator(
        task_id="run_drift_detection",
        image="pulsetrade-drift:latest",
        # Don't let Airflow try to pull from a registry — image is local only.
        force_pull=False,
        auto_remove="success",          # clean up the container after it exits ok
        command=None,                   # use the image's default CMD (python run_drift.py)
        environment=drift_environment,
        mounts=[
            Mount(
                source=HOST_REPORTS_DIR,
                target="/reports",
                type="bind",
            ),
        ],
        # The spawned container talks to host services (Postgres, Databricks)
        # via host.docker.internal, same as the worker. On Docker Desktop this
        # resolves automatically; the explicit extra_host makes it robust.
        extra_hosts={"host.docker.internal": "host-gateway"},
        # Reach the host Docker daemon through the mounted socket.
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,            # avoid the /tmp mount that trips up DoD on macOS
    )