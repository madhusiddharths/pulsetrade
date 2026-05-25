## Day 6 — Orchestration + ML lifecycle

### What works
- Airflow on Docker Compose (LocalExecutor → CeleryExecutor; 6 services, custom Dockerfile)
- DAG 1: pulsetrade_anomaly_sweep (every 5 min)
  - z-score detector inserts to anomaly_queue
  - claim_batch with FOR UPDATE SKIP LOCKED for safe concurrency
  - investigate_one.expand() — dynamic task mapping to /investigate
- DAG 2: pulsetrade_nightly_retrain (2 AM)
  - Isolation Forest training on 30-day gold window
  - MLflow Model Registry: pulsetrade_iforest
  - Conditional promotion: candidate must beat champion by ≥5% precision@k

### Lessons captured
- Airflow 2.10 + SQLAlchemy 2.0 incompatibility (use bundled 1.4)
- pydantic Settings() validates eagerly at import; required fields must be
  in container env, not just .env on host
- POSTGRES_HOST differs Mac (localhost) vs container (host.docker.internal)
- Switched from _PIP_ADDITIONAL_REQUIREMENTS to custom Dockerfile
  (container restart 90s → 5s; reproducible builds)
- Databricks Free Edition allows external MLflow access via DATABRICKS_TOKEN

### Interview-ready stories
- Two-tier anomaly detection: fast z-score in streaming, smart Isolation
  Forest in nightly batch
- Conditional model promotion as a safety mechanism against bad training data
- Dynamic task mapping idiom for fan-out parallelism