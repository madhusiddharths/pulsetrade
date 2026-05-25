## Day 8 — Prometheus + Grafana for agent observability

**Built:**
- FastAPI instrumented with prometheus-fastapi-instrumentator (default HTTP metrics)
- 3 custom metrics: investigations_total, investigation_duration_seconds, gemini_tokens_used_total
- Custom LangChain callback to capture Gemini token usage per LangGraph node
- Docker Compose stack: Prometheus + Grafana (separate from Airflow compose)
- Grafana dashboard with 6 panels (stats, p50/p95/p99 latency, ticker mix, token rate)

**Decisions:**
- Custom histogram buckets (1s to 120s) matched to observed agent latency range
- Labels chosen for low cardinality: ticker (5 values), anomaly_type (~3), status (2)
- Auto-provisioned Prometheus data source via Grafana provisioning config
- Dashboard JSON committed to repo for K8s migration on Day 10

**Gotchas hit:**
- [fill in what bit you — e.g., host.docker.internal on Linux, or langchain-google-genai usage_metadata vs token_usage]

**Deferred to Day 9:**
- Evidently AI for data drift on gold_5min_features (separate concern — runs in Airflow, not FastAPI)