# Architecture Decision Records — Index

Full ADRs live in [`docs/adr/`](adr/). Each records context, the decision, and
its consequences at the moment it was made.

| ADR | Decision | Area |
|-----|----------|------|
| [ADR-001](adr/ADR-001-finhub-over-yfinance.md) | Finnhub over yfinance for the market data source | Ingestion |
| [ADR-002](adr/ADR-002-gemini-explicit-apikey.md) | Gemini as the reasoning LLM, with an explicit API key (not ADC) | Agent |
| [ADR-003](adr/ADR-003-microbatch-vs-continuous.md) | Micro-batch (Trigger.AvailableNow) consumption over continuous streaming | Streaming |
| [ADR-004](adr/ADR-004-mendallion-contract-merge-idempotency.md) | Medallion layer contracts + MERGE-based idempotency | Lakehouse |
| [ADR-005](adr/ADR-005-mcp-studio-transport.md) | MCP server over stdio transport for the agent tool layer | Agent |
| [ADR-006](adr/ADR-006-observability-stack.md) | Self-hosted Prometheus/Grafana for system metrics | Observability |
| [ADR-007](adr/ADR-007-data-drift-detection.md) | Data drift detection: Evidently + Airflow DockerOperator | ML Ops |
| [ADR-008](adr/ADR-008-mcp-colocation.md) | Co-locate the MCP server inside the API image (stdio), defer HTTP split | Serving |
| [ADR-009](adr/ADR-009-pod-vs-cluster.md) | Postgres as an in-cluster pod for the GKE demo (not Cloud SQL) | Infra |
