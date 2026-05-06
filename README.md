# PulseTrade

Real-time financial intelligence platform that streams market data through Kafka and Delta Lake, then uses an agentic AI investigator to explain anomalies in plain language.

## Architecture

Seven-layer pipeline: ingestion → Kafka → Spark Structured Streaming → Delta medallion → agentic AI → Kubernetes serving → observability. Real-time data flows through layers 1–3 continuously; Airflow reads from layer 2 and writes to layers 3–4 on schedule.

![Architecture](docs/architecture.svg)

## Stack

- **Ingestion**: Python producers polling Finnhub + NewsAPI
- **Streaming**: Confluent Cloud Kafka, Spark Structured Streaming, Delta Lake on Databricks (Free Edition)
- **Sentiment**: FinBERT via Pandas UDF
- **Agent**: FastAPI + LangGraph + MCP + Gemini 2.5 Flash + Tavily
- **Storage**: Postgres (investigations) + Delta Lake (analytical)
- **Orchestration**: Apache Airflow
- **Serving**: Streamlit, GKE, Helm, GitHub Actions CI/CD
- **Observability**: Prometheus, Grafana, LangSmith, Evidently AI

## Status

In active development. See [docs/daily.md](docs/daily.md) for the build journal and [docs/decisions.md](docs/decisions.md) for ADRs.

| Day | Status | Component |
|-----|--------|-----------|
| 1   | ✅     | Project scaffolding, cloud accounts, healthcheck |
| 2   | ✅     | Kafka producers (Finnhub + NewsAPI) |
| 3   | ✅     | Bronze → silver → gold medallion + FinBERT sentiment |
| 4   | ✅     | LangGraph agent + FastAPI + Postgres |
| 5   | 🚧     | MCP server + dynamic tool use |
| 6   | ⏳     | Airflow + anomaly detection |
| 7   | ⏳     | Streamlit dashboard |
| 8-9 | ⏳     | Observability (Prometheus, Grafana, Evidently) |
| 10-13 | ⏳   | Containerize + deploy to GKE |
| 14-21 | ⏳   | Polish, demo, write-up |

## Quickstart

1. Copy `.env.example` to `.env` and fill in your API keys
2. Start Postgres: `cd postgres && docker compose up -d`
3. Activate per-service venvs (each service has its own under `*/.venv/`)
4. Run producers: `cd producers && python stock_producer.py`
5. Run the agent API: `cd api && uvicorn main:app --reload`
6. Trigger an investigation: `curl -X POST http://localhost:8000/investigate -H "Content-Type: application/json" -d '{"ticker":"AAPL","anomaly_type":"price_spike","window_start":"2026-05-04T21:15:00Z","lookback_minutes":1500}'`

## Author

Madhu Siddharth Suthagar — MAS Data Science, Illinois Institute of Technology, 2026.
