# Day 1 — Local setup + cloud accounts (✅ complete)

## What I built

Day 1 was zero application code — pure infrastructure plumbing. The point was
to remove every possible "infrastructure-not-ready" blocker before writing any
real Python on Day 2+. By end of day, I could run a smoke test in every venv
and have credentials for every external service I'd need for Week 1.

### Project structure decided

```
pulsetrade/
├── producers/.venv/        Kafka producers (Finnhub + NewsAPI)
├── api/.venv/              FastAPI + LangGraph + MCP (Day 4+)
├── dashboard/.venv/        Streamlit (Day 7)
├── airflow/                Docker Compose (Day 6)
├── databricks/notebooks/   Spark notebooks, source-of-truth in git
├── postgres/               Local Docker Compose for investigations DB
├── infra/k8s/, infra/helm/ Kubernetes (Day 10+)
├── docs/decisions.md       ADRs
├── docs/daily.md           Rolling journal
├── docs/screenshots/       Demo evidence for portfolio
└── scripts/healthcheck.sh  Multi-venv smoke test
```

**Per-service venvs**, not one mega-venv. Each service has different deps that
fight each other if combined (Airflow pins old pydantic; LangChain wants new
pydantic; Databricks Connect pins specific pyspark). Same reason production
deploys each as its own container.

### Local tools installed

- Homebrew + git + wget + pyenv + pipx + uv
- Python 3.11 specifically (Airflow 2.10 doesn't fully support 3.13 yet)
- Docker Desktop with 8GB RAM / 4 CPUs allocated

### Per-service venvs created and smoke-tested

| Venv | Key deps | Smoke test |
|---|---|---|
| `producers/.venv` | confluent-kafka, requests, pyyaml | imported confluent_kafka without error |
| `api/.venv` | fastapi, langgraph, langchain-google-genai | one real Gemini API call succeeded |
| `dashboard/.venv` | streamlit, plotly, databricks-sql-connector | imports clean |
| `airflow/` | (no venv — Docker images pulled) | `docker compose pull` finished |

### Cloud accounts wired up

All credentials landed in `pulsetrade/.env` (gitignored from day one).

- **Confluent Cloud Kafka**: cluster `pulsetrade-dev`, AWS us-east-2, Basic tier.
  Topics created: `stock-prices`, `market-news`. **$5 spend alert set** so I get
  notified before any real money happens. Card-on-file is unavoidable for
  Confluent — they need it after the free $400 credit.
- **Databricks Free Edition**: workspace at `dbc-e99162d2-d0a1.cloud.databricks.com`.
  PAT generated, 30-day lifetime. Catalog: `workspace.pulsetrade.*`. No clusters
  spun up yet — those auto-suspend but cost nothing while idle on Free Edition.
- **Google AI Studio (Gemini 2.5 Flash)**: 1500 req/day free, no card required.
  Picked Gemini over Anthropic for cost — Day 2's planning math showed Gemini
  Flash is ~10× cheaper per token at this volume. Anthropic gets a $5 spend
  cap regardless, as a hedge in case I want to swap back.
- **NewsAPI**: 100 req/day free. Plan is to poll every 30 min = 48 req/day
  which gives headroom.

### Not yet activated (deliberate)

- **GCP / GKE**: Week 4. The $300 free credit clock starts the moment you
  activate billing, and an idle cluster eats that fast.
- **LangSmith**: Day 4 when there's an agent to trace.
- **Tavily**: Day 5 when the MCP server adds web search.
- **GitHub Actions**: Day 10 when there's something to deploy.

The pattern: **activate cloud accounts just-in-time** so trial clocks align
with when I actually need them.

## Lessons captured

- **`uv venv` is significantly faster than `python -m venv`** for setting up
  multiple Python venvs in a row. Saved ~5 min total across the four services.
- **`set -a && source .env && set +a` is the right pattern** for getting env
  vars into the current shell without explicitly exporting each. The `-a`
  flag means "automatically export every var that gets set." Use it whenever
  a shell command needs `.env` to be readable in subprocesses.
- **Trial clocks are the real budget**, not money. Databricks Free Edition
  doesn't have a clock, but the trial alternative does. Picked Free Edition
  for that reason. Confluent Basic is $1.50/day after the $400 credit
  burns down — set a calendar reminder for day 28 to either delete the
  cluster or accept the charge.
- **The per-service venv pattern duplicates pinned deps across files** but
  that's the cost of isolation. Worth it.

## Interview-ready stories

- **Per-service venv decision**: "Tried a single mega-venv first; pip resolver
  failed on the Airflow + LangChain pydantic version conflict. Split into per-
  service venvs to match how each service would deploy as its own container
  in production."
- **Gemini vs Anthropic cost analysis**: Did a back-of-envelope on expected
  agent token volume (say 10 investigations/day × 5K tokens/each = 50K
  tokens/day). At Gemini Flash pricing, that's $0.01/day; at Claude Sonnet,
  about $0.15/day. Order-of-magnitude difference for a portfolio project
  where I'm paying out of pocket.
- **Just-in-time cloud activation**: Skipped GCP and LangSmith on Day 1
  because their free-tier or trial clocks would burn before I had anything
  to deploy or trace.

## Files committed

```
.env.example      (template, no secrets)
.gitignore        (excludes .env, .venv/, __pycache__, etc.)
docs/             (empty for now)
producers/        (empty venv)
api/              (empty venv)
dashboard/        (empty venv)
airflow/          (placeholder)
scripts/healthcheck.sh
README.md         (stub)
```

Commit message:
`day 1: structure + venvs + week 1 cloud accounts`

## Cost at end of day 1

~$5 (the Anthropic top-up I bought, $0.0001 of which is actually used).
Everything else is free tier.

## What's next

Day 2: actual streaming code. Build `stock_producer.py` (Finnhub → Kafka) and
`news_producer.py` (NewsAPI → Kafka). Verify messages land in Confluent's
Topics UI. By end of Day 2, the streaming spine is live.