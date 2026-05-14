# Day 4 — FastAPI + LangGraph agent (✅ complete)

## What I built

Day 4 is the architectural turning point. Up to Day 3 the project was a
data pipeline; today it became an AI service that reads from that pipeline
and produces human-readable investigation briefs.

### Block 1 — FastAPI scaffold + typed config (~30 min)

`api/main.py` is a thin FastAPI app with:
- `/health` — liveness probe (returns `{"status":"ok",
  "timestamp":"...", "version":"0.1.0"}`)
- `/ready` — readiness probe (actually checks Databricks + Postgres
  reachability; returns 503 if either is down)

`api/config.py` is the typed-config singleton. Uses `pydantic-settings`
with `SettingsConfigDict(env_file=ENV_FILE)` reading from project-root
`.env`. Fields:

- LLM: `google_api_key`, `groq_api_key` (optional backup)
- Databricks: `databricks_host`, `databricks_token`, `databricks_http_path`,
  `databricks_catalog`, `databricks_schema`
- Postgres: `postgres_host`, `postgres_port`, `postgres_user`,
  `postgres_password`, `postgres_db`, plus a computed `postgres_url`
  property
- LangSmith (optional): `langsmith_api_key`, `langsmith_project`
- Tavily (Day 5): `tavily_api_key`

Required fields use `Field(...)`; missing ones fail fast at startup with
a clear pydantic validation error. Optional fields default to `None`.

This typed config singleton paid off later — Day 6 ran into "pydantic
validates Settings eagerly at import" when the same code ran inside the
Airflow container. Knowing exactly which fields were required and which
were optional made the fix targeted.

### Block 2 — Data layer (Databricks SQL + Postgres) (~45 min)

`api/data/databricks.py` — uses `databricks-sql-connector` (the SQL-only
client, ~10MB) rather than PySpark (~300MB). The agent process is small;
it just needs to query existing Delta tables, not transform data.

Three public functions:
- `get_recent_gold(ticker, lookback_minutes=30)` — last N min of gold for
  one ticker
- `get_news_for_window(ticker, start, end, limit=20)` — silver news in
  range
- `healthcheck()` — for `/ready`

Connection management via `@contextmanager` so cursors and connections
always close cleanly.

`api/data/postgres.py` — uses SQLAlchemy (engine pool, `pool_pre_ping=True`)
to write to the local Postgres `investigations` table:

```sql
CREATE TABLE IF NOT EXISTS investigations (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10)  NOT NULL,
    anomaly_type    VARCHAR(50),
    window_start    TIMESTAMPTZ,
    report_markdown TEXT,
    agent_thoughts  JSONB,
    created_at      TIMESTAMPTZ DEFAULT now()
);
-- + idx on ticker, idx on created_at DESC
```

`agent_thoughts` is `JSONB` to store the LangGraph state at end-of-run —
useful for replay/debugging without having to re-run the whole agent.
This came back to bite me on Day 5 when I tried to JSON-serialize
`AIMessage` objects (had to add `default=str` to `json.dumps`).

`init_schema()` runs in the FastAPI lifespan hook on startup, so the
table exists before any request can hit `/investigate`.

### Block 3 — LangGraph 4-node agent (~90 min)

`api/agent/state.py` defines the state TypedDict:

```python
class AgentState(TypedDict):
    ticker: str
    anomaly_type: str
    window_start: datetime
    lookback_minutes: int
    # Filled by nodes:
    gold_context: list[dict]
    news_context: list[dict]
    reasoning: str
    report_id: int
    errors: list[str]
```

`api/agent/nodes.py` — four pure functions, each taking AgentState and
returning AgentState:

1. **`fetch_context`** — calls `get_recent_gold()`, stuffs into
   `gold_context`. Errors get appended to `state["errors"]` instead of
   raising, so downstream nodes can decide what to do with partial data.
2. **`fetch_news`** — calls `get_news_for_window()` for the same time
   range as gold. Same error-handling pattern.
3. **`reason`** — builds a prompt from gold + news context, calls Gemini
   2.5 Flash via `langchain-google-genai`. Returns `reasoning` (markdown
   string). The prompt is structured: "you are a financial analyst,
   here's the price data, here's the news, what happened and why?"
4. **`write_report`** — persists to Postgres via SQLAlchemy. Returns
   `report_id`.

`api/agent/graph.py` wires them up:

```
fetch_context → fetch_news → reason → write_report → END
```

Linear pipeline. No conditional edges yet — that's Day 5's job when
the agent gets to pick its own tool sequence.

#### Gemini ADC bug (took ~45 min to debug)

`langchain-google-genai`'s default auth path is Google's Application
Default Credentials (ADC), which expects `gcloud auth application-default
login`. I don't have gcloud installed and don't want to — that pulls in
a Java runtime + 200MB of GCP SDK.

The fix: pass `google_api_key=settings.google_api_key` *explicitly* to
the `ChatGoogleGenerativeAI` constructor. With an explicit key, the
client uses the AI Studio path (different auth, doesn't need ADC).

Documented as a gotcha in `docs/decisions.md` because the error message
was a 403 from a totally different API surface — wasted ~45 min staring
at it before realizing.

### Block 4 — Wire to FastAPI + smoke test (~60 min)

`POST /investigate` endpoint:

```python
class InvestigationRequest(BaseModel):
    ticker: str
    anomaly_type: str = "price_spike"
    window_start: datetime | None = None
    lookback_minutes: int = 30

@app.post("/investigate", response_model=InvestigationResponse)
async def investigate(req: InvestigationRequest):
    state = make_initial_state(req)
    final = agent.invoke(state)
    return InvestigationResponse(
        investigation_id=final["report_id"],
        ticker=final["ticker"],
        gold_rows=len(final["gold_context"]),
        news_rows=len(final["news_context"]),
        report_markdown=final["report_markdown"],
        errors=final["errors"],
    )
```

End-to-end test (`api/tests/test_agent_e2e.py`) runs the agent against
real gold data, prints the markdown report, and verifies the row exists
in Postgres.

#### The "agent pushed back on bad data" moment

First real test: the test set `anomaly_type="price_spike"` for AAPL.
Gold had one window with `price_stddev=0` (only one observation —
zero variance is meaningless). The agent's brief flagged this:

> "The label suggests a price_spike, but the available gold data shows
> only one observation in the window with zero price variance. There's
> no evidence of a spike — this anomaly was likely misclassified upstream
> or the window has insufficient data to support the label."

That's senior-analyst output from a Day 4 build. Not just "here are
the numbers," but pushing back on the input premise. Documented as
the moment the project started feeling real.

## Lessons captured

- **Pass `google_api_key` explicitly to `ChatGoogleGenerativeAI`** —
  default path expects ADC which fails with 403. The error message
  doesn't say "missing key"; it looks like a Google Cloud auth problem.
- **Use `databricks-sql-connector` not PySpark for query-only paths**.
  10MB vs 300MB, and it's all the agent process needs.
- **Run `init_schema()` in the FastAPI lifespan hook** so the table
  exists before any request can hit. Without lifespan, you'd have a
  race on first request.
- **Error-collection (`state["errors"]`) > raising** in LangGraph nodes.
  Lets downstream nodes decide what to do with partial state. Especially
  important when Day 5's ReAct loop replaces this with retries.
- **Store the full LangGraph state in Postgres as `JSONB`** for replay.
  But `default=str` is required because some types (AIMessage,
  datetime, Decimal) don't serialize natively.

## Interview-ready stories

- **The ADC debugging session**: 45 minutes of staring at a 403 before
  realizing it was the auth path, not the API key. Fix was one parameter
  (`google_api_key=...`). Documented as ADR-002.
- **Why SQL Connector instead of PySpark**: the agent only reads existing
  tables. Bringing a full Spark runtime into a small FastAPI service is
  300MB of dependencies for zero benefit. The Connector is the right
  scope.
- **Why error-collection over raising in agent nodes**: errors in
  `fetch_context` shouldn't stop the agent from trying `fetch_news`.
  Each node appends to `state["errors"]` and downstream nodes decide
  whether to bail. Makes the pipeline more like a real production
  observability pattern than a fragile chain.
- **The agent pushing back on bad labels**: the system prompt asked the
  model to reason from evidence rather than accept the input premise.
  When fed insufficient data, it noticed and said so — instead of
  hallucinating a price-spike explanation. That's the prompt design
  paying off.
- **`agent_thoughts` JSONB column**: every investigation persists its
  full final state. Means I can replay any investigation later for
  debugging without re-running the agent (which costs Gemini tokens).

## Files committed

```
api/
├── .venv/                  (Day 1)
├── main.py                 (FastAPI + lifespan)
├── config.py               (typed pydantic-settings)
├── requirements.txt
├── data/
│   ├── databricks.py       (SQL connector queries)
│   └── postgres.py         (SQLAlchemy, init_schema, save_investigation)
├── agent/
│   ├── state.py            (TypedDict + helpers)
│   ├── nodes.py            (4 nodes)
│   └── graph.py            (compiled StateGraph)
└── tests/
    └── test_agent_e2e.py
postgres/
└── docker-compose.yaml     (local postgres:16-alpine on port 5432
                             — moved to 5433 in Day 6)
docs/decisions.md           (ADR-002 added: explicit Gemini API key)
```

Commit messages:
- `day 4 blocks 1-3: agent core (FastAPI + LangGraph + Gemini + Postgres)`
- `day 4 block 4: wired agent to POST /investigate + e2e test`

## Cost at end of day 4

Still ~$5. Gemini calls are free tier. ~10 test investigations ran
today, each using ~3K tokens; total under 30K tokens (well under the
1500-request-per-day free limit).

## What's next

Day 5: MCP server. Replace the hardcoded 4-node pipeline with a ReAct
loop where Gemini picks tools dynamically. The MCP server exposes
gold queries, news queries, and Tavily web search as tools; LangGraph
spawns it as a stdio subprocess; the `reason` node becomes an
iteration-capped loop. This is where the agent gets to feel "smart"
instead of "scripted."