## Day 10 — Dockerize the deployable services (FastAPI agent + Streamlit dashboard)

**Goal:** take the three would-be-cloud services from "runs on my laptop" toward
"runs in a container," as the foundation for the GKE/Helm work on Days 11–13.
Day 10 is packaging only — no Kubernetes yet.

### What was built
- `api/Dockerfile` — multi-stage (builder → slim runtime), non-root user
  (uid 10001), Python 3.11-slim base to match local 3.11.15. MCP server is
  **co-located in this same image** (Option A — see decision below).
- `api/.dockerignore` — keeps `.venv`, `__pycache__`, `.env`, `tests/` out of
  the build context and image.
- `dashboard/Dockerfile` — same multi-stage + non-root pattern; runs Streamlit
  bound to 0.0.0.0:8501, headless, telemetry off.
- `dashboard/.dockerignore`.

Airflow stays on local Docker Compose; Databricks and Kafka/Confluent stay where
they are. Only the **agent and dashboard** get containerized for GKE — the
realistic split established back in the Day 1–3 plan.

### The one real decision today: MCP transport (Option A vs Option B)

The agent talks to its tools (`get_recent_gold`, `get_news_for_window`,
`tavily_web_search`) through an MCP server. Two ways to package that:

- **Option A — co-locate (CHOSEN).** MCP server ships inside the API image.
  `agent/mcp_client.py` spawns `python -m mcp_server.server` as a stdio
  subprocess per investigation (cwd = image WORKDIR), exactly as on the laptop.
  Zero code change. One image.
- **Option B — standalone MCP service over HTTP/SSE.** MCP server becomes its
  own image + K8s Deployment; the agent reaches it over the network.

**Why A, stated honestly (this is the part to remember for interviews):**

1. **The decision rule is "independent variation," not "scale."** You split a
   component into its own service when it needs to vary independently from its
   caller along some axis — scaling, deploy cadence, runtime, resource profile,
   fault isolation, team ownership. You co-locate when it shares the caller's
   lifecycle and scaling profile.

2. **Our MCP server has no independent-variation reason to exist as its own
   service.** It's a stateless wrapper over two DB queries + a Tavily call. Its
   load is 1:1 with investigations (there is no independent load source), it's
   the same Python runtime as the API, same deploy cadence, no separate team.
   By the rule above, **A is correct on the merits — even at industry scale.**
   "Industry scale" does NOT by itself require B; a large company running this
   exact workload would likely also co-locate it.

3. **The efficiency argument for B doesn't actually hold for our code.** The
   tempting reason for B was "one shared server every API reuses." But
   `load_mcp_tools(session)` binds tools to a *per-session* connection — under
   B, each investigation would still open a fresh SSE session and re-run MCP
   initialize + tool discovery over the network. The per-investigation setup
   cost doesn't vanish; it moves from a ~200ms local fork to a network handshake.
   For a 15–30s investigation, both are negligible — so the efficiency win that
   motivated B is mostly not real at our scale.

4. **B would revive a bug we already solved.** `mcp_client.py` deliberately uses
   the lower-level `ClientSession` + `load_mcp_tools` *because*
   `MultiServerMCPClient.get_tools()` in langchain-mcp-adapters 0.0.3 returned an
   empty tool list (the "0 tools loaded" bug from Day 5). The HTTP path routes
   back through that adapter. Doing B on the same day as first-time Docker work
   means debugging that adapter and Docker simultaneously — how both end up
   half-broken.

5. **Splitting is a liability, not free virtue.** Every service you split out is
   a new independent failure mode, a new network hop that can time out, a new
   deployment to coordinate, a new readiness probe. Senior engineering is
   knowing *not* to split what doesn't need splitting. "Co-located it because
   nothing required otherwise" is a more senior sentence than reflexively
   distributing everything.

**When we WOULD switch to B:** the moment the tool layer needs to scale or
resource independently from the agent — e.g. tool calls become CPU/memory-heavy
and we want N tool-server pods behind M API pods, or the tools need a different
runtime, or a different team owns them. None of that is true today. B is
recorded as the planned evolution under that trigger (see ADR-008), not as work
we skipped.

### Implementation notes / gotchas to watch on build
- **WORKDIR must stay consistent** so `-m mcp_server.server` resolves the same
  way it does locally — both agent and MCP code copied into `/app`, run from
  `/app`. This is the single thing that makes Option A "just work" unchanged.
- **`PYTHONUNBUFFERED=1` is not optional here** — the MCP server prints
  diagnostics to *stderr*; buffered output would swallow those lines and make
  subprocess failures invisible.
- **Single uvicorn worker per container, on purpose** — one MCP subprocess per
  worker means multi-worker = multiple MCP subprocesses in one container. We
  scale via K8s replicas instead; each replica = one tidy (agent + its MCP
  subprocess) unit.
- **libpq split**: `libpq-dev` (headers) in the builder stage only; `libpq5`
  (shared lib) in runtime. No compilers ship in the final image.
- **Current MCP pattern left as-is**: spawn-per-investigation is the
  un-optimized form (pays the fork + DB connection setup every investigation).
  The optimization — spawn one MCP subprocess at FastAPI startup and reuse it —
  was deliberately NOT done today. Don't change working code on packaging day;
  it's noted as a possible later step. Containerizing does not require it.

### Deferred to Day 11+
- Build + run each image locally and smoke-test (`docker run` the API, curl
  `/investigate`; `docker run` the dashboard, load the UI).
- A local `docker-compose.yml` to bring both up together as a pre-K8s sanity
  check.
- Then Helm chart + GKE (Days 11–13).
- Option B (HTTP/SSE MCP) only if/when the independent-scaling trigger fires.

### Day 10 — verification & fixes (what actually happened on build/test day)

All three test gates passed. Recorded here because the bugs found are the real
learning, and two of them will recur throughout the K8s phase.

#### Gate 1 — builds
Both images build clean (multi-stage, non-root). Dashboard built first (simpler,
cheap failure if Docker itself misconfigured), then the API.

#### Gate 2 — run / serve
- Dashboard boots, renders at `http://localhost:8501`. Note: the startup banner
  prints `URL: http://0.0.0.0:8501` — `0.0.0.0` is a *bind* address, browse to
  `localhost` instead.
- API boots, `/health` + `/ready` both 200, Postgres schema init ok.

#### Gate 3 — end-to-end (the Option-A proof)
`POST /investigate` returned `investigation_id: 20`, `tool_calls: 2`,
`iterations: 3`. The agent correctly identified a future-timestamp test anomaly
as noise and reported "ignore" rather than hallucinating — same evidence-based
behaviour noted on Day 6. `tool_calls: 2` is *functional* proof the co-located
MCP subprocess spawned and served tools.

Visual proof of the lifecycle (sampling `/proc/*/comm` every 2s during an
investigation): `uvicorn` + a second `python3.11` present during the run, the
second process gone the instant it completed. The MCP server is born
per-investigation inside the API container and torn down after — exactly the
Option-A model. Screenshot in `docs/screenshots/`.

#### Bug found + fixed: subprocess did not inherit container env
First containerized investigation crashed — NOT the FastAPI process (it started
fine with all env present), but the **MCP subprocess**, with a pydantic
`ValidationError: GOOGLE_API_KEY/DATABRICKS_* Field required`, `input_value={}`.

Root cause: `--env-file .env` injects vars into PID 1 (uvicorn), but the agent
spawns the MCP server via `StdioServerParameters`, and the stdio client launches
the subprocess with an **empty environment** by default — it does not pass the
parent's `os.environ` through. On the laptop this never surfaced (vars were in
the shell / a `.env` file was on disk); in the container there is no `.env`
(correctly excluded by `.dockerignore`) and nothing was inherited.

Fix (`api/agent/mcp_client.py`, `_server_params()`):
```python
env=os.environ.copy(),   # pass parent env to the co-located subprocess
```
This is the *correct* fix, not a workaround: a co-located Option-A subprocess
should share its parent's runtime environment. (A hardened version would pass
only the keys the MCP server needs — Databricks + Tavily, not Gemini — but the
full copy is fine here.)

Why this mattered to catch now: in GKE the env arrives via Secret/ConfigMap →
pod env, and this exact subprocess-inheritance gap would have reappeared there
and been far harder to diagnose. Caught locally = cheap.

#### Two Docker gotchas (will recur in the K8s phase)
1. **Cached `COPY . .` hid a code edit.** After editing `mcp_client.py`, a normal
   rebuild showed every layer `CACHED`, so the fix wasn't in the image and the
   bug "didn't go away." Lesson: after a code edit, confirm the `COPY` layer did
   NOT say CACHED; when verifying a fix, `docker build --no-cache`.
2. **Stale container pinned to an old image ID.** A rebuild moves the
   `:dev` tag to the new image, but an already-running container stays bound to
   the *old* image ID — so `--filter ancestor=pulsetrade-api:dev` didn't match it
   and it kept holding port 8000 (`port is already allocated`). Lesson: after a
   rebuild, `docker ps` and check the IMAGE column shows your tag, not a bare
   hash; stop stale containers by ID.

General principle for both: "I fixed it but the container still shows the old
behaviour" is almost always (a) a cached COPY or (b) a stale running container.

#### Note on test data
Producers had been off ~6h, so gold was empty (`gold_rows: 0`) — the agent
completing with an "insufficient data / not a real anomaly" verdict is a
*successful* run, not a failure. Packaging is proven the moment the subprocess
spawns and tools load; rich data is not required to validate the image.