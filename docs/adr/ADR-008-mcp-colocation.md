# ADR-008: Co-locate the MCP server inside the API image (stdio), defer HTTP split

**Status:** Accepted
**Date:** 2026-05-24
**Context day:** Day 10 (containerization)

## Context

The investigation agent (LangGraph + Gemini, in the FastAPI service) reaches its
data tools — `get_recent_gold`, `get_news_for_window`, `tavily_web_search` —
through an MCP server. Today that server runs as a stdio subprocess spawned
per-investigation by `api/agent/mcp_client.py` (`python -m mcp_server.server`,
cwd = `api/`). Containerizing for GKE forces a decision about how the MCP server
is packaged.

## Options considered

### Option A — Co-locate in the API image, keep stdio (CHOSEN)
MCP server code ships in the same image as the agent. The agent spawns it as a
stdio subprocess exactly as it does locally. One image, zero code change.

### Option B — Standalone MCP service over HTTP/SSE
MCP server becomes its own image and K8s Deployment; the agent connects over the
network via `MultiServerMCPClient` pointed at a URL.

## Decision

Option A.

## Rationale

1. **Decision rule = independent variation, not scale.** Split a component out
   when it must vary independently from its caller (scaling, deploy cadence,
   runtime, resource profile, fault isolation, ownership). Co-locate otherwise.

2. **The MCP server has no independent-variation reason.** Stateless wrapper
   over two DB queries + a Tavily call; load is 1:1 with investigations; same
   runtime, deploy cadence, and owner as the API. By the rule, A is correct on
   the merits at any scale — "industry scale" does not by itself mandate B.

3. **The shared-server efficiency argument doesn't hold for our code.**
   `load_mcp_tools(session)` binds tools to a per-session connection. Under B,
   each investigation still opens a fresh SSE session + MCP initialize over the
   network. The per-investigation setup cost moves (local ~200ms fork → network
   handshake), it doesn't disappear. Negligible either way for a 15–30s run.

4. **B revives a solved bug.** `mcp_client.py` uses lower-level `ClientSession` +
   `load_mcp_tools` specifically because `MultiServerMCPClient.get_tools()` in
   langchain-mcp-adapters 0.0.3 returned an empty list (Day 5 "0 tools" bug).
   The HTTP path routes back through that adapter. Bundling that debugging into
   first-time Docker work multiplies risk.

5. **Splitting is a liability.** Each split adds an independent failure mode, a
   network hop, a deployment to coordinate, a probe to tune. Not splitting what
   needn't be split is the more senior choice.

## Consequences

- One image to build/scan/deploy for the agent; simpler K8s topology.
- Scaling is via API replicas; each replica carries its own MCP subprocess —
  acceptable because the subprocess is cheap and stateless.
- Per-investigation subprocess isolation is retained as a side benefit: one
  investigation's tool failure/hang cannot corrupt another's (no shared memory
  or connection state).

## Revisit trigger (when to move to Option B)

Switch to a standalone HTTP/SSE MCP service when the tool layer needs to scale or
resource **independently** from the agent — e.g. tool calls become CPU/memory
heavy and warrant N tool pods behind M API pods, the tools need a different
runtime, or a separate team takes ownership. Until that trigger fires, A stands.

## Related
- Day 5 — MCP server + stdio transport, the 0.0.3 adapter workaround.
- Day 10 build journal — `docs/daily_day10.md`.

## Addendum (Day 10 build): co-located subprocess must inherit parent env

A consequence of Option A surfaced on first containerization. Because the MCP
server runs as a stdio subprocess spawned by the agent (not a separate service
with its own env injection), it does **not** automatically inherit the
environment that `--env-file`/Secret/ConfigMap injects into the API process. The
stdio client launches the subprocess with an empty environment by default.

The first containerized investigation therefore crashed with a pydantic
`ValidationError` (`GOOGLE_API_KEY`/`DATABRICKS_* Field required`,
`input_value={}`) — raised by the *subprocess* at import, while the parent
FastAPI process had started fine with all config present.

Resolution: pass the parent environment explicitly in `_server_params()`:
```python
env=os.environ.copy()
```

This is consistent with the Option-A trust model — the co-located subprocess
shares the parent's lifecycle and security context, so sharing its runtime
environment is correct. A tighter implementation could pass only the keys the
MCP server actually needs (Databricks + Tavily; not the Gemini key, which is
used in the parent). Recorded because under Option B this class of bug would not
exist (the standalone service would get its own env injection), so it is a
genuine, accepted cost of co-location — caught locally on Day 10 rather than in
GKE, where the same gap would otherwise have reappeared.