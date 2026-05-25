# ADR-005: MCP server over stdio transport for the agent tool layer

**Status:** Accepted (superseded in part by ADR-008 for packaging)
**Date:** ~Day 5 (reconstructed 2026-05-24 from project history)
**Context day:** Day 5 (agent dynamic-tool layer)

> Reconstructed from the Day 5 build notes, which recorded this reasoning in
> detail. Assigned ADR-005 here.

## Context

On Day 4 the agent called data functions directly inside fixed LangGraph nodes
(`fetch_context` → `fetch_news` → `reason` → `write_report`). Day 5 replaced the
hardcoded calls with **dynamic tool selection**: the agent's data functions are
exposed as MCP tools, the `reason` node becomes a ReAct loop, and Gemini chooses
which tools to call. A transport had to be chosen for the MCP server.

## Decision

Run the MCP server as a **stdio subprocess** that the agent spawns
(`python -m mcp_server.server`, cwd = `api/`), not as an HTTP service. Wrap its
tools as LangChain tools and bind them to Gemini for a ReAct loop.

## Rationale

Three reasons stdio was chosen over HTTP+SSE:

1. **Trust boundary** — an stdio server runs on the same machine as the client,
   same user privileges. No network exposure, no auth needed, no port conflicts.
2. **Lifecycle** — the subprocess is born when the parent starts and dies when
   the parent dies. No orphaned servers to manage.
3. **Single-tenant isolation** — each client gets its own server instance, so
   state (DB connections, in-memory caches) is isolated per investigation.

The known trade-off, recorded at the time: **stdio can't be shared across
machines.** For agents on K8s pods that can't fork subprocesses, MCP supports
HTTP+SSE transport. That was explicitly deferred to "Day 10 when we containerize."

## Consequences

- **stdout is sacred**: the MCP JSON-RPC protocol owns stdout, so every diagnostic
  print uses `file=sys.stderr, flush=True`. A stray `print()` to stdout corrupts
  the protocol and the client sees a parse error.
- The ReAct loop emits tool *intentions* (gold → news → web search → answer),
  can retry with different args on empty results, and stops early when satisfied
  — 5–10 dynamic calls per investigation instead of 4 fixed ones.
- **Implementation note / known fragility**: the agent uses the lower-level
  `mcp.ClientSession` + `langchain_mcp_adapters.load_mcp_tools` rather than
  `MultiServerMCPClient.get_tools()`, because in `langchain-mcp-adapters==0.0.3`
  the latter returned an empty tool list when called before sessions were
  initialized (the "0 tools loaded" bug). This workaround is load-bearing.

## Supersession / evolution

The "Day 10" revisit happened: **ADR-008** records the decision to keep stdio and
**co-locate** the MCP server inside the API container (Option A) rather than
splitting it into a standalone HTTP/SSE service (Option B), because the tool
layer has no independent-scaling reason to exist as its own service. The HTTP/SSE
transport contemplated here remains the planned evolution if/when that changes.

## Related
- ADR-008 (MCP co-location vs standalone for containerization).
- Day 5 journal (stdio mechanics, the stdout/stderr discipline).