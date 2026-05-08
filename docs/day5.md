## Day 5 — ReAct + MCP

Replaced the Day 4 hardcoded 4-node pipeline with a ReAct loop where Gemini
selects MCP tools dynamically. fetch_context kept as a free baseline; news
fetching moved out of the graph into an MCP tool the agent calls only when
relevant.

Three debugging stories worth keeping:

1. **MultiServerMCPClient.get_tools() returns [] before session init.**
   In langchain-mcp-adapters 0.0.3, the high-level client lazy-initializes
   subprocesses but get_tools() doesn't wait. Got "0 tools loaded" silently —
   Gemini hallucinated tool calls as pseudo-Python text. Fix: drop down to
   mcp.ClientSession + langchain_mcp_adapters.tools.load_mcp_tools, wrap the
   ReAct loop in `async with mcp_session()` so the subprocess lives for the
   loop's duration.

2. **LANGSMITH_TRACING=true is the master switch.** API key + project name
   alone don't enable auto-tracing. Without TRACING=true in os.environ at
   import time, LangChain stays silent. Added to .env, also added explicit
   load_dotenv() at the top of main.py so env loads before LangChain imports.

3. **Test passing on hallucinated tool calls.** Validation only checked
   reasoning length and error array, both of which a one-shot hallucination
   passes. Added a check: if no baseline gold AND zero tool calls, fail —
   that combo means the ReAct loop didn't actually run.

Notable: agent on first real run chose gold+news+tavily, found a data-pipeline
gap (no recent gold for AAPL), correctly recommended "investigate the pipeline"
rather than confidently calling it a false anomaly. Cited the conflicting
"market closed / extended hours" Tavily result as part of the uncertainty.
Grounded reasoning, hedged confidence — exactly the behavior you want.