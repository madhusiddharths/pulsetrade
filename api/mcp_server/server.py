# api/mcp_server/server.py
"""
PulseTrade MCP server.

Exposes the agent's data tools over the Model Context Protocol:
  - get_recent_gold        — query gold_5min_features
  - get_news_for_window    — query silver_market_news
  - tavily_web_search      — search the live web for recent news/context

Runs as a stdio subprocess. LangGraph spawns this process and
communicates via stdin/stdout using the MCP JSON-RPC protocol.

Manual smoke test:
    python -m mcp_server.server
    (then send MCP requests on stdin — see test_mcp_server.py)
"""

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# When run as `python -m mcp_server.server` from api/, sys.path is api/.
# Ensure config + data modules resolve regardless of cwd.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from config import settings
from data import databricks as dbx


# ─────────────────────────────────────────────────────────────────────────────
# Server instance
# ─────────────────────────────────────────────────────────────────────────────
app = Server("pulsetrade-mcp")


# ─────────────────────────────────────────────────────────────────────────────
# Tool catalogue — discovered by the LLM via list_tools()
# ─────────────────────────────────────────────────────────────────────────────
@app.list_tools()
async def list_tools() -> list[Tool]:
    """Declare the tools available to LLM callers."""
    return [
        Tool(
            name="get_recent_gold",
            description=(
                "Query the gold_5min_features Delta table for a ticker's "
                "recent 5-minute aggregated windows. Returns OHLC, mean price, "
                "stddev, change_pct, news sentiment per window. Use this to "
                "see how a ticker has been moving and how news sentiment has "
                "tracked. Best for understanding intra-hour patterns."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g. AAPL, NVDA)",
                    },
                    "lookback_minutes": {
                        "type": "integer",
                        "description": "How far back to query (default 30, max 10080 = 1 week)",
                        "default": 30,
                        "minimum": 5,
                        "maximum": 10080,
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="get_news_for_window",
            description=(
                "Query silver_market_news for ticker-tagged news articles "
                "ingested in a specified time window. Each article has a "
                "FinBERT sentiment label (positive/neutral/negative) plus "
                "confidence score. Use this when you need to know what news "
                "preceded a price movement."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol",
                    },
                    "start_iso": {
                        "type": "string",
                        "description": "Window start as ISO-8601 timestamp (e.g. 2026-05-04T20:45:00Z)",
                    },
                    "end_iso": {
                        "type": "string",
                        "description": "Window end as ISO-8601 timestamp",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max articles to return (default 20)",
                        "default": 20,
                        "minimum": 1,
                        "maximum": 50,
                    },
                },
                "required": ["ticker", "start_iso", "end_iso"],
            },
        ),
        Tool(
            name="tavily_web_search",
            description=(
                "Search the live web via Tavily for recent news, analysis, "
                "or context that may not yet be in our news pipeline. Use "
                "sparingly — this hits the public internet. Best for: "
                "specific company events (earnings, lawsuits, exec changes), "
                "macro context (Fed announcements, sector trends), or "
                "verifying a hypothesis against external sources."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query (e.g. 'AAPL earnings May 2026')",
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Max results to return (default 5, max 10)",
                        "default": 5,
                        "minimum": 1,
                        "maximum": 10,
                    },
                },
                "required": ["query"],
            },
        ),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Tool implementations
# ─────────────────────────────────────────────────────────────────────────────
def _serialize(obj: Any) -> Any:
    """Make Databricks rows JSON-serializable (datetime, Decimal, etc.)."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "__float__"):
        try:
            return float(obj)
        except Exception:
            pass
    return str(obj)


def _rows_to_json(rows: list[dict]) -> str:
    """Compact JSON for tool output. The LLM reads this as text."""
    return json.dumps(rows, default=_serialize, indent=2)


async def _tool_get_recent_gold(args: dict) -> str:
    ticker = args["ticker"].upper()
    lookback = int(args.get("lookback_minutes", 30))
    rows = dbx.get_recent_gold(ticker, lookback_minutes=lookback)
    return _rows_to_json(rows) if rows else f"No gold rows for {ticker} in last {lookback} min"


async def _tool_get_news_for_window(args: dict) -> str:
    ticker = args["ticker"].upper()
    start = datetime.fromisoformat(args["start_iso"].replace("Z", "+00:00"))
    end = datetime.fromisoformat(args["end_iso"].replace("Z", "+00:00"))
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    limit = int(args.get("limit", 20))
    rows = dbx.get_news_for_window(ticker, start, end, limit=limit)
    return _rows_to_json(rows) if rows else f"No news for {ticker} in [{start}, {end}]"


# Lazy singleton — built on first call, reused thereafter.
_tavily_client = None


def _get_tavily_client():
    """Return cached Tavily client, building it on first call."""
    global _tavily_client
    if _tavily_client is None:
        if not settings.tavily_api_key:
            raise RuntimeError(
                "TAVILY_API_KEY not configured — cannot use tavily_web_search"
            )
        from tavily import TavilyClient
        _tavily_client = TavilyClient(api_key=settings.tavily_api_key)
    return _tavily_client


async def _tool_tavily_search(args: dict) -> str:
    """
    Search the live web via Tavily.

    Returns a compact JSON list of {title, url, content_snippet, score}.
    We trim Tavily's verbose response — the LLM doesn't need raw_content,
    favicon URLs, or per-result metadata.
    """
    query = args["query"]
    max_results = int(args.get("max_results", 5))

    client = _get_tavily_client()
    # search_depth="basic" is faster + cheaper than "advanced" — fine for our use
    raw = client.search(
        query=query,
        max_results=max_results,
        search_depth="basic",
        include_answer=True,  # Tavily's auto-summary
    )

    trimmed = {
        "query": query,
        "answer": raw.get("answer"),
        "results": [
            {
                "title": r.get("title"),
                "url": r.get("url"),
                "content": (r.get("content") or "")[:500],  # cap each snippet
                "score": round(r.get("score", 0.0), 3),
            }
            for r in raw.get("results", [])[:max_results]
        ],
    }
    return json.dumps(trimmed, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# Dispatcher
# ─────────────────────────────────────────────────────────────────────────────
@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Route an incoming tool call to its implementation."""
    print(f"[mcp] tool call: {name}({arguments})", file=sys.stderr, flush=True)

    handlers = {
        "get_recent_gold": _tool_get_recent_gold,
        "get_news_for_window": _tool_get_news_for_window,
        "tavily_web_search": _tool_tavily_search,
    }
    handler = handlers.get(name)
    if handler is None:
        return [TextContent(type="text", text=f"unknown tool: {name}")]

    try:
        result = await handler(arguments)
    except Exception as e:
        print(f"[mcp] tool {name} failed: {e}", file=sys.stderr, flush=True)
        return [TextContent(type="text", text=f"tool error: {e}")]

    return [TextContent(type="text", text=result)]


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────
async def main():
    print(
        f"[mcp] PulseTrade MCP server starting; databricks={settings.databricks_host}",
        file=sys.stderr,
        flush=True,
    )
    async with stdio_server() as (read, write):
        await app.run(read, write, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())