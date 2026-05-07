# api/agent/mcp_client.py
"""
MCP client wrapper for the investigation agent.

Spawns the local MCP server (mcp_server.server) as a stdio subprocess and
exposes its tools as LangChain BaseTool objects.

Uses the lower-level mcp.ClientSession + langchain-mcp-adapters.load_mcp_tools
because MultiServerMCPClient.get_tools() in 0.0.3 returns an empty list when
called before sessions are initialized — caused our "0 tools loaded" bug.

Each call to get_mcp_tools() builds a fresh session within the current
asyncio loop. Spawning the subprocess is ~200ms; acceptable for a 15-30s
investigation. The session and subprocess are cleaned up automatically when
the context manager exits at the end of investigate().
"""

import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from langchain_core.tools import BaseTool
from langchain_mcp_adapters.tools import load_mcp_tools
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


def _server_params() -> StdioServerParameters:
    """Build the stdio params for spawning our MCP server."""
    api_dir = Path(__file__).resolve().parents[1]  # agent/ → api/
    return StdioServerParameters(
        command=sys.executable,
        args=["-m", "mcp_server.server"],
        cwd=str(api_dir),
    )


@asynccontextmanager
async def mcp_session() -> AsyncIterator[tuple[ClientSession, list[BaseTool]]]:
    """
    Spawn MCP server, initialize session, load LangChain tools, yield both.

    Usage:
        async with mcp_session() as (session, tools):
            # use tools — they will route through this session
            await tools[0].ainvoke({...})
        # subprocess cleaned up automatically here
    """
    params = _server_params()
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await load_mcp_tools(session)
            print(
                f"[mcp_client] loaded {len(tools)} tools: "
                f"{[t.name for t in tools]}",
                flush=True,
            )
            yield session, tools