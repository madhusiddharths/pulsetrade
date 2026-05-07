# api/tests/test_mcp_server.py
"""
Standalone smoke test for the MCP server.

Spawns mcp_server.server as a subprocess, lists tools, calls get_recent_gold,
and prints the result. Validates the MCP protocol layer before wiring to
LangGraph.

Usage:
    cd api && python tests/test_mcp_server.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

print(f"[debug] running with: {sys.executable}", flush=True)

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def main():
    # Spawn the server as a subprocess of THIS python interpreter,
    # so it inherits our venv's installed packages.
    server_params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "mcp_server.server"],
        cwd=str(Path(__file__).resolve().parents[1]),  # api/
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            print("✓ MCP session initialized")

            # 1. Tool discovery
            tools_result = await session.list_tools()
            tools = tools_result.tools
            print(f"\n=== {len(tools)} tools advertised ===")
            for t in tools:
                print(f"  • {t.name}")
                print(f"      {t.description[:80]}...")

            # 2. Call get_recent_gold
            print(f"\n=== calling get_recent_gold(AAPL, 1500) ===")
            result = await session.call_tool(
                "get_recent_gold",
                arguments={"ticker": "AAPL", "lookback_minutes": 1500},
            )
            text = result.content[0].text if result.content else "(empty)"
            print(f"got {len(text)} chars of output")
            print(f"first 400 chars:\n{text[:400]}")

            # 3. Real Tavily search
            print(f"\n=== calling tavily_web_search ===")
            result = await session.call_tool(
                "tavily_web_search",
                arguments={
                    "query": "AAPL Apple stock news this week",
                    "max_results": 3,
                },
            )
            text = result.content[0].text if result.content else "(empty)"
            print(f"got {len(text)} chars of output")
            print(f"first 600 chars:\n{text[:600]}")


if __name__ == "__main__":
    asyncio.run(main())