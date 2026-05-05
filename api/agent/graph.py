# api/agent/graph.py
"""
LangGraph wiring for the investigation agent.

Topology (linear pipeline for v1):
    fetch_context → fetch_news → reason → write_report → END

In Day 5 we'll switch reason to a ReAct loop with MCP tool calls,
which makes the topology dynamic. For now, fixed order.
"""

from langgraph.graph import StateGraph, END

from .nodes import fetch_context, fetch_news, reason, write_report
from .state import AgentState


def build_graph():
    g = StateGraph(AgentState)

    g.add_node("fetch_context", fetch_context)
    g.add_node("fetch_news",    fetch_news)
    g.add_node("reason",        reason)
    g.add_node("write_report",  write_report)

    g.set_entry_point("fetch_context")
    g.add_edge("fetch_context", "fetch_news")
    g.add_edge("fetch_news",    "reason")
    g.add_edge("reason",        "write_report")
    g.add_edge("write_report",  END)

    return g.compile()


# Compiled graph singleton — import this from FastAPI in Block 4
agent = build_graph()