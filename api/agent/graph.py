# api/agent/graph.py
"""
LangGraph wiring — Day 5 version.

Topology:
    fetch_context → investigate (ReAct loop with MCP tools) → write_report → END

fetch_context is kept hardcoded as a free baseline. investigate replaces the
old fetch_news + reason nodes — news is now fetched on-demand by the agent
when it decides news is relevant to the investigation.
"""

from langgraph.graph import StateGraph, END

from .nodes import fetch_context, investigate, write_report
from .state import AgentState


def build_graph():
    g = StateGraph(AgentState)

    g.add_node("fetch_context", fetch_context)
    g.add_node("investigate",   investigate)
    g.add_node("write_report",  write_report)

    g.set_entry_point("fetch_context")
    g.add_edge("fetch_context", "investigate")
    g.add_edge("investigate",   "write_report")
    g.add_edge("write_report",  END)

    return g.compile()


# Compiled graph singleton — import from FastAPI
agent = build_graph()