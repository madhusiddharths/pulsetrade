# api/agent/nodes.py
"""
Investigation agent nodes — Day 5 ReAct version.

Topology:
    fetch_context  → investigate (ReAct loop) → write_report

`fetch_context` is kept hardcoded — giving Gemini the last 30 min of gold
"for free" saves the agent ~30s and several tool calls every run. The agent
only needs MCP tools when it wants more than this baseline.
"""

import asyncio
from observability.token_callback import GeminiTokenCounter

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_google_genai import ChatGoogleGenerativeAI

from config import settings
from data import databricks as dbx
from data import postgres as pg
from .mcp_client import mcp_session
from .state import AgentState
import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="google.generativeai")

_max_iterations = 8


def _build_system_prompt(state: AgentState) -> str:
    """The agent's role + guidance on when to use which tool."""
    return f"""You are a financial market anomaly investigator.

Your job: given an anomaly trigger, decide whether it represents a real,
meaningful market event — and explain why with specific evidence.

You have access to tools that query our internal financial data lake plus
the live web. Use them deliberately:

  • get_recent_gold       — pull more historical 5-minute windows. Use this
                            if the baseline context isn't enough to judge the
                            move (e.g. you need a longer comparison).
  • get_news_for_window   — pull news in a specific time window. Use this
                            when you suspect news drove the move.
  • tavily_web_search     — search the live web. Use this for events that
                            may not be in our internal news pipeline yet
                            (earnings dates, exec changes, lawsuits, macro
                            announcements).

You do NOT need to call every tool. Stop when you have enough evidence to
answer. Calling tools costs latency and money — be efficient.

When you have enough evidence, write your final brief in markdown with
these sections, and DO NOT call any more tools after that:

  ## Summary
    One sentence: is this a real anomaly or noise? Confidence level.

  ## Evidence
    Bullet points citing specific numbers from the data.

  ## Likely Cause
    Best hypothesis given the evidence. Cite sources by URL when relevant.

  ## Recommended Action
    One of: "monitor", "investigate further", "alert human analyst", "ignore"

CONTEXT FOR THIS INVESTIGATION:
  Ticker:        {state['ticker']}
  Anomaly type:  {state['anomaly_type']}
  Window start:  {state['window_start']}
"""


def _build_initial_user_message(state: AgentState) -> str:
    """First user message — includes the baseline gold context for free."""
    gold = state.get("gold_context", [])
    if not gold:
        gold_section = "(no baseline gold rows — agent must fetch via tools)"
    else:
        lines = []
        for r in gold[:10]:
            lines.append(
                f"  {r['window_start']}: open={r['open_5min']}, close={r['close_5min']}, "
                f"high={r['high_5min']}, low={r['low_5min']}, "
                f"stddev={r.get('price_stddev')}, "
                f"change_pct={r.get('mean_change_pct')}, "
                f"sentiment={r.get('mean_news_sentiment')}, "
                f"news_count={r.get('news_article_count')}"
            )
        gold_section = "\n".join(lines)

    return f"""Investigate this anomaly.

BASELINE GOLD CONTEXT (last 30 min, already fetched for you):
{gold_section}

Decide whether you need more data via tool calls, or whether this baseline
is sufficient. Then produce your investigation brief.
"""


# ─────────────────────────────────────────────────────────────────────────────
# Node 1: fetch_context — baseline (unchanged from Day 4)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_context(state: AgentState) -> AgentState:
    """Pull baseline gold rows for the ticker. Lookback configurable via state."""
    try:
        lookback = state.get("lookback_minutes", 30)
        rows = dbx.get_recent_gold(state["ticker"], lookback_minutes=lookback)
        state["gold_context"] = rows
        print(
            f"[fetch_context] {state['ticker']}: {len(rows)} gold rows "
            f"(lookback {lookback}min)",
            flush=True,
        )
    except Exception as e:
        state.setdefault("errors", []).append(f"fetch_context: {e}")
        state["gold_context"] = []
    return state


# ─────────────────────────────────────────────────────────────────────────────
# Node 2: investigate — the ReAct loop
# ─────────────────────────────────────────────────────────────────────────────
async def _investigate_async(state: AgentState) -> AgentState:
    """
    Async ReAct loop.

    Wraps the entire loop in `mcp_session()` so the MCP subprocess stays
    alive while tools are being invoked. Exiting this context kills the
    subprocess and invalidates the tool handles — must NOT call tools
    after the `async with` block exits.
    """

    async with mcp_session() as (_session, tools):
        tools_by_name = {t.name: t for t in tools}

        # Build LLM bound to the now-loaded tools.
        # Built fresh each investigation because tools' underlying session
        # changes per investigation — can't cache across runs.
        base_llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            temperature=0.2,
            google_api_key=settings.google_api_key,
        )
        llm = base_llm.bind_tools(tools)
        print(
            f"[llm] bound {len(tools)} MCP tools to Gemini: "
            f"{[t.name for t in tools]}",
            flush=True,
        )

        messages = [
            SystemMessage(content=_build_system_prompt(state)),
            HumanMessage(content=_build_initial_user_message(state)),
        ]

        iteration = 0
        final_text = ""

        while iteration < _max_iterations:
            iteration += 1
            print(f"[investigate] iteration {iteration}/{_max_iterations}", flush=True)

            try:
                response: AIMessage = await llm.ainvoke(
                    messages,
                    config={"callbacks": [GeminiTokenCounter(node_name="investigate")]},
                )
            except Exception as e:
                state.setdefault("errors", []).append(f"investigate llm.ainvoke: {e}")
                break

            messages.append(response)
            tool_calls = getattr(response, "tool_calls", None) or []

            if not tool_calls:
                final_text = response.content if isinstance(response.content, str) else str(response.content)
                print(
                    f"[investigate] no more tool calls — final answer "
                    f"({len(final_text)} chars)",
                    flush=True,
                )
                break

            print(
                f"[investigate] gemini wants to call: "
                f"{[(tc['name'], list(tc['args'].keys())) for tc in tool_calls]}",
                flush=True,
            )

            for tc in tool_calls:
                tool_name = tc["name"]
                tool_args = tc["args"]
                tool_call_id = tc["id"]

                # Defensive: some providers serialize args as JSON string
                if isinstance(tool_args, str):
                    import json as _json
                    try:
                        tool_args = _json.loads(tool_args)
                    except _json.JSONDecodeError:
                        tool_args = {}

                tool = tools_by_name.get(tool_name)
                if tool is None:
                    result_text = f"ERROR: unknown tool {tool_name}"
                else:
                    try:
                        result_text = await tool.ainvoke(tool_args)
                        if not isinstance(result_text, str):
                            result_text = str(result_text)
                    except Exception as e:
                        result_text = f"ERROR: tool {tool_name} failed: {e}"
                        state.setdefault("errors", []).append(
                            f"investigate tool {tool_name}: {e}"
                        )

                if not result_text or not result_text.strip():
                    result_text = "(tool returned no results)"

                if len(result_text) > 8000:
                    result_text = result_text[:8000] + "\n... [truncated]"

                messages.append(ToolMessage(content=result_text, tool_call_id=tool_call_id))

        if iteration >= _max_iterations and not final_text:
            print("[investigate] hit iteration cap, requesting final summary", flush=True)
            messages.append(HumanMessage(
                content=(
                    "You've used your tool-call budget. Stop calling tools and "
                    "write your final investigation brief now using whatever "
                    "evidence you've gathered."
                )
            ))
            try:
                response = await llm.ainvoke(
                    messages,
                    config={"callbacks": [GeminiTokenCounter(node_name="investigate")]},
                )
                final_text = response.content if isinstance(response.content, str) else str(response.content)
            except Exception as e:
                state.setdefault("errors", []).append(f"investigate forced-summary: {e}")
                final_text = "# Investigation incomplete\n\nIteration cap reached without final answer."
        state["messages"] = messages
        state["iterations"] = iteration
        state["reasoning"] = final_text
        state["report_markdown"] = final_text or "# Investigation failed\n\nNo reasoning produced."
        return state

def investigate(state: AgentState) -> AgentState:
    """
    Sync wrapper around the async ReAct loop.

    LangGraph nodes can be sync or async. We expose this as sync because the
    surrounding graph build is sync, and we run the loop with asyncio.run.
    """
    try:
        return asyncio.run(_investigate_async(state))
    except Exception as e:
        state.setdefault("errors", []).append(f"investigate (top-level): {e}")
        state["reasoning"] = ""
        state["report_markdown"] = f"# Investigation failed\n\nError: {e}"
        return state


# ─────────────────────────────────────────────────────────────────────────────
# Node 3: write_report — persist (unchanged from Day 4 except thoughts shape)
# ─────────────────────────────────────────────────────────────────────────────
def write_report(state: AgentState) -> AgentState:
    """Insert the report row, capture the new id back into state."""
    try:
        # Serialize messages compactly — full LangChain messages are huge
        msg_summary = []
        for m in state.get("messages", []):
            kind = type(m).__name__
            if kind == "AIMessage":
                tcs = getattr(m, "tool_calls", None) or []
                if tcs:
                    msg_summary.append({
                        "kind": "AIMessage",
                        "tool_calls": [{"name": tc["name"], "args": tc["args"]} for tc in tcs],
                    })
                else:
                    text = m.content if isinstance(m.content, str) else str(m.content)
                    msg_summary.append({"kind": "AIMessage", "text_chars": len(text)})
            elif kind == "ToolMessage":
                msg_summary.append({
                    "kind": "ToolMessage",
                    "tool_call_id": getattr(m, "tool_call_id", None),
                    "content_chars": len(m.content) if isinstance(m.content, str) else 0,
                })
            else:
                msg_summary.append({"kind": kind})

        thoughts = {
            "gold_context": state.get("gold_context", []),
            "iterations": state.get("iterations", 0),
            "messages_summary": msg_summary,
            "errors": state.get("errors", []),
        }

        report_id = pg.save_investigation(
            ticker=state["ticker"],
            anomaly_type=state["anomaly_type"],
            window_start=state["window_start"],
            report_markdown=state.get("report_markdown", ""),
            agent_thoughts=thoughts,
        )
        state["report_id"] = report_id
        print(f"[write_report] saved investigation #{report_id}", flush=True)
    except Exception as e:
        state.setdefault("errors", []).append(f"write_report: {e}")
        state["report_id"] = 0
    return state