# api/agent/nodes.py
"""
Investigation agent nodes.

Each node:
  - Reads input from `state`
  - Does ONE thing (fetch data, call LLM, write DB)
  - Returns the updated state

Errors are appended to state["errors"] but never raise — graph must complete.
"""

from datetime import timedelta, timezone

from langchain_google_genai import ChatGoogleGenerativeAI

from config import settings
from data import databricks as dbx
from data import postgres as pg
from .state import AgentState


# Lazily-built singleton — first call to reason() constructs it,
# subsequent calls reuse the same client (saves ~100ms per run on init).
_llm: ChatGoogleGenerativeAI | None = None


def _get_llm() -> ChatGoogleGenerativeAI:
    """
    Build (or return cached) Gemini client.

    Passing `google_api_key=` explicitly is critical: without it,
    google.auth picks up cached `gcloud` Application Default Credentials
    and uses those scopes instead — which fail with 403 on Gemini.
    """
    global _llm
    if _llm is None:
        _llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            temperature=0.2,
            google_api_key=settings.google_api_key,
        )
    return _llm


# ─────────────────────────────────────────────────────────────────────────────
# Node 1: fetch_context — pull recent gold features for the ticker
# ─────────────────────────────────────────────────────────────────────────────
def fetch_context(state: AgentState) -> AgentState:
    """
    Pull gold_5min_features rows for this ticker into state.
    Lookback window read from state["lookback_minutes"]; defaults to 30.
    """
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
# Node 2: fetch_news — pull news around the anomaly window
# ─────────────────────────────────────────────────────────────────────────────
def fetch_news(state: AgentState) -> AgentState:
    """
    Pull silver_market_news rows for this ticker in a window centered on
    `window_start` — 30 min before, 30 min after.
    """
    try:
        ws = state["window_start"]
        if ws.tzinfo is None:
            ws = ws.replace(tzinfo=timezone.utc)
        start = ws - timedelta(minutes=30)
        end = ws + timedelta(minutes=30)

        rows = dbx.get_news_for_window(state["ticker"], start, end, limit=20)
        state["news_context"] = rows
        print(
            f"[fetch_news] {state['ticker']}: {len(rows)} news rows "
            f"in [{start}, {end}]",
            flush=True,
        )
    except Exception as e:
        state.setdefault("errors", []).append(f"fetch_news: {e}")
        state["news_context"] = []
    return state


# ─────────────────────────────────────────────────────────────────────────────
# Node 3: reason — Gemini analyzes and writes the brief
# ─────────────────────────────────────────────────────────────────────────────
def _build_reasoning_prompt(state: AgentState) -> str:
    """Assemble the prompt for Gemini from the gathered context."""
    gold = state.get("gold_context", [])
    news = state.get("news_context", [])

    # Compact gold table → easy-to-scan text for the model
    gold_lines = []
    for r in gold[:10]:  # cap to keep prompt small
        gold_lines.append(
            f"  {r['window_start']}: open={r['open_5min']}, close={r['close_5min']}, "
            f"high={r['high_5min']}, low={r['low_5min']}, "
            f"stddev={r.get('price_stddev')}, "
            f"change_pct={r.get('mean_change_pct')}, "
            f"sentiment={r.get('mean_news_sentiment')}, "
            f"news_count={r.get('news_article_count')}"
        )
    gold_section = "\n".join(gold_lines) if gold_lines else "  (no gold rows)"

    news_lines = []
    for n in news[:10]:
        score = n.get("sentiment_score")
        score_str = f"{score:.2f}" if isinstance(score, (int, float)) else "n/a"
        news_lines.append(
            f"  [{n.get('sentiment_label')} / {score_str}] "
            f"{n.get('title')} — {n.get('source_name')}"
        )
    news_section = "\n".join(news_lines) if news_lines else "  (no news in window)"

    return f"""You are a financial analyst investigating a market anomaly.

ANOMALY DETAILS
  Ticker:        {state['ticker']}
  Anomaly type:  {state['anomaly_type']}
  Window start:  {state['window_start']}

RECENT 5-MINUTE FEATURE WINDOWS (most recent first)
{gold_section}

NEWS IN ±30 MIN WINDOW
{news_section}

YOUR TASK
Write a concise investigation brief in markdown with these sections:

  ## Summary
    One sentence: is this a real anomaly or noise? Confidence level.

  ## Evidence
    Bullet points citing specific numbers from the data above.

  ## Likely Cause
    Best hypothesis given the evidence. If news supports it, name the article.

  ## Recommended Action
    One of: "monitor", "investigate further", "alert human analyst", "ignore"

Be specific. Cite numbers. If data is insufficient, say so explicitly — do not speculate.
"""


def reason(state: AgentState) -> AgentState:
    """Build prompt → invoke Gemini → store reasoning + report_markdown."""
    try:
        prompt = _build_reasoning_prompt(state)
        llm = _get_llm()
        resp = llm.invoke(prompt)
        text = resp.content if hasattr(resp, "content") else str(resp)

        state["reasoning"] = text
        state["report_markdown"] = text  # for now, same as reasoning
        print(f"[reason] generated {len(text)} chars of analysis", flush=True)
    except Exception as e:
        state.setdefault("errors", []).append(f"reason: {e}")
        state["reasoning"] = ""
        state["report_markdown"] = f"# Investigation failed\n\nError: {e}"
    return state


# ─────────────────────────────────────────────────────────────────────────────
# Node 4: write_report — persist to Postgres
# ─────────────────────────────────────────────────────────────────────────────
def write_report(state: AgentState) -> AgentState:
    """Insert the report row, capture the new id back into state."""
    try:
        # agent_thoughts = full state minus the markdown (avoid duplicating)
        thoughts = {
            "gold_context": state.get("gold_context", []),
            "news_context": state.get("news_context", []),
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