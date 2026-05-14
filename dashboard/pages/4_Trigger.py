"""
Page 4 — Manually trigger an investigation.

Calls FastAPI /investigate synchronously. The ReAct agent typically takes
15-30s, during which we show a spinner. The result is rendered inline
including the full markdown brief.

When the page loads:
  1. Check FastAPI /health
  2. If unhealthy: disable button, show banner
  3. If healthy: enable the form

Note: this is the only page that *writes* to the system. Read-only pages
(1, 2, 3) read directly from Databricks/Postgres. This page goes through
FastAPI because triggering an investigation runs the LangGraph + MCP agent.
"""

import streamlit as st

from lib.api import (
    API_URL,
    health,
    trigger_investigation,
    format_investigation_summary,
)
from lib.data import TICKERS


st.set_page_config(
    page_title="PulseTrade — Trigger",
    page_icon="⚡",
    layout="wide",
)


# ── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("PulseTrade")
    st.divider()
    st.subheader("FastAPI status")

    is_healthy, err = health()
    if is_healthy:
        st.success(f"✓ Connected to {API_URL}")
    else:
        st.error(f"✗ Cannot reach {API_URL}")
        if err:
            st.caption(f"`{err}`")

    st.divider()
    st.caption(
        "Run `uvicorn main:app --reload --port 8000` in `api/` "
        "if the API is down."
    )


# ── Main ────────────────────────────────────────────────────────────────────
st.title("⚡ Trigger Investigation")
st.caption(
    "Manually run the agent against any ticker. Useful for demos and for "
    "exploring why a ticker looked unusual."
)

if not is_healthy:
    st.error(
        "**FastAPI is unreachable.** The trigger button is disabled. "
        "Start uvicorn locally to enable it."
    )
    st.stop()

# Persist the last result across reruns so the user can read it without
# the spinner clearing the screen on next interaction.
if "last_investigation" not in st.session_state:
    st.session_state.last_investigation = None
if "last_error" not in st.session_state:
    st.session_state.last_error = None

# ── Form: inputs ───────────────────────────────────────────────────────────
with st.form("trigger_form", clear_on_submit=False):
    c1, c2, c3 = st.columns(3)
    with c1:
        ticker = st.selectbox("Ticker", options=TICKERS, index=0)
    with c2:
        anomaly_type = st.selectbox(
            "Anomaly type",
            options=["price_spike", "sentiment_shock", "volume_anomaly"],
            index=0,
            help=(
                "Hint to the agent about what to investigate. The agent "
                "decides its own tool sequence regardless."
            ),
        )
    with c3:
        lookback_minutes = st.select_slider(
            "Lookback window",
            options=[60, 180, 360, 720, 1440],
            value=360,
            format_func=lambda m: f"{m//60}h" if m >= 60 else f"{m}m",
            help="How far back the agent considers context",
        )

    submitted = st.form_submit_button(
        "🔍 Investigate Now",
        type="primary",
        use_container_width=True,
    )

# ── Submit handler ──────────────────────────────────────────────────────────
if submitted:
    st.session_state.last_error = None
    with st.spinner(
        f"Running ReAct agent on {ticker}… "
        f"(typically 15-30s; the agent decides its own tool sequence)"
    ):
        try:
            resp = trigger_investigation(
                ticker=ticker,
                anomaly_type=anomaly_type,
                lookback_minutes=lookback_minutes,
            )
            st.session_state.last_investigation = resp
        except Exception as e:
            st.session_state.last_error = str(e)[:300]
            st.session_state.last_investigation = None

# ── Display the last result (persists across reruns) ──────────────────────
if st.session_state.last_error:
    st.error(f"**Investigation failed.** `{st.session_state.last_error}`")

elif st.session_state.last_investigation:
    resp = st.session_state.last_investigation
    summary = format_investigation_summary(resp)

    st.success(f"✓ Investigation complete  ·  {summary}")

    # Metric row — the recruiter-impressive part
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Investigation ID", resp.get("investigation_id", "—"))
    with c2:
        st.metric("ReAct iterations", resp.get("iterations") or 0)
    with c3:
        st.metric("Tool calls", resp.get("tool_calls") or 0)
    with c4:
        st.metric("Gold rows", resp.get("gold_rows") or 0)

    st.divider()

    # The agent's markdown brief — inline
    st.subheader("Agent brief")
    brief = resp.get("report_markdown") or "_(no report returned)_"
    st.markdown(brief)

    st.divider()

    # Link to the Investigations page for context
    st.caption(
        f"This investigation is also visible in the **Investigations** "
        f"page (ID #{resp.get('investigation_id', '?')})."
    )

else:
    st.info(
        "Pick a ticker and anomaly type above, then click "
        "**Investigate Now** to run the agent."
    )