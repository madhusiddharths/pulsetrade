"""
PulseTrade dashboard entry point.

Streamlit auto-discovers files in pages/ and renders them as additional
pages in the sidebar. This file is the landing page.
"""

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from lib.data import get_recent_gold, get_investigations, TICKERS

# ── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PulseTrade",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Auto-refresh ────────────────────────────────────────────────────────────
# Reruns the whole script every 30s. Cached queries (ttl=30s in lib.data)
# avoid hammering Databricks. Refresh count is just for visibility.
count = st_autorefresh(interval=30_000, key="home_refresh")


# ── Sidebar (shared across all pages) ───────────────────────────────────────
with st.sidebar:
    st.title("PulseTrade")
    st.divider()
    st.caption("Real-time market intelligence platform")
    st.caption("Streaming · Agentic · Orchestrated")
    st.divider()
    st.caption(f"Auto-refresh: every 30s (count: {count})")


# ── Main content ────────────────────────────────────────────────────────────
st.title("📈 PulseTrade — Market Overview")

# Quick stats row
col1, col2, col3, col4 = st.columns(4)

gold_result = get_recent_gold(hours=1)
inv_result = get_investigations(limit=50)

with col1:
    st.metric(
        label="Tickers Tracked",
        value=len(TICKERS),
    )

with col2:
    st.metric(
        label="Gold Windows (1h)",
        value=len(gold_result.df) if not gold_result.df.empty else 0,
    )

with col3:
    st.metric(
        label="Total Investigations",
        value=len(inv_result.df),
    )

with col4:
    last_inv = (
        inv_result.df["created_at"].max() if not inv_result.df.empty else None
    )
    st.metric(
        label="Last Investigation",
        value=last_inv.strftime("%H:%M UTC") if last_inv is not None else "—",
    )

# Synthetic-data warning banners
if gold_result.is_synthetic:
    st.warning(
        f"⚠️ Gold data unavailable — showing synthetic. Error: `{gold_result.error}`"
    )
if inv_result.is_synthetic:
    st.warning(
        f"⚠️ Investigations DB unavailable — showing synthetic. Error: `{inv_result.error}`"
    )

st.divider()

st.markdown(
    """
    ### Navigation

    Use the sidebar to explore:

    - **Live Prices** — 5-min OHLC candlestick charts per ticker
    - **News Sentiment** — sentiment trends over time
    - **Investigations** — agent-generated market briefs
    - **Trigger** — manually investigate any ticker

    ---

    **About**: PulseTrade aggregates streaming market data + news from Kafka,
    enriches it via FinBERT sentiment + 5-minute windowed features in Databricks,
    and uses a LangGraph + MCP agent to investigate anomalies. Airflow orchestrates
    the surrounding batch workflows (anomaly sweeps every 5 min, nightly model
    retraining at 2 AM).
    """
)