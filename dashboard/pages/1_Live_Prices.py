"""
Page 1 — Live OHLC candlestick chart per ticker, plus a summary row
showing all tickers at a glance.

Data flow:
  1. Sidebar: time range selector
  2. Top: 5-card summary row (one per ticker, with sparkline)
  3. Sidebar: ticker selector
  4. Main: full candlestick for selected ticker
"""

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from lib.data import get_recent_gold, get_ticker_gold, get_latest_per_ticker, TICKERS
from lib.charts import candlestick_chart, sparkline


# ── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PulseTrade — Live Prices",
    page_icon="📊",
    layout="wide",
)

# ── Auto-refresh every 30s (matches data ttl) ───────────────────────────────
st_autorefresh(interval=30_000, key="live_prices_refresh")


# ── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("PulseTrade")
    st.divider()

    st.subheader("View settings")

    hours_label = st.radio(
        "Time range",
        options=["1h", "6h", "24h"],
        index=1,  # default: 6h
        horizontal=True,
    )
    hours_map = {"1h": 1, "6h": 6, "24h": 24}
    hours = hours_map[hours_label]

    selected_ticker = st.selectbox(
        "Ticker",
        options=TICKERS,
        index=0,
    )

    st.divider()
    st.caption("Auto-refresh: every 30s")


# ── Main ────────────────────────────────────────────────────────────────────
st.title("📊 Live Prices")

# Synthetic-data warning
latest_result = get_latest_per_ticker()
if latest_result.is_synthetic:
    st.warning(
        f"⚠️ Gold data unavailable — showing synthetic. Error: `{latest_result.error}`"
    )

# ── Summary row (5 cards, one per ticker) ──────────────────────────────────
st.subheader("Latest")
cols = st.columns(len(TICKERS))

# Pull last 1h of gold once for all sparklines
spark_data = get_recent_gold(hours=1)

for col, ticker in zip(cols, TICKERS):
    with col:
        ticker_latest = latest_result.df[latest_result.df["ticker"] == ticker]
        if ticker_latest.empty:
            st.metric(label=ticker, value="—", delta=None)
            continue
        row = ticker_latest.iloc[0]
        price = row.get("mean_price") or row.get("close_5min")
        change_pct = row.get("mean_change_pct", 0) or 0

        st.metric(
            label=ticker,
            value=f"${price:.2f}" if price else "—",
            delta=f"{change_pct*100:+.2f}%" if change_pct else None,
        )

        # Sparkline (last hour)
        ticker_spark = spark_data.df[spark_data.df["ticker"] == ticker]
        if not ticker_spark.empty:
            prices = ticker_spark["mean_price"].dropna().tolist()
            st.plotly_chart(
                sparkline(prices),
                use_container_width=True,
                config={"displayModeBar": False},
            )

st.divider()

# ── Main candlestick ───────────────────────────────────────────────────────
st.subheader(f"{selected_ticker} — {hours_label}")

ticker_result = get_ticker_gold(selected_ticker, hours=hours)
if ticker_result.df.empty:
    st.info(
        f"No gold data for {selected_ticker} in the last {hours_label}. "
        "Producers may still be warming up — try a larger time range."
    )
else:
    fig = candlestick_chart(ticker_result.df, selected_ticker)
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={"displayModeBar": False},
    )

    # Quick stats below the chart
    df = ticker_result.df
    stat_cols = st.columns(4)
    with stat_cols[0]:
        st.metric("Windows", len(df))
    with stat_cols[1]:
        st.metric("High", f"${df['high_5min'].max():.2f}")
    with stat_cols[2]:
        st.metric("Low", f"${df['low_5min'].min():.2f}")
    with stat_cols[3]:
        avg_obs = df["n_observations"].mean()
        st.metric("Avg obs/window", f"{avg_obs:.0f}" if avg_obs else "—")