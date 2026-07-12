"""
Page 2 — News sentiment trends per ticker, derived from gold's
mean_news_sentiment column. Comparison view: all tickers on one chart.
"""

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from lib.data import get_news_sentiment, TICKERS
from lib.charts import sentiment_chart


st.set_page_config(
    page_title="PulseTrade — News Sentiment",
    page_icon="💬",
    layout="wide",
)

st_autorefresh(interval=30_000, key="news_refresh")


# ── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("PulseTrade")
    st.divider()
    st.subheader("View settings")

    hours_label = st.radio(
        "Time range",
        options=["6h", "24h", "72h"],
        index=1,
        horizontal=True,
    )
    hours_map = {"6h": 6, "24h": 24, "72h": 72}
    hours = hours_map[hours_label]

    show_tickers = st.multiselect(
        "Tickers",
        options=TICKERS,
        default=TICKERS,
    )

    st.divider()
    st.caption("Auto-refresh: every 30s")


# ── Main ────────────────────────────────────────────────────────────────────
st.title("💬 News Sentiment")
st.caption(
    "Mean FinBERT sentiment score per ticker, aggregated into 5-min windows. "
    "Range: −1 (negative) to +1 (positive)."
)

result = get_news_sentiment(hours=hours)

if result.is_synthetic:
    st.warning(
        f"⚠️ Gold data unavailable — showing synthetic. Error: `{result.error}`"
    )

if result.df.empty:
    st.info(
        "No sentiment data yet. The news producer may still be warming up, "
        "or articles haven't aligned with 5-min windows. Try a longer time range."
    )
elif not show_tickers:
    st.info("Select at least one ticker in the sidebar.")
else:
    df = result.df[result.df["ticker"].isin(show_tickers)]
    if df.empty:
        st.info("No data for the selected tickers in the chosen range.")
    else:
        fig = sentiment_chart(df, show_tickers)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        st.divider()

        # ── Per-ticker summary table ──────────────────────────────────────
        st.subheader("Per-ticker summary")
        summary = (
            df.groupby("ticker")
            .agg(
                avg_sentiment=("mean_news_sentiment", "mean"),
                article_count=("news_article_count", "sum"),
                window_count=("window_start", "count"),
            )
            .reset_index()
            .sort_values("avg_sentiment", ascending=False)
        )
        summary["avg_sentiment"] = summary["avg_sentiment"].round(3)
        summary["article_count"] = summary["article_count"].astype(int)

        st.dataframe(
            summary,
            use_container_width=True,
            hide_index=True,
            column_config={
                "ticker": st.column_config.TextColumn("Ticker"),
                "avg_sentiment": st.column_config.NumberColumn(
                    "Avg sentiment", format="%.3f"
                ),
                "article_count": st.column_config.NumberColumn(
                    "Articles in range", format="%d"
                ),
                "window_count": st.column_config.NumberColumn(
                    "Windows w/ news", format="%d"
                ),
            },
        )