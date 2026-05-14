"""
Reusable Plotly chart builders.
Keep page files focused on layout; put chart logic here.

Color convention (matches the Bloomberg theme):
  - GREEN = up, GAIN, positive sentiment
  - RED   = down, LOSS, negative sentiment
  - AMBER = neutral, accent, axis text
  - GRID  = subtle gray, on dark background
"""

import pandas as pd
import plotly.graph_objects as go


GREEN = "#00C853"
RED = "#FF1744"
AMBER = "#FFB300"
GRID = "#2A2A2A"
TEXT = "#E0E0E0"
BG = "#0E0E0E"


def candlestick_chart(df: pd.DataFrame, ticker: str) -> go.Figure:
    """
    OHLC candlestick chart for a single ticker.
    df must have: window_start, open_5min, high_5min, low_5min, close_5min,
                  optionally news_article_count for the secondary axis.
    """
    fig = go.Figure()

    # Main candlestick
    fig.add_trace(go.Candlestick(
        x=df["window_start"],
        open=df["open_5min"],
        high=df["high_5min"],
        low=df["low_5min"],
        close=df["close_5min"],
        increasing_line_color=GREEN,
        decreasing_line_color=RED,
        increasing_fillcolor=GREEN,
        decreasing_fillcolor=RED,
        name=ticker,
    ))

    # If news count is present, overlay as bars on a secondary axis
    if "news_article_count" in df.columns and df["news_article_count"].sum() > 0:
        fig.add_trace(go.Bar(
            x=df["window_start"],
            y=df["news_article_count"],
            name="News articles",
            marker_color=AMBER,
            opacity=0.4,
            yaxis="y2",
        ))

    fig.update_layout(
        title=dict(
            text=f"{ticker} — 5-min OHLC",
            font=dict(color=TEXT, size=16, family="monospace"),
        ),
        plot_bgcolor=BG,
        paper_bgcolor=BG,
        font=dict(color=TEXT, family="monospace"),
        xaxis=dict(
            gridcolor=GRID,
            rangeslider_visible=False,
            showspikes=True,
            spikemode="across",
            spikecolor=AMBER,
            spikethickness=1,
        ),
        yaxis=dict(
            title="Price (USD)",
            gridcolor=GRID,
            side="left",
        ),
        yaxis2=dict(
            title="News articles",
            overlaying="y",
            side="right",
            showgrid=False,
            rangemode="tozero",
        ),
        legend=dict(
            bgcolor=BG,
            bordercolor=GRID,
            borderwidth=1,
            x=0.01,
            y=0.99,
        ),
        height=500,
        margin=dict(l=50, r=50, t=50, b=40),
        hovermode="x unified",
    )
    return fig


def sparkline(prices: list[float]) -> go.Figure:
    """
    Tiny line chart for use inside summary cards. No axes, no labels.
    Color follows the trend (green=up overall, red=down overall).
    """
    if not prices or len(prices) < 2:
        # Degenerate case — return empty figure
        fig = go.Figure()
        fig.update_layout(
            height=50, margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor=BG, plot_bgcolor=BG,
            xaxis=dict(visible=False), yaxis=dict(visible=False),
        )
        return fig

    color = GREEN if prices[-1] >= prices[0] else RED
    fig = go.Figure(go.Scatter(
        y=prices,
        mode="lines",
        line=dict(color=color, width=2),
        showlegend=False,
        hoverinfo="skip",
    ))
    fig.update_layout(
        height=50,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor=BG,
        plot_bgcolor=BG,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig

def sentiment_chart(df, tickers: list[str]) -> go.Figure:
    """
    Multi-ticker sentiment over time. One line per ticker on the primary axis.
    Article count bars stacked on the secondary axis (faint, for context).

    df: must have columns ticker, window_start, mean_news_sentiment, news_article_count
    """
    # Distinct colors for up to 5 tickers (matches len(TICKERS) in data.py)
    palette = ["#00C853", "#FFB300", "#03A9F4", "#E91E63", "#9C27B0"]
    color_for = {t: palette[i % len(palette)] for i, t in enumerate(tickers)}

    fig = go.Figure()

    # One sentiment line per ticker
    for ticker in tickers:
        sub = df[df["ticker"] == ticker].sort_values("window_start")
        if sub.empty:
            continue
        fig.add_trace(go.Scatter(
            x=sub["window_start"],
            y=sub["mean_news_sentiment"],
            mode="lines+markers",
            name=ticker,
            line=dict(color=color_for[ticker], width=2),
            marker=dict(size=4),
        ))

    # Total article count across all tickers as faint bars on secondary axis
    if "news_article_count" in df.columns:
        total = (
            df.groupby("window_start")["news_article_count"]
            .sum()
            .reset_index()
            .sort_values("window_start")
        )
        if total["news_article_count"].sum() > 0:
            fig.add_trace(go.Bar(
                x=total["window_start"],
                y=total["news_article_count"],
                name="Total articles",
                marker_color=AMBER,
                opacity=0.25,
                yaxis="y2",
            ))

    # Horizontal line at sentiment = 0 (neutral reference)
    fig.add_hline(y=0, line_dash="dash", line_color=GRID, opacity=0.6)

    fig.update_layout(
        title=dict(
            text="News sentiment — 5-min rolling mean per ticker",
            font=dict(color=TEXT, size=16, family="monospace"),
        ),
        plot_bgcolor=BG,
        paper_bgcolor=BG,
        font=dict(color=TEXT, family="monospace"),
        xaxis=dict(gridcolor=GRID, showspikes=True,
                   spikemode="across", spikecolor=AMBER, spikethickness=1),
        yaxis=dict(
            title="Sentiment (−1 to +1)",
            gridcolor=GRID,
            range=[-1, 1],
            zeroline=False,
        ),
        yaxis2=dict(
            title="Article count",
            overlaying="y", side="right",
            showgrid=False, rangemode="tozero",
        ),
        legend=dict(
            bgcolor=BG, bordercolor=GRID, borderwidth=1,
            x=0.01, y=0.99, orientation="v",
        ),
        height=500,
        margin=dict(l=50, r=50, t=50, b=40),
        hovermode="x unified",
    )
    return fig