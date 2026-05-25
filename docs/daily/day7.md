## Day 7 — Streamlit dashboard (✅ complete)

### What works
- 5-page dashboard at localhost:8501, Bloomberg-style dark theme
- **Live Prices**: candlestick OHLC + per-ticker summary cards with sparklines
- **News Sentiment**: multi-line comparison chart + per-ticker summary table
- **Investigations**: paginated browser, expandable cards with full markdown briefs
- **Trigger**: form-based UI to run an investigation via FastAPI /investigate
- Auto-refresh (30s / 60s) with @st.cache_data TTL on every query
- Defensive synthetic-data fallback on every Databricks/Postgres call;
  yellow banner makes fallback state explicit to the viewer

### Lessons captured
- Streamlit reruns the entire script on every interaction; @st.cache_data
  + session_state are the mitigations
- Per-service venv pattern: dashboard has its own .venv (streamlit + plotly
  are heavy; isolation prevents conflicts with the api venv)
- "Reads bypass FastAPI" architectural split: Live Prices and Investigations
  query Databricks/Postgres directly. Only Trigger goes through FastAPI
  (because triggers are writes that must run the agent). Reduces API
  load + simplifies reasoning about state.

### Demo-ready
- The Trigger panel + Investigations page together is the demo opener:
  trigger an investigation live → 30 seconds later see the agent's brief
  appear → switch to Investigations → find the new entry
- Every page has a screenshot in docs/screenshots/