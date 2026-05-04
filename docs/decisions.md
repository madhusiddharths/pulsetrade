# Architecture Decision Records

## ADR-001: Switch from yfinance to Finnhub for stock data ingestion

**Date:** 2026-05-04
**Status:** Accepted

### Context
Initial implementation used `yfinance` for stock price ingestion due to its
popularity and zero-cost access. Production testing revealed persistent
failures across all tickers with errors:
- `Expecting value: line 1 column 1 (char 0)`
- `KeyError: 'currentTradingPeriod'`

### Investigation
Direct HTTP test against Yahoo's chart API confirmed the data source itself
returns valid JSON with current quotes. The failure is internal to yfinance —
it issues a secondary metadata request whose response shape has drifted from
what the library expects.

This is a known and recurring pattern: yfinance is an unofficial scraper of
Yahoo's undocumented endpoints, and Yahoo periodically changes those endpoints
without notice.

### Decision
Switch to Finnhub's official REST API.

### Rationale
- Official documented contract — won't break silently
- Free tier: 60 req/min, no credit card
- Returns richer per-call data (open/high/low/close/prev_close)
- Predictable rate limits enable proper polling cadence
- Production reliability over zero-config convenience

### Consequences
- One additional API key in `.env` (FINNHUB_API_KEY)
- Slightly different schema in `StockTick` dataclass (more fields)
- Removes yfinance + curl_cffi from runtime dependencies
