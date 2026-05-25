# ADR-001: Finnhub over yfinance for the market data source

**Status:** Accepted
**Date:** ~Day 1 (reconstructed 2026-05-24 from project history)
**Context day:** Day 1–2 (ingestion layer)

> Reconstructed retroactively from build notes and the status doc, which
> already referenced this decision as "ADR-001." Content reflects the real
> reasoning recorded at the time; wording is new.

## Context

The stock producer needs a reliable source of intraday OHLC + previous-close
data for ~5 tickers, polled on a ~30s cadence and published to the
`stock-prices` Kafka topic. Early prototyping used the `yfinance` library.

## Decision

Use **Finnhub's official REST quote endpoint** as the stock data source instead
of `yfinance`.

## Rationale

- `yfinance` broke repeatedly with internal `currentTradingPeriod KeyError`
  failures. The root cause is that it scrapes Yahoo's front-end rather than
  calling a supported API, so it breaks whenever Yahoo changes their page.
- Finnhub is an **official REST endpoint** with documented, explicit rate
  limits — predictable behaviour we can build retry/backoff around.
- A single Finnhub call returns full OHLC **plus previous close**, which is
  exactly what the silver layer needs to compute `gap` and `change_pct`.
- A scraped, unofficial source is an unacceptable foundation for a pipeline
  whose entire point is reliability — a data-source choice should be deliberate,
  not a stack accident.

## Consequences

- Producer is bound to Finnhub's rate limits; the 30s poll cadence for 5 tickers
  stays well within free-tier limits.
- Both producers ship with graceful shutdown handlers, exponential-backoff retry
  on network errors, `acks=all` for durability, and lz4 compression.
- If Finnhub free-tier limits ever bind, the swap surface is isolated to the
  producer — bronze and everything downstream is source-agnostic.

## Related
- ADR-003 (micro-batch consumption) — downstream of this ingestion choice.