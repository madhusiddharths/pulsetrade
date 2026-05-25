# ADR-004: Medallion layer contracts + MERGE-based idempotency

**Status:** Accepted
**Date:** ~Day 2–3 (reconstructed 2026-05-24 from project history)
**Context day:** Days 2–3 (silver/gold build), validated Day 6

> Reconstructed from build notes and the Day 3 journal. Assigned ADR-004 here.
> Reasoning reflects the contracts actually enforced in the notebooks.

## Context

Data flows Kafka → bronze → silver → gold on Delta Lake. Without explicit
per-layer contracts, transformations leak across boundaries (e.g. aggregation
creeping into silver), and re-running a notebook against the same Kafka offsets
produces duplicate rows.

## Decision

Enforce three strict layer contracts, and make silver/gold writes **idempotent
via `MERGE INTO`** on natural keys.

| Layer  | Contract | Operation type |
|--------|----------|----------------|
| Bronze | Raw Kafka events, append-only, no transformation — an audit log | pass-through |
| Silver | Cleaned, deduplicated, per-row enriched; enforces data-quality contract | 1:1 row transforms |
| Gold   | Business-ready 5-minute aggregated feature windows | N:1 aggregation |

## Rationale

- **Consumer flexibility is the reason silver stays row-level.** If silver
  pre-aggregated, every downstream consumer would be stuck with that window. By
  keeping silver at row granularity, multiple gold tables (`gold_5min` for the
  detector, hourly for the dashboard, daily for backtesting) can each pick their
  own window off the same silver. The smell test: *if silver had aggregation,
  gold would have nothing left to do.*
- **Bronze as append-only audit log** preserves lineage and allows full replay.
- **`MERGE INTO` on natural keys** (`ticker + quote_timestamp` for stocks,
  `article_id + ticker` for news) makes re-runs free of duplicates — silver is
  mutable because corrections arrive, and MERGE expresses upsert-on-key
  precisely. Idempotent re-runs against the same offsets produce no dupes.
- Gold computes its own 5-minute OHLC (distinct from Finnhub's day OHLC carried
  in silver) specifically to feed rolling volatility / breakout / anomaly
  features.

## Consequences

- Naming convention `<from>_to_<to>` (e.g. `02_bronze_to_silver_stocks`) encodes
  the boundary each notebook crosses, as a readability aid.
- News rows are exploded one-per-matched-ticker in silver, then FinBERT-scored
  in a Pandas UDF (see the FinBERT-on-Free-Edition debugging saga in the journal:
  read-only fs → env-var propagation → Xet protocol → missing transformers).
- A low-volume-gold issue (empty gold from streaming watermark emission delay)
  drove a switch to `foreachBatch` + idempotent MERGE windowing — trading
  exactly-once streaming semantics for simpler, correct batch semantics. Same
  correctness, much simpler. (Recorded as an interview story.)

## Related
- ADR-003 (micro-batch consumption) — how rows arrive into bronze.