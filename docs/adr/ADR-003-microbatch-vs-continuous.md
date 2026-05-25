# ADR-003: Micro-batch (Trigger.AvailableNow) consumption over continuous streaming

**Status:** Accepted
**Date:** ~Day 1–2 (reconstructed 2026-05-24 from project history)
**Context day:** Day 1 (forced by Free Edition), validated through Days 2–6

> Reconstructed from build notes. This decision wasn't explicitly numbered in
> the surviving notes; it is assigned ADR-003 here as one of the foundational
> architecture decisions. Reasoning reflects what was actually decided at the
> time.

## Context

The original architecture called for three continuous Spark Structured Streaming
jobs (bronze, silver, gold) reading from Kafka 24/7. The workspace is
**Databricks Free Edition**, which supports only serverless compute and does
**not** support continuous streaming triggers — only
`trigger(availableNow=True)`, which processes all available data once and stops.

## Decision

Consume Kafka with **scheduled micro-batches** (`Trigger.AvailableNow`)
orchestrated by Airflow, rather than continuous 24/7 streaming.

## Rationale

- Free Edition makes continuous triggers impossible, so this was forced — but
  it is also **more realistic for industry**, which is why it was embraced
  rather than worked around.
- Continuous streaming consumers cost a fortune (compute always on). Most
  real use cases tolerate a 5-minute delay, and **streaming source + scheduled
  micro-batch consumer is the most common production pattern**.
- It folds cleanly into the Airflow layer we were already building: Airflow's
  responsibility simply expands to trigger the bronze/silver/gold micro-batches
  on a schedule.
- The precise framing — *"the data IS streaming; the consumer is micro-batch"* —
  is both accurate and interview-strong.

## Consequences

- The pipeline is "real-time streaming ingestion with scheduled micro-batch
  processing." Resume keywords retained: Spark Structured Streaming, Delta Lake
  medallion, Kafka ingestion, Airflow orchestration — and **gained**
  `Trigger.AvailableNow` plus a tighter Airflow integration story.
- Airflow owns the Databricks Jobs triggers; **Airflow never touches Kafka**
  directly — that boundary is deliberate (recorded again in the Day 6 notes).
- Offsets are checkpointed under Unity Catalog Volumes so each micro-batch reads
  only new messages since the last run.
- Free Edition Spark Connect constraints were tracked alongside this (no
  `.rdd`/`.persist()`/`.cache()`, DBFS off, use `/Volumes/...`,
  no `F.current_timestamp()` inside `foreachBatch`).

## Related
- ADR-004 (medallion layer contracts) — what each micro-batch writes.