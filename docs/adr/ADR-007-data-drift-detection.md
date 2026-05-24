# ADR-007: Data Drift Detection (Evidently + Airflow DockerOperator)

**Status:** Accepted
**Date:** 2026-05-24
**Supersedes:** none
**Related:** ADR-006 (observability stack)

## Context

PulseTrade had two observability layers after Day 8:

1. **LangSmith** (Day 5) — agent-level tracing: prompts, tool calls, and
   reasoning within a single investigation.
2. **Prometheus + Grafana** (Day 8) — system-level metrics: latency,
   throughput, error rate, token cost across investigations over time.

Neither catches a third class of failure: the *input data itself* silently
changing shape. The gold feature table (`gold_5min_features`) feeds both the
anomaly detector and the agent. If a feature's distribution drifts — a data
source changes a field's scale, FinBERT's sentiment behavior shifts, or a
silent bug corrupts a transformation — nothing crashes. The pipeline stays
green while downstream behavior quietly degrades. This is the hardest failure
to debug after the fact precisely because there is no error.

We need **data observability**: scheduled detection of distribution drift in
the gold features, with alerts persisted for review.

## Decision

### 1. Use Evidently AI for drift detection

Evidently (open source, pinned `>=0.7,<0.8`) provides per-column statistical
drift tests (Kolmogorov–Smirnov for numerics, chi-square for low-cardinality
categoricals), a dataset-level drift summary, and an HTML visualization, out
of the box. Rolling our own KS tests + plotting would duplicate well-tested
library code for no benefit at this scale.

Note: Evidently rewrote its API at 0.7. We pin to 0.7.x and write against the
new API (`from evidently import Report`, `from evidently.presets import
DataDriftPreset`, `report.run(current, reference)` — current first). The
result dict has no pre-computed "drifted" boolean; per-column drift is derived
as `p_value < threshold` (default 0.05). Lower p-value = more drift.

### 2. Run drift as a separate container (DockerOperator), not in-worker

The drift job runs as its own image (`pulsetrade-drift`), spawned by Airflow's
`DockerOperator` (Docker-out-of-Docker via the mounted host socket), rather
than in-process via `PythonOperator`.

Rationale:
- **Dependency isolation.** Evidently pulls a heavy tree (scikit-learn, scipy,
  numpy). Keeping it out of the Airflow image avoids version conflicts with
  Airflow's own pandas/numpy pins.
- **Dev/prod parity.** The same image becomes a `KubernetesPodOperator` or
  CronJob in K8s later — same image, same entrypoint, different orchestrator.
  Building it as a container *now* means the K8s migration is a wrapper change,
  not a rewrite. We deliberately chose the harder local path (DoD socket
  mounting) to test the container-invocation model we'll actually deploy.
- **Consistency with existing pattern.** The Day 6 DAGs already orchestrate
  external work (DatabricksSubmitRunOperator) rather than doing everything
  in-worker; DockerOperator fits that established shape.

### 3. Alert on both share-based and key-feature conditions

An alert fires if EITHER:
- share of drifted columns ≥ 0.30, OR
- any key feature (`mean_news_sentiment`, `mean_change_pct`) drifts.

The key-feature condition catches drifts that matter even when overall share
is low — a single critical feature shifting should alert regardless of the
aggregate. Every run writes a `drift_alerts` row (not only alerts), preserving
a full history of drift over time, with an `alerted` boolean.

### 4. Daily schedule

Drift is a slow phenomenon; daily detection is realistic and conserves
Databricks Free Edition compute versus hourly.

## Demonstration vs. production data

With ~10 days of intermittently-collected gold data (~440 rows across 5
tickers), there is not yet enough cleanly-windowed history for genuine
week-over-week comparison. Comparing arbitrary slices would produce false
drift that is an artifact of *when* data was sampled, not a real distribution
change.

Therefore the current pipeline runs in **demo mode**: it pulls real gold data,
shuffles, splits 50/50, and injects a sentiment shift into the "current" half
to demonstrate detection on the real schema. This is honest — the detection
machinery, schema, and full pipeline are real; only the drift is synthetic.

The data layer is built with a swappable seam: `get_current_window(
inject_drift=False)` and time-separated window queries convert this to real
week-over-week with no change to the detector, alerting, or DAG. This is a
one-function swap once sufficient production history accumulates.

## Consequences

- A new deployable unit (`pulsetrade-drift` image) and a new table
  (`drift_alerts`) in the application Postgres.
- The Airflow worker now mounts the Docker socket (Docker-out-of-Docker).
  Acceptable for local dev; in K8s this is replaced by native pod scheduling
  (KubernetesPodOperator), which is the cleaner production form.
- Evidently is pinned to 0.7.x; a future 0.8 may change both the API and the
  result-dict structure the extraction logic depends on. The pin protects us;
  upgrading is a deliberate, tested action.

## Lessons captured

- **Windowing comparability is the core risk of drift detection.** Reference
  and current windows must be sampled under comparable conditions. A sorted or
  non-comparable split produces drift everywhere as an artifact (observed
  directly during development with a class-sorted iris split). For real gold
  windows, this means comparing like sessions (e.g. same time-of-day, both
  trading days), not arbitrary date ranges.