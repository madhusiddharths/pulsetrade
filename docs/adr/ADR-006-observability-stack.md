# ADR-006: Self-hosted Prometheus/Grafana for system metrics

**Status:** Accepted  
**Date:** 2026-05-15

## Context

The agent service needs system-level observability — latency percentiles,
throughput, error rates, token cost tracking. LangSmith already covers
agent-internal tracing (prompts, tool calls per investigation), but it
doesn't aggregate across runs over time.

## Options considered

1. **Hosted (Datadog / New Relic / Grafana Cloud free tier)**
   - Pros: zero ops, polished UX
   - Cons: cost at scale, vendor lock-in, less resume signal

2. **Self-hosted Prometheus + Grafana (Docker Compose locally,
   eventually K8s)** ← chosen
   - Pros: industry-standard stack, transferable skill, fits K8s
     migration on Day 10
   - Cons: more setup time, must manage retention/storage

3. **OpenTelemetry Collector + a hosted backend**
   - Pros: vendor-neutral
   - Cons: extra layer for no benefit at this scale

## Decision

Self-host Prometheus + Grafana via Docker Compose now, port to a
kube-prometheus-stack Helm release in K8s on Day 10. Reuse the
exported dashboard JSON.

## Consequences

- We commit dashboard JSON to the repo (declarative config).
- Retention capped at 15 days locally — fine for demo.
- Adds one custom LangChain callback that must be maintained as
  langchain-google-genai evolves.