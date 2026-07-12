# ADR-009: Postgres as an in-cluster pod for the GKE demo (not Cloud SQL)

**Status:** Accepted
**Date:** 2026-05-25
**Context day:** Day 12 (GKE deployment)

## Context

Deploying the API + dashboard to GKE requires a Postgres for the
`investigations` and `anomaly_queue` tables. Two options: run Postgres as a pod
*inside* the cluster (StatefulSet/Deployment + PersistentVolume), or use Google
**Cloud SQL**, a managed Postgres running *outside* the cluster that pods connect
to over the network. The compose stack (Day 11) already proved the in-cluster
pattern with service-name networking.

## Decision

Run Postgres as an **in-cluster pod** for the demo deployment. Document Cloud SQL
as the production-correct choice and the trigger for adopting it.

## Rationale

### Cost (the decisive factor for this project)
- **Pod:** ~$0 extra. It runs on the node-pool compute already paid for, with a
  small PersistentVolume disk (pennies). Torn down with the cluster → no standing
  charge. Fits the ephemeral "create cluster, demo, destroy" workflow.
- **Cloud SQL:** the cheapest tier (`db-f1-micro`, shared-core) is ~$9–11/month,
  and it is a **standing charge** — it bills independently of the cluster, so it
  keeps accruing after the cluster is torn down unless the instance is explicitly
  stopped or deleted. Against the project's ~$18 Google Developer Program credit,
  an always-on Cloud SQL instance could consume roughly half the monthly credit,
  or eat $10–20 over the remaining project days if left running. Also: shared-core
  tiers are **not covered by the Cloud SQL SLA**.

### Scope / risk
Day 12 is the heaviest day (amd64 rebuild, GAR push, cluster creation, Helm,
deploy, verify, teardown). Cloud SQL would add instance provisioning, the
GKE↔Cloud SQL connection (Auth Proxy sidecar or private-IP VPC — a common source
of connection debugging), and a service account. That extra surface risks the
"one focused session" plan. The pod pattern was already proven in compose
yesterday, so it is a near-direct, low-risk translation.

### Demo vs production
For a demo, data does **not** need to survive cluster teardown — a fresh DB whose
schema is created on API startup (`init_schema()`) is sufficient to show the full
path working. The pod is the right tool for that purpose.

## When Cloud SQL is preferred (real-world / production)

In production, where investigation history and queue state **must be retained**,
Cloud SQL (or any managed DB) is preferred:

- **Data persistence** — the database lifecycle is decoupled from the cluster
  lifecycle. Tear down or recreate the cluster and the data is untouched. An
  in-cluster pod's data dies with the ephemeral cluster.
- **Managed operations** — automated backups, patching, failover, point-in-time
  recovery, all handled by the provider rather than by us.
- **Correct stateful/stateless separation** — Kubernetes is designed around
  stateless workloads; primary databases as in-cluster pods are a known
  anti-pattern for production stateful data. Managed DBs outside the cluster are
  the industry-standard pattern.

This is the same independent-variation principle as ADR-008: the database varies
independently from the app (different lifecycle, durability, and operational
needs), so in production it belongs **outside** the cluster as its own managed
service.

## Revisit trigger

Adopt Cloud SQL when the deployment stops being an ephemeral demo and needs to
**retain data across cluster lifecycles** — i.e. a persistent/staging/production
environment, or any time accumulated investigation history must survive a
teardown. Connectivity would use the Cloud SQL Auth Proxy (sidecar) or private IP
via VPC.

## Consequences

- The GKE deploy is fully self-contained and ephemeral; nothing standing bills
  after teardown.
- Investigation history does **not** persist between cluster sessions — expected
  and acceptable for the demo.
- The production path is documented, so the senior judgment (and the Cloud SQL
  pattern) is captured without spending credit or Day-12 time on it.

## Related
- ADR-008 (independent-variation principle; MCP co-location).
- Day 11 journal (in-cluster Postgres + service-name networking, proven in compose).
- Day 12 journal (GKE deployment).