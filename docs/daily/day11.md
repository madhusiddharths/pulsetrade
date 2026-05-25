# Day 11 — Local compose stack with real service-to-service networking

**Goal:** run the API + dashboard + Postgres containers together locally with
proper service networking, as the bridge between Day 10 (single images) and
Day 12 (Kubernetes). The deliberate point: solve container-to-container
networking *before* adding K8s complexity, because the mental model is the same
— services reach each other by name, not by `host.docker.internal`.

Cloud work (GAR push, GKE, Helm) was explicitly pushed to Day 12 so this day
stays fully local and $0.

## What was built
- `docker-compose.yml` at project root — three services on a shared bridge
  network (`pulsenet`):
  - **postgres** (`postgres:16-alpine`) — fresh DB on its own named volume
    (`pgdata`), isolated from the existing `pulsetrade-postgres` dev container.
    Host port `5434` (see conflict note below). Healthcheck gates the api.
  - **api** — built from `./api`; `host.docker.internal:5433` overridden to
    `postgres:5432` (service name + internal port).
  - **dashboard** — built from `./dashboard`; reads Postgres directly and calls
    the api via `PULSETRADE_API_URL=http://api:8000`.

## The core change: service-name networking replaces host.docker.internal
On Day 10 the single API container reached the host's Postgres via
`host.docker.internal:5433` — a local-Docker convenience that does not exist in
compose networks or K8s. In compose, containers on the same network resolve each
other by **service name**. So:
- api → `postgres:5432` (NOT `host.docker.internal:5433`)
- dashboard → `api:8000`

Note the port: containers use Postgres's **internal** port `5432` via the
service name. The host-side `5434` mapping is only for *me* connecting from the
Mac (psql/GUI); containers never use it. This is exactly the K8s Service model,
so Day 12 is the same idea in different syntax.

## Proof it works (end-to-end)
The API startup log went from the Day-10 `host.docker.internal` line to:
```
[startup] postgres: postgres:5432/pulsetrade
[startup] postgres schema ok
```
`postgres schema ok` = the api resolved the service name, connected on 5432, and
ran init_schema() over the compose network.

Full path proven visually: triggered an investigation from the dashboard's
Trigger panel → dashboard reached the **api** service → api spawned its
co-located MCP subprocess → agent ran tools → wrote the report to the
**postgres** service → dashboard read it back and rendered the "Agent brief".
Every container-to-container hop succeeded by name. Screenshot in
`docs/screenshots/`.

(Agent reasoning was sharp again: with no gold/news data it inferred the anomaly
timestamp 21:40 UTC = 5:40 PM EDT is *after* the 4 PM market close, and concluded
"illiquid after-hours trade or data error, not a real event" — evidence-based,
no hallucination.)

## Bugs / lessons (the interview-valuable part)

### 1. The "DNS can't resolve postgres" red herring
First run inside compose still failed with `could not translate host name
"postgres"`. It *looked* like a network bug, but the real cause was that the
**postgres container never started** — so there was no peer to resolve. The DNS
error was a symptom, not the disease. Lesson: when a service name won't resolve,
first check the target container is actually up, before suspecting the network.

### 2. Host-port conflict with the existing dev Postgres
The compose Postgres tried to bind host `5433`, but the existing
`pulsetrade-postgres` dev container was already on `5433` →
`Bind for 0.0.0.0:5433 failed: port is already allocated`, which killed the
whole stack and *caused* bug #1. Fix: moved the compose Postgres host mapping to
`5434:5432`. Only the host-side access port changed; the internal `5432` and the
`postgres:5432` service-name path the containers use were unaffected. This let
the existing dev DB keep running untouched.

### 3. Inferred env var name was wrong
The compose file first guessed `API_BASE_URL` for the dashboard's API endpoint.
A `grep` of the dashboard code showed it actually reads
`PULSETRADE_API_URL` (default `http://localhost:8000` in `dashboard/lib/api.py`).
Wrong key = the Trigger panel silently falls back to localhost (= the dashboard
container itself) and can't reach the api. Fix: use the real key,
`PULSETRADE_API_URL=http://api:8000`. Lesson: verify env var names against the
code, don't infer them from logs.

### Also seen (harmless)
- `Database directory appears to contain a database; Skipping initialization` —
  the `pgdata` volume persisted across runs, so Postgres didn't re-bootstrap.
  Harmless (schema hook runs regardless). `docker compose down -v` wipes it for a
  truly fresh start.
- "Recreated" containers from earlier partial runs needed a clean
  `docker compose down` (removes containers + network) before `up` to attach to
  a freshly-created network.

## Commands
```bash
docker compose up --build     # build + run all three
docker compose down           # stop + remove containers and network
docker compose down -v        # also wipe the pgdata volume (fresh DB)
```

## Deferred to Day 12 (pure Kubernetes session)
- Rebuild images for `linux/amd64` (Mac builds arm64; GKE nodes are amd64 —
  arm64 image on the cluster = `exec format error`). Use `--platform` / buildx.
- Push both images to Google Artifact Registry (GAR).
- Create the GKE cluster (ephemeral — create, deploy, demo, tear down in one
  session to stay inside the ~$18 Developer Program credit).
- Helm chart: Deployment / Service / ConfigMap / Secret for api + dashboard.
- The `host.docker.internal` → service-name pattern proven today maps directly
  onto K8s Services; Postgres on K8s will be a service named `postgres` or an
  external managed DB.

## Cost note
GCP free trial ($300) is expired. Current funding: ~$18 in Google Developer
Program monthly credits (one $10 + one $8.38), valid through 2027, applicable to
all Google Cloud services. Ephemeral GKE sessions fit inside this if torn down
promptly. Account-mode (paid vs trial) still to be confirmed before Day 12, but
current credit covers the planned work.